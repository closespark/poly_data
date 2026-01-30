"""
Book Reconciler - REST vs WebSocket Reconciliation

Provides independent ground truth check for WS book state.
Runs async (60s poll), never in the hot path.

Design Principles:
1. REST is never consulted inline - it only annotates health state
2. Compare top-of-book only (not full depth - that's noisy)
3. If REST disagrees with WS: trust neither, widen and wait
4. Never "correct" WS with REST mid-stream

Drift Metric:
    book_drift_pct = max(
        abs(ws_bid - rest_bid) / mid,
        abs(ws_ask - rest_ask) / mid
    )

Thresholds:
    < 0.2%      OK
    0.2 - 0.5%  WARN (log only)
    0.5 - 1.0%  WIDEN + schedule reseed
    > 1.0%      book_ok = false (integrity gate trips)

Usage:
    reconciler = BookReconciler(book_store)

    # In a background thread/task:
    while True:
        for token_id in active_tokens:
            result = reconciler.poll(token_id)
            if result:
                reconciler.update_drift_state(token_id, result)
        time.sleep(60)

    # In signal engine:
    drift = reconciler.get_drift(token_id)
    if drift and drift['book_drift_pct'] > DRIFT_FAIL_THRESHOLD:
        # Trip integrity gate
"""

import json
import logging
import time
import threading
import requests
from dataclasses import dataclass
from typing import Dict, Optional, List
from collections import defaultdict


# Structured logger for reconciliation events
reconciler_logger = logging.getLogger('signal_engine.reconciler')
reconciler_logger.setLevel(logging.INFO)


# Drift thresholds
DRIFT_OK_THRESHOLD = 0.002        # 0.2% - no action
DRIFT_WARN_THRESHOLD = 0.005     # 0.5% - log warning
DRIFT_WIDEN_THRESHOLD = 0.01     # 1.0% - widen + reseed
DRIFT_FAIL_THRESHOLD = 0.01      # 1.0% - integrity gate trips


@dataclass
class DriftState:
    """Current drift state for a token"""
    token_id: str
    book_drift_pct: float
    ws_bid: float
    ws_ask: float
    rest_bid: float
    rest_ask: float
    last_poll_ts: float
    action: str  # 'ok', 'warn', 'widen', 'fail'
    consecutive_failures: int = 0


class BookReconciler:
    """
    Async REST reconciliation for WS book state.

    Call poll() in a background thread, never inline.
    Query get_drift() from the signal engine hot path.
    """

    # Polymarket REST endpoint
    BOOK_URL = "https://clob.polymarket.com/book"

    def __init__(self, book_store, poll_interval: float = 60.0):
        """
        Args:
            book_store: Reference to the live BookStore
            poll_interval: Seconds between polls (default 60)
        """
        self.book_store = book_store
        self.poll_interval = poll_interval

        # Current drift state per token
        # {token_id: DriftState}
        self.drift_states: Dict[str, DriftState] = {}
        self.lock = threading.RLock()

        # Tokens that need reseed due to drift
        self._needs_reseed_from_drift: set = set()

        # Stats
        self.polls_total = 0
        self.polls_failed = 0
        self.drift_warnings = 0
        self.drift_failures = 0

        # For health logging
        self._drift_history: List[float] = []  # Last N drift values
        self._drift_history_max = 100

    def fetch_rest_book(self, token_id: str) -> Optional[dict]:
        """
        Fetch top-of-book from REST API.

        Returns:
            {
                'best_bid': float or None,
                'best_ask': float or None,
                'timestamp': float
            }
        """
        try:
            response = requests.get(
                self.BOOK_URL,
                params={'token_id': token_id},
                timeout=5
            )
            response.raise_for_status()
            data = response.json()

            # Extract top of book
            bids = data.get('bids', [])
            asks = data.get('asks', [])

            best_bid = float(bids[0]['price']) if bids else None
            best_ask = float(asks[0]['price']) if asks else None

            return {
                'best_bid': best_bid,
                'best_ask': best_ask,
                'timestamp': time.time()
            }

        except Exception as e:
            reconciler_logger.warning(json.dumps({
                'event': 'rest_fetch_failed',
                'token_id': token_id[:16] if token_id else '',
                'error': str(e)
            }))
            return None

    def get_ws_top_of_book(self, token_id: str) -> Optional[dict]:
        """
        Get current top-of-book from WS state.

        Returns:
            {
                'best_bid': float or None,
                'best_ask': float or None,
                'last_update_ts': float
            }
        """
        with self.book_store.lock:
            book = self.book_store.books.get(token_id)
            if not book:
                return None

            return {
                'best_bid': book.bids[0].price if book.bids else None,
                'best_ask': book.asks[0].price if book.asks else None,
                'last_update_ts': book.last_update_ts
            }

    def compute_drift(self, ws: dict, rest: dict) -> Optional[float]:
        """
        Compute book_drift_pct.

        Formula:
            book_drift_pct = max(
                abs(ws_bid - rest_bid) / mid,
                abs(ws_ask - rest_ask) / mid
            )

        Returns None if either side is missing.
        """
        ws_bid = ws.get('best_bid')
        ws_ask = ws.get('best_ask')
        rest_bid = rest.get('best_bid')
        rest_ask = rest.get('best_ask')

        # Need all four values to compute drift
        if None in (ws_bid, ws_ask, rest_bid, rest_ask):
            return None

        # Mid from WS (our authoritative state)
        mid = (ws_bid + ws_ask) / 2
        if mid <= 0:
            return None

        bid_drift = abs(ws_bid - rest_bid) / mid
        ask_drift = abs(ws_ask - rest_ask) / mid

        return max(bid_drift, ask_drift)

    def classify_drift(self, drift_pct: float) -> str:
        """Classify drift into action category"""
        if drift_pct < DRIFT_OK_THRESHOLD:
            return 'ok'
        elif drift_pct < DRIFT_WARN_THRESHOLD:
            return 'warn'
        elif drift_pct < DRIFT_WIDEN_THRESHOLD:
            return 'widen'
        else:
            return 'fail'

    def poll(self, token_id: str) -> Optional[dict]:
        """
        Poll REST and compare to WS state.

        This is the main entry point for the background thread.

        Returns:
            {
                'token_id': str,
                'book_drift_pct': float,
                'ws': dict,
                'rest': dict,
                'action': str,  # 'ok', 'warn', 'widen', 'fail'
            }

        Returns None if unable to fetch or compare.
        """
        self.polls_total += 1

        # Get REST state
        rest = self.fetch_rest_book(token_id)
        if not rest:
            self.polls_failed += 1
            return None

        # Get WS state
        ws = self.get_ws_top_of_book(token_id)
        if not ws:
            return None

        # Compute drift
        drift = self.compute_drift(ws, rest)
        if drift is None:
            return None

        # Classify
        action = self.classify_drift(drift)

        result = {
            'token_id': token_id,
            'book_drift_pct': drift,
            'ws_bid': ws.get('best_bid'),
            'ws_ask': ws.get('best_ask'),
            'rest_bid': rest.get('best_bid'),
            'rest_ask': rest.get('best_ask'),
            'action': action,
        }

        # Track history for health stats
        self._drift_history.append(drift)
        if len(self._drift_history) > self._drift_history_max:
            self._drift_history = self._drift_history[-self._drift_history_max:]

        return result

    def update_drift_state(self, token_id: str, result: dict):
        """
        Update internal drift state after a poll.

        Call this after poll() returns a result.
        Handles logging and reseed scheduling.
        """
        if not result:
            return

        now = time.time()
        action = result['action']
        drift_pct = result['book_drift_pct']

        with self.lock:
            # Get or create state
            prev_state = self.drift_states.get(token_id)
            consecutive_failures = 0

            if prev_state and action in ('widen', 'fail'):
                consecutive_failures = prev_state.consecutive_failures + 1

            # Update state
            self.drift_states[token_id] = DriftState(
                token_id=token_id,
                book_drift_pct=drift_pct,
                ws_bid=result['ws_bid'],
                ws_ask=result['ws_ask'],
                rest_bid=result['rest_bid'],
                rest_ask=result['rest_ask'],
                last_poll_ts=now,
                action=action,
                consecutive_failures=consecutive_failures,
            )

            # Handle escalations
            if action == 'warn':
                self.drift_warnings += 1
                reconciler_logger.info(json.dumps({
                    'event': 'drift_warning',
                    'token_id': token_id[:16],
                    'book_drift_pct': round(drift_pct, 5),
                    'ws_bid': result['ws_bid'],
                    'ws_ask': result['ws_ask'],
                    'rest_bid': result['rest_bid'],
                    'rest_ask': result['rest_ask'],
                }))

            elif action in ('widen', 'fail'):
                self.drift_failures += 1

                # Schedule reseed
                self._needs_reseed_from_drift.add(token_id)

                # Also add to book_store's reseed queue for consistency
                if hasattr(self.book_store, '_needs_reseed'):
                    self.book_store._needs_reseed.add(token_id)

                reconciler_logger.warning(json.dumps({
                    'event': 'drift_failure',
                    'token_id': token_id[:16],
                    'book_drift_pct': round(drift_pct, 5),
                    'action': action,
                    'consecutive_failures': consecutive_failures,
                    'ws_bid': result['ws_bid'],
                    'ws_ask': result['ws_ask'],
                    'rest_bid': result['rest_bid'],
                    'rest_ask': result['rest_ask'],
                    'reseed_scheduled': True,
                }))

    def get_drift(self, token_id: str) -> Optional[dict]:
        """
        Get current drift state for a token.

        Called from the hot path (SignalEngine._check_book_integrity).
        Must be fast - just a dict lookup.

        Returns:
            {
                'book_drift_pct': float,
                'action': str,
                'stale': bool,  # True if poll is older than 2x interval
            }
        """
        with self.lock:
            state = self.drift_states.get(token_id)
            if not state:
                return None

            # Check if state is stale
            age = time.time() - state.last_poll_ts
            stale = age > (self.poll_interval * 2)

            return {
                'book_drift_pct': state.book_drift_pct,
                'action': state.action,
                'stale': stale,
                'age_s': round(age, 1),
            }

    def get_tokens_needing_reseed(self) -> List[str]:
        """Get and clear tokens that need reseed due to drift"""
        with self.lock:
            tokens = list(self._needs_reseed_from_drift)
            self._needs_reseed_from_drift.clear()
            return tokens

    def get_health_stats(self) -> dict:
        """Get health statistics for periodic logging"""
        with self.lock:
            drift_values = self._drift_history[-50:] if self._drift_history else []

            return {
                'polls_total': self.polls_total,
                'polls_failed': self.polls_failed,
                'drift_warnings': self.drift_warnings,
                'drift_failures': self.drift_failures,
                'tokens_tracked': len(self.drift_states),
                'avg_drift_pct': round(sum(drift_values) / len(drift_values), 5) if drift_values else 0,
                'max_drift_pct': round(max(drift_values), 5) if drift_values else 0,
                'tokens_in_fail_state': sum(
                    1 for s in self.drift_states.values()
                    if s.action in ('widen', 'fail')
                ),
            }


class ReconcilerRunner:
    """
    Background thread that runs the reconciler on a schedule.

    Usage:
        runner = ReconcilerRunner(reconciler, active_tokens_fn)
        runner.start()
        # ... later ...
        runner.stop()
    """

    def __init__(self, reconciler: BookReconciler,
                 get_active_tokens: callable,
                 poll_interval: float = 60.0):
        """
        Args:
            reconciler: BookReconciler instance
            get_active_tokens: Callable that returns list of token_ids to poll
            poll_interval: Seconds between full cycles
        """
        self.reconciler = reconciler
        self.get_active_tokens = get_active_tokens
        self.poll_interval = poll_interval

        self._running = False
        self._thread = None

    def start(self):
        """Start the background reconciliation thread"""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        reconciler_logger.info(json.dumps({
            'event': 'reconciler_started',
            'poll_interval': self.poll_interval,
        }))

    def stop(self):
        """Stop the background thread"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        reconciler_logger.info(json.dumps({
            'event': 'reconciler_stopped',
        }))

    def _run_loop(self):
        """Main reconciliation loop"""
        while self._running:
            try:
                tokens = self.get_active_tokens()

                for token_id in tokens:
                    if not self._running:
                        break

                    result = self.reconciler.poll(token_id)
                    if result:
                        self.reconciler.update_drift_state(token_id, result)

                    # Small delay between tokens to avoid rate limiting
                    time.sleep(0.5)

                # Log health stats periodically
                health = self.reconciler.get_health_stats()
                reconciler_logger.info(json.dumps({
                    'event': 'reconciler_health',
                    **health
                }))

            except Exception as e:
                reconciler_logger.error(json.dumps({
                    'event': 'reconciler_error',
                    'error': str(e),
                }))

            # Wait for next cycle
            time.sleep(self.poll_interval)


# ============ Example Usage ============

if __name__ == '__main__':
    from realtime.book_store import BookStore

    # Initialize
    store = BookStore()
    reconciler = BookReconciler(store, poll_interval=60.0)

    # Demo: seed a book and poll it
    test_token = "21742633143463906290569050155826241533067272736897614950488156847949938836455"

    print("Seeding book via REST...")
    if store.seed_book(test_token):
        print("Book seeded. Polling for drift...")

        result = reconciler.poll(test_token)
        if result:
            print(f"\nDrift result:")
            print(f"  book_drift_pct: {result['book_drift_pct']:.4%}")
            print(f"  action: {result['action']}")
            print(f"  WS bid/ask: {result['ws_bid']:.4f} / {result['ws_ask']:.4f}")
            print(f"  REST bid/ask: {result['rest_bid']:.4f} / {result['rest_ask']:.4f}")

            reconciler.update_drift_state(test_token, result)

            # Query the stored state
            drift = reconciler.get_drift(test_token)
            print(f"\nStored drift state:")
            print(f"  {drift}")
        else:
            print("Failed to poll")
    else:
        print("Failed to seed book")
