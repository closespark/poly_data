"""
Signal Engine - Real-Time Trading Signals

Combines:
- Real-time book state (from BookStore)
- Wallet toxicity scores (from offline analysis)
- Market context (time-to-expiry, market type)

Outputs actionable signals for market making:
- quote/pull liquidity
- widen/tighten spread
- skew inventory
- size adjustments

Usage:
    from realtime.signal_engine import SignalEngine
    from realtime.book_store import BookStore

    store = BookStore()
    engine = SignalEngine(store, toxicity_path='data/wallet_toxicity_takers.csv')

    # On each trade/update
    signal = engine.evaluate(token_id, taker_wallet)
    if signal.action == 'PULL':
        # Cancel quotes
    elif signal.action == 'WIDEN':
        # Increase spread
"""

import csv
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from enum import Enum

from realtime.book_store import BookStore
from realtime.book_reconciler import BookReconciler, ReconcilerRunner, DRIFT_FAIL_THRESHOLD


# Structured loggers - separate streams for decisions vs health
signal_logger = logging.getLogger('signal_engine.decisions')
signal_logger.setLevel(logging.INFO)

health_logger = logging.getLogger('signal_engine.health')
health_logger.setLevel(logging.INFO)


def _generate_decision_id(ts_ms: int, token_id: str, taker: str, trigger: str) -> str:
    """Generate deterministic decision ID for correlation"""
    raw = f"{ts_ms}:{token_id}:{taker}:{trigger}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _clamp01(x: float) -> float:
    """Clamp value to [0, 1]"""
    return max(0.0, min(1.0, x))


class Action(Enum):
    """Trading actions"""
    QUOTE = "QUOTE"      # Normal quoting
    PULL = "PULL"        # Cancel all quotes
    WIDEN = "WIDEN"      # Increase spread
    TIGHTEN = "TIGHTEN"  # Decrease spread (low toxicity)
    SKEW_BID = "SKEW_BID"  # Favor buying
    SKEW_ASK = "SKEW_ASK"  # Favor selling


@dataclass
class Signal:
    """Trading signal with context"""
    action: Action
    confidence: float  # 0-1
    reason: str
    spread_multiplier: float = 1.0  # 1.0 = normal, 2.0 = double spread
    size_multiplier: float = 1.0    # Quote size adjustment

    # Context
    token_id: str = ""
    taker_wallet: str = ""
    toxicity_score: float = 0
    imbalance: float = 0
    spread: float = 0

    # Debug breakdown (for structured logging)
    _debug: Optional[dict] = None

    # Correlation IDs
    decision_id: str = ""
    event_id: str = ""  # Upstream event (trade id, tx hash, ws sequence)

    def to_log_dict(self) -> dict:
        """Convert to structured log dictionary"""
        d = {
            'decision_id': self.decision_id,
            'event_id': self.event_id,
            'action': self.action.value,
            'confidence': round(self.confidence, 3),
            'reason': self.reason,
            'spread_mult': self.spread_multiplier,
            'size_mult': self.size_multiplier,
            'token_id': self.token_id[:16] if self.token_id else '',
            'taker': self.taker_wallet[:12] if self.taker_wallet else '',
            'tox_score': round(self.toxicity_score, 2),
            'imbalance': round(self.imbalance, 3) if self.imbalance else 0,
            'spread': round(self.spread, 4) if self.spread else 0,
        }
        if self._debug:
            d.update(self._debug)  # Flatten debug into top level for easier querying
        return d


class EnrichmentLookup:
    """
    Fast lookup for wallet enrichment data (identity priors).
    Loaded from wallet_enrichment.csv

    Provides behavior_multiplier to modify toxicity scores.
    """

    def __init__(self, filepath: str = None):
        self.multipliers: Dict[str, float] = {}
        self.data: Dict[str, dict] = {}

        if filepath is None:
            filepath = os.path.join(
                os.environ.get('DATA_DIR', './data'),
                'wallet_enrichment.csv'
            )

        if os.path.exists(filepath):
            self._load(filepath)

    def _load(self, filepath: str):
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                wallet = row.get('wallet', '').lower()
                if wallet:
                    mult = float(row.get('behavior_multiplier', 1.0) or 1.0)
                    self.multipliers[wallet] = mult
                    self.data[wallet] = row

        print(f"[EnrichmentLookup] Loaded {len(self.multipliers):,} wallet enrichments")

    def get_multiplier(self, wallet: str) -> float:
        """Get behavior multiplier for wallet (default 1.0)"""
        return self.multipliers.get(wallet.lower(), 1.0)

    def get_data(self, wallet: str) -> Optional[dict]:
        """Get full enrichment data for wallet"""
        return self.data.get(wallet.lower())

    def is_bot(self, wallet: str, threshold: float = 0.7) -> bool:
        """Check if wallet is likely a bot"""
        data = self.data.get(wallet.lower())
        if data:
            return float(data.get('bot_likelihood', 0) or 0) > threshold
        return False

    def is_whale(self, wallet: str) -> bool:
        """Check if wallet is whale tier"""
        data = self.data.get(wallet.lower())
        if data:
            return data.get('capital_tier') == 'whale'
        return False


class ToxicityLookup:
    """
    Fast lookup for wallet toxicity scores.
    Loaded from offline analysis.

    Optionally uses EnrichmentLookup to apply behavior_multiplier.
    """

    def __init__(self, filepath: str, enrichment: EnrichmentLookup = None):
        """Load toxicity scores from CSV"""
        self.scores: Dict[Tuple[str, str, str, str], dict] = {}
        self.filepath = filepath
        self.enrichment = enrichment

        if os.path.exists(filepath):
            self._load()
        else:
            print(f"[ToxicityLookup] Warning: {filepath} not found")

    def _load(self):
        """Load CSV into memory"""
        print(f"[ToxicityLookup] Loading {self.filepath}...")

        with open(self.filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (
                    row['wallet'],
                    row['market_type'],
                    row['expiry_bucket'],
                    row.get('side', 'ALL')  # Handle files without side column
                )
                self.scores[key] = {
                    'toxicity_score': float(row.get('toxicity_score', 0) or 0),
                    'confidence': float(row.get('confidence', 0) or 0),
                    'trade_count': int(row.get('trade_count', 0) or 0),
                    'win_rate_5s': float(row.get('win_rate_5s', 0) or 0),
                    'avg_markout_5s': float(row.get('avg_markout_5s', 0) or 0),
                }

        print(f"[ToxicityLookup] Loaded {len(self.scores):,} wallet contexts")

    def get(self, wallet: str, market_type: str,
            expiry_bucket: str, side: str = 'ALL') -> Optional[dict]:
        """
        Look up toxicity for a wallet in context.

        Falls back through with DECAY (0.7x per fallback level):
        1. Exact match (wallet, market_type, expiry, side) - 1.0x
        2. Without side - 0.85x
        3. Without expiry bucket - 0.7x
        4. Without market type - 0.5x

        This prevents unknown contexts from triggering hard PULLs.
        """
        decay = 1.0

        # Try exact match
        key = (wallet, market_type, expiry_bucket, side)
        if key in self.scores:
            return self._apply_decay(self.scores[key], decay, wallet)

        # Fallback: without side (mild decay)
        decay *= 0.85
        key = (wallet, market_type, expiry_bucket, 'ALL')
        if key in self.scores:
            return self._apply_decay(self.scores[key], decay, wallet)

        # Fallback: any expiry bucket (significant decay)
        decay *= 0.82  # Now ~0.7
        for eb in ['<10m', '10-30m', '>30m', 'no_expiry']:
            if eb == expiry_bucket:
                continue  # Already tried
            key = (wallet, market_type, eb, side)
            if key in self.scores:
                return self._apply_decay(self.scores[key], decay, wallet)
            key = (wallet, market_type, eb, 'ALL')
            if key in self.scores:
                return self._apply_decay(self.scores[key], decay, wallet)

        # Fallback: any market type (heavy decay - very uncertain)
        decay *= 0.7  # Now ~0.5
        for mt in ['crypto_15m', 'crypto_1h', 'crypto_other', 'political', 'other']:
            if mt == market_type:
                continue  # Already tried
            key = (wallet, mt, expiry_bucket, 'ALL')
            if key in self.scores:
                return self._apply_decay(self.scores[key], decay, wallet)

        return None

    # Behavior multiplier bounds (prevent runaway)
    BEHAVIOR_MULT_MIN = 0.8
    BEHAVIOR_MULT_MAX = 1.5

    def _apply_decay(self, data: dict, decay: float, wallet: str = None) -> dict:
        """
        Apply decay multiplier, enrichment multiplier, and confidence.

        Final formula:
        effective_toxicity = base_toxicity × decay × behavior_multiplier × confidence

        Behavior multiplier is bounded to [0.8, 1.5] to prevent enrichment errors
        from dominating outcome-based evidence.
        """
        base_toxicity = data['toxicity_score']
        base_confidence = data['confidence']

        # Apply decay
        confidence = base_confidence * decay

        # Apply enrichment multiplier if available (with bounds)
        enrichment_mult = 1.0
        if self.enrichment and wallet:
            raw_mult = self.enrichment.get_multiplier(wallet)
            # Bound the multiplier to prevent runaway
            enrichment_mult = max(self.BEHAVIOR_MULT_MIN,
                                 min(self.BEHAVIOR_MULT_MAX, raw_mult))

        # FINAL FORMULA: base × decay × enrichment × confidence
        # Confidence directly damps - low confidence should never drive hard actions
        effective_toxicity = base_toxicity * decay * enrichment_mult * confidence

        return {
            'toxicity_score': effective_toxicity,
            'confidence': confidence,
            'trade_count': data['trade_count'],
            'win_rate_5s': data['win_rate_5s'],
            'avg_markout_5s': data['avg_markout_5s'],
            # Debug breakdown
            '_base_toxicity': base_toxicity,
            '_decay_applied': decay,
            '_enrichment_mult': enrichment_mult,
            '_confidence_applied': confidence,
        }

    def is_toxic(self, wallet: str, market_type: str,
                 expiry_bucket: str, threshold: float = 5.0) -> bool:
        """Quick check if wallet is toxic (above threshold)"""
        data = self.get(wallet, market_type, expiry_bucket)
        if data and data['confidence'] > 0.3:
            return data['toxicity_score'] > threshold
        return False

    def is_dumb_money(self, wallet: str, market_type: str,
                      expiry_bucket: str, threshold: float = -3.0) -> bool:
        """Quick check if wallet is consistently wrong"""
        data = self.get(wallet, market_type, expiry_bucket)
        if data and data['confidence'] > 0.3:
            return data['toxicity_score'] < threshold
        return False


class ToxicFlowTracker:
    """
    Tracks toxic flow intensity over rolling windows.
    Used to escalate from WIDEN → PULL and delay re-quoting.
    """

    def __init__(self, window_seconds: float = 30.0):
        self.window = window_seconds
        # {token_id: [(timestamp, toxicity_score, volume), ...]}
        self.events: Dict[str, List[Tuple[float, float, float]]] = defaultdict(list)

    def record(self, token_id: str, toxicity_score: float, volume: float):
        """Record a toxic trade event"""
        now = time.time()
        self.events[token_id].append((now, toxicity_score, volume))
        self._cleanup(token_id, now)

    def _cleanup(self, token_id: str, now: float):
        """Remove old events"""
        cutoff = now - self.window
        self.events[token_id] = [
            e for e in self.events[token_id] if e[0] > cutoff
        ]

    def get_intensity(self, token_id: str) -> dict:
        """
        Get toxic flow intensity for a token.

        Returns:
            count: number of toxic trades in window
            volume: total toxic volume
            avg_toxicity: average toxicity score
            intensity: composite score (count × avg_toxicity)
        """
        now = time.time()
        self._cleanup(token_id, now)

        events = self.events.get(token_id, [])
        if not events:
            return {'count': 0, 'volume': 0, 'avg_toxicity': 0, 'intensity': 0}

        count = len(events)
        volume = sum(e[2] for e in events)
        avg_tox = sum(e[1] for e in events) / count

        # Intensity = burst factor × toxicity
        intensity = count * avg_tox

        return {
            'count': count,
            'volume': volume,
            'avg_toxicity': avg_tox,
            'intensity': intensity,
        }

    def should_escalate(self, token_id: str,
                        count_threshold: int = 3,
                        intensity_threshold: float = 15.0) -> bool:
        """Check if toxic flow is intense enough to escalate"""
        stats = self.get_intensity(token_id)
        return (stats['count'] >= count_threshold or
                stats['intensity'] >= intensity_threshold)


class SignalEngine:
    """
    Generates trading signals based on real-time + offline data.
    """

    # Thresholds (tunable)
    TOXIC_THRESHOLD = 5.0       # Toxicity score above this = dangerous
    DUMB_THRESHOLD = -3.0       # Below this = dumb money (opportunity)
    IMBALANCE_THRESHOLD = 0.5   # OFI above this = directional pressure
    SPREAD_WIDE_THRESHOLD = 0.02  # 2% spread = already wide

    # Toxic flow escalation
    TOXIC_BURST_COUNT = 3       # N toxic trades in window → escalate
    TOXIC_INTENSITY_THRESHOLD = 15.0  # Composite threshold

    def __init__(self, book_store: BookStore,
                 toxicity_path: str = None,
                 enrichment_path: str = None,
                 market_contexts: Dict[str, dict] = None):
        """
        Args:
            book_store: Real-time book state
            toxicity_path: Path to wallet_toxicity_takers.csv
            enrichment_path: Path to wallet_enrichment.csv (optional)
            market_contexts: {token_id: {market_type, expiry_bucket}}
        """
        self.book_store = book_store

        # Load enrichment (optional, for behavior multipliers)
        self.enrichment = None
        if enrichment_path is None:
            enrichment_path = os.path.join(
                os.environ.get('DATA_DIR', './data'),
                'wallet_enrichment.csv'
            )
        if os.path.exists(enrichment_path):
            self.enrichment = EnrichmentLookup(enrichment_path)

        # Load toxicity (with enrichment if available)
        if toxicity_path is None:
            toxicity_path = os.path.join(
                os.environ.get('DATA_DIR', './data'),
                'wallet_toxicity_takers.csv'
            )
        self.toxicity = ToxicityLookup(toxicity_path, enrichment=self.enrichment)

        # Market contexts (should be populated from markets.csv)
        self.market_contexts = market_contexts or {}

        # Toxic flow tracking (for burst detection)
        self.toxic_flow = ToxicFlowTracker(window_seconds=30.0)

        # Cooldown tracking (delay re-quoting after PULL)
        # {token_id: cooldown_until_timestamp}
        self.cooldown_until: Dict[str, float] = {}
        self.PULL_COOLDOWN_SECONDS = 5.0  # Base cooldown duration
        self.MAX_COOLDOWN_EXTENSION = 10.0  # Max total cooldown

        # For backward compat - pull_cooldowns points to same dict
        self.pull_cooldowns = self.cooldown_until

        # Book integrity tracking
        # If book is stale/unseeded, force defensive action
        self.BOOK_STALE_THRESHOLD = 30.0  # Seconds without update = stale

        # REST reconciliation (background validation)
        self.reconciler = BookReconciler(book_store, poll_interval=60.0)
        self._reconciler_runner = None  # Started via start_reconciler()

        # Stats
        self.signals_generated = 0
        self.pulls_triggered = 0
        self.widens_triggered = 0
        self.escalations_triggered = 0
        self.cooldowns_extended = 0
        self.book_integrity_pulls = 0

        # Logging control
        self.log_decisions = True  # Set False to disable structured logging

        # Health logging (periodic stats)
        self._last_health_log = 0
        self.HEALTH_LOG_INTERVAL = 30.0  # Log health every 30s

        # Message rate tracking for health
        self._message_times: List[float] = []
        self._message_window = 60.0  # 1 minute window

    def set_market_context(self, token_id: str, market_type: str,
                           expiry_bucket: str):
        """Set context for a token (call after loading markets)"""
        self.market_contexts[token_id] = {
            'market_type': market_type,
            'expiry_bucket': expiry_bucket
        }

    def start_reconciler(self):
        """
        Start background REST reconciliation.

        Call this after the system is warmed up and you have
        a set of active tokens to monitor.
        """
        if self._reconciler_runner:
            return  # Already running

        def get_active_tokens():
            # Poll tokens that have market contexts set (i.e., we're trading them)
            return list(self.market_contexts.keys())

        self._reconciler_runner = ReconcilerRunner(
            self.reconciler,
            get_active_tokens,
            poll_interval=60.0
        )
        self._reconciler_runner.start()

    def stop_reconciler(self):
        """Stop background REST reconciliation"""
        if self._reconciler_runner:
            self._reconciler_runner.stop()
            self._reconciler_runner = None

    def _log_decision(self, signal: Signal, debug_info: dict = None):
        """
        Log non-QUOTE decisions with structured breakdown.

        Enables surgical threshold tuning by capturing all factors
        that contributed to the decision.
        """
        if not self.log_decisions:
            return

        # Only log non-QUOTE decisions (the interesting ones)
        if signal.action == Action.QUOTE:
            return

        signal._debug = debug_info
        log_entry = signal.to_log_dict()

        # Log as JSON for easy parsing
        signal_logger.info(json.dumps(log_entry))

        # Maybe log health stats
        self._maybe_log_health()

    def _maybe_log_health(self):
        """Log health stats periodically"""
        now = time.time()
        if now - self._last_health_log < self.HEALTH_LOG_INTERVAL:
            return

        self._last_health_log = now
        health_logger.info(json.dumps(self._get_health_stats()))

    def _get_health_stats(self) -> dict:
        """Get health statistics for periodic logging"""
        now = time.time()

        # Calculate message rate
        cutoff = now - self._message_window
        self._message_times = [t for t in self._message_times if t > cutoff]
        msg_rate = len(self._message_times) / self._message_window * 60  # per minute

        # Calculate average book age
        book_ages = []
        for token_id, book in self.book_store.books.items():
            age_ms = (now - book.last_update_ts) * 1000
            book_ages.append(age_ms)
        avg_book_age_ms = sum(book_ages) / len(book_ages) if book_ages else 0

        # Get reconciler stats
        reconciler_stats = self.reconciler.get_health_stats()

        # Calculate pulls/widens per minute (rough, from cumulative stats)
        # In production you'd track these in rolling windows
        return {
            'event': 'health',
            'ts': now,
            'tokens_tracked': len(self.book_store.books),
            'contexts_loaded': len(self.toxicity.scores),
            'market_contexts_set': len(self.market_contexts),
            'msg_rate_per_min': round(msg_rate, 1),
            'avg_book_age_ms': round(avg_book_age_ms, 1),
            'reseed_queue_len': len(self.book_store._needs_reseed) if hasattr(self.book_store, '_needs_reseed') else 0,
            'active_cooldowns': sum(1 for t, until in self.cooldown_until.items() if until > now),
            'total_signals': self.signals_generated,
            'total_pulls': self.pulls_triggered,
            'total_escalations': self.escalations_triggered,
            'total_widens': self.widens_triggered,
            'total_book_integrity': self.book_integrity_pulls,
            'total_cooldown_extensions': self.cooldowns_extended,
            # REST reconciliation stats
            'reconciler_polls': reconciler_stats['polls_total'],
            'reconciler_failures': reconciler_stats['polls_failed'],
            'reconciler_drift_warnings': reconciler_stats['drift_warnings'],
            'reconciler_drift_failures': reconciler_stats['drift_failures'],
            'avg_drift_pct': reconciler_stats['avg_drift_pct'],
            'max_drift_pct': reconciler_stats['max_drift_pct'],
        }

    def record_message(self):
        """Call this when processing a WS message (for rate tracking)"""
        self._message_times.append(time.time())

    def extend_cooldown(self, token_id: str, extra_seconds: float,
                        reason: str = "external") -> bool:
        """
        EXTEND an existing cooldown (never INITIATE).

        This is the ONLY way external systems (like Alchemy observer)
        should interact with cooldowns.

        Rules:
        1. Can only extend if cooldown is currently active OR
           there was a PULL in the last 10 seconds
        2. Cannot extend beyond MAX_COOLDOWN_EXTENSION total
        3. Logs the extension with reason

        Returns: True if extended, False if no active cooldown to extend
        """
        now = time.time()
        current_until = self.cooldown_until.get(token_id, 0)

        # Rule 1: Must have active or recent cooldown
        # "Recent" means cooldown ended within last 10 seconds
        if current_until < now - 10.0:
            # No active or recent cooldown - cannot initiate
            return False

        # Calculate new cooldown end
        if current_until > now:
            # Active cooldown - extend from current end
            new_until = current_until + extra_seconds
        else:
            # Recent cooldown (ended within 10s) - extend from now
            new_until = now + extra_seconds

        # Rule 2: Cap at max extension from original PULL
        # Find when the original pull happened
        original_pull_time = current_until - self.PULL_COOLDOWN_SECONDS
        max_until = original_pull_time + self.MAX_COOLDOWN_EXTENSION

        new_until = min(new_until, max_until)

        # Only update if actually extending
        if new_until > current_until:
            cooldown_before = current_until - now if current_until > now else 0
            cooldown_after = new_until - now
            extension_applied = new_until - max(current_until, now)

            self.cooldown_until[token_id] = new_until
            self.cooldowns_extended += 1

            # Log the extension with before/after
            if self.log_decisions:
                signal_logger.info(json.dumps({
                    'event': 'cooldown_extended',
                    'ts': now,
                    'token_id': token_id[:16] if token_id else '',
                    'reason': reason,
                    'extra_seconds_requested': round(extra_seconds, 2),
                    'cooldown_before_s': round(cooldown_before, 2),
                    'cooldown_after_s': round(cooldown_after, 2),
                    'extension_applied_s': round(extension_applied, 2),
                    'hit_max_cap': new_until >= max_until - 0.01,
                }))
            return True

        return False

    def _check_book_integrity(self, token_id: str) -> dict:
        """
        Check if book state is reliable.

        Includes REST reconciliation check when available.

        Returns dict with:
            ok: bool
            reason: str
            book_seq: last sequence number
            book_age_ms: milliseconds since last update
            book_drift_pct: drift vs REST (if available)
        """
        book = self.book_store.books.get(token_id)
        now = time.time()

        # Default drift info
        drift_info = {
            'book_drift_pct': None,
            'drift_action': None,
            'drift_stale': None,
        }

        # Get drift from reconciler (fast lookup, never blocks)
        drift_state = self.reconciler.get_drift(token_id)
        if drift_state:
            drift_info['book_drift_pct'] = drift_state['book_drift_pct']
            drift_info['drift_action'] = drift_state['action']
            drift_info['drift_stale'] = drift_state['stale']

        if book is None:
            return {
                'ok': False,
                'reason': 'book_unseeded',
                'book_seq': 0,
                'book_age_ms': -1,
                **drift_info,
            }

        book_age_ms = (now - book.last_update_ts) * 1000
        book_seq = getattr(book, 'last_sequence', 0)

        # Check if book is stale
        if now - book.last_update_ts > self.BOOK_STALE_THRESHOLD:
            return {
                'ok': False,
                'reason': 'book_stale',
                'book_seq': book_seq,
                'book_age_ms': round(book_age_ms, 1),
                **drift_info,
            }

        # Check if pending reseed (from sequence gap OR drift failure)
        if hasattr(self.book_store, '_needs_reseed') and token_id in self.book_store._needs_reseed:
            return {
                'ok': False,
                'reason': 'pending_reseed',
                'book_seq': book_seq,
                'book_age_ms': round(book_age_ms, 1),
                **drift_info,
            }

        # Check if book has any levels
        if not book.bids and not book.asks:
            return {
                'ok': False,
                'reason': 'book_empty',
                'book_seq': book_seq,
                'book_age_ms': round(book_age_ms, 1),
                **drift_info,
            }

        # NEW: Check REST drift (if we have non-stale drift data)
        # This is the key addition - REST disagreement trips the gate
        if drift_state and not drift_state['stale']:
            if drift_state['book_drift_pct'] > DRIFT_FAIL_THRESHOLD:
                return {
                    'ok': False,
                    'reason': 'rest_drift',
                    'book_seq': book_seq,
                    'book_age_ms': round(book_age_ms, 1),
                    **drift_info,
                }

        return {
            'ok': True,
            'reason': 'ok',
            'book_seq': book_seq,
            'book_age_ms': round(book_age_ms, 1),
            **drift_info,
        }

    def evaluate(self, token_id: str, taker_wallet: str,
                 trade_side: str = None, event_id: str = "") -> Signal:
        """
        Evaluate current state and generate signal.

        Args:
            token_id: Token being traded
            taker_wallet: Wallet that initiated trade
            trade_side: 'BUY' or 'SELL' if known
            event_id: Upstream event identifier (trade id, tx hash, ws sequence)

        Returns:
            Signal with recommended action
        """
        self.signals_generated += 1
        self.record_message()  # Track for rate calculation

        # Get book state
        spread = self.book_store.get_spread(token_id)
        imbalance = self.book_store.get_imbalance(token_id)
        microprice = self.book_store.get_microprice(token_id)

        # Get market context
        ctx = self.market_contexts.get(token_id, {})
        market_type = ctx.get('market_type', 'other')
        expiry_bucket = ctx.get('expiry_bucket', 'no_expiry')

        # Get toxicity
        tox_data = self.toxicity.get(
            taker_wallet, market_type, expiry_bucket,
            trade_side or 'ALL'
        )

        toxicity_score = tox_data['toxicity_score'] if tox_data else 0
        tox_confidence = tox_data['confidence'] if tox_data else 0

        # Store event_id for signal
        _event_id = event_id

        now = time.time()
        ts_ms = int(now * 1000)

        # Get burst intensity early (needed for risk_score)
        burst_info = self.toxic_flow.get_intensity(token_id)

        # Check book integrity (returns dict with details)
        book_state = self._check_book_integrity(token_id)

        # Cooldown state
        cooldown_until = self.cooldown_until.get(token_id, 0)
        cooldown_remaining = max(0, cooldown_until - now)

        # ========== Compute Risk Score ==========
        # Normalized "how scared am I" for logging and future tuning
        # risk_score = 0.6 * tox_factor + 0.3 * burst_factor + 0.1 * book_factor
        tox_factor = _clamp01(toxicity_score / self.TOXIC_THRESHOLD) if self.TOXIC_THRESHOLD > 0 else 0
        burst_factor = _clamp01(burst_info['intensity'] / self.TOXIC_INTENSITY_THRESHOLD) if self.TOXIC_INTENSITY_THRESHOLD > 0 else 0
        book_factor = 0.0 if book_state['ok'] else 1.0
        risk_score = 0.6 * tox_factor + 0.3 * burst_factor + 0.1 * book_factor

        # ========== Build Debug Info ==========
        # Collect all factors that COULD trigger, but we'll pick one primary
        triggers_true = []
        if not book_state['ok']:
            triggers_true.append('book_integrity')
        if cooldown_remaining > 0:
            triggers_true.append('cooldown_active')
        if tox_data and tox_confidence > 0.5 and toxicity_score > self.TOXIC_THRESHOLD:
            triggers_true.append('toxic_wallet')
        if toxicity_score > 2.0 and self.toxic_flow.should_escalate(token_id, self.TOXIC_BURST_COUNT, self.TOXIC_INTENSITY_THRESHOLD):
            triggers_true.append('toxic_burst')
        if imbalance is not None and abs(imbalance) > self.IMBALANCE_THRESHOLD and toxicity_score > 2.0:
            triggers_true.append('imbalance_toxicity')
        if expiry_bucket == '<10m':
            triggers_true.append('near_expiry')
        if tox_data and tox_confidence > 0.5 and toxicity_score < self.DUMB_THRESHOLD:
            triggers_true.append('dumb_money')

        debug_info = {
            'ts': now,
            # Toxicity breakdown
            'base_tox': round(tox_data.get('_base_toxicity', 0), 2) if tox_data else 0,
            'decay': round(tox_data.get('_decay_applied', 1.0), 3) if tox_data else 1.0,
            'enrich_mult': round(tox_data.get('_enrichment_mult', 1.0), 3) if tox_data else 1.0,
            'tox_confidence': round(tox_data.get('_confidence_applied', 0), 3) if tox_data else 0,
            'effective_tox': round(toxicity_score, 2),
            # Context
            'market_type': market_type,
            'expiry_bucket': expiry_bucket,
            'trade_side': trade_side or 'ALL',
            # Book state
            'book_ok': book_state['ok'],
            'book_reason': book_state['reason'],
            'book_seq': book_state['book_seq'],
            'book_age_ms': book_state['book_age_ms'],
            # REST reconciliation (from background poller)
            'book_drift_pct': round(book_state.get('book_drift_pct', 0) or 0, 5),
            'drift_action': book_state.get('drift_action'),
            'drift_stale': book_state.get('drift_stale'),
            # Burst state
            'burst_count': burst_info['count'],
            'burst_intensity': round(burst_info['intensity'], 2),
            # Cooldown state
            'cooldown_remaining_s': round(cooldown_remaining, 2),
            # Composite risk score
            'risk_score': round(risk_score, 3),
            # Trigger info (will set trigger_primary below)
            'triggers_true': triggers_true,
        }

        # ========== Signal Logic ==========
        # Each branch sets trigger_primary and returns

        # Priority -1: Book integrity gate (safety rail)
        if not book_state['ok']:
            self.book_integrity_pulls += 1
            debug_info['trigger_primary'] = 'book_integrity'
            debug_info['trigger_secondary'] = [t for t in triggers_true if t != 'book_integrity']
            decision_id = _generate_decision_id(ts_ms, token_id, taker_wallet, 'book_integrity')
            signal = Signal(
                action=Action.WIDEN,  # WIDEN not PULL - we're blind, not under attack
                confidence=0.95,
                reason=f"Book integrity: {book_state['reason']}",
                spread_multiplier=2.0,
                size_multiplier=0.3,
                token_id=token_id,
                taker_wallet=taker_wallet,
                toxicity_score=toxicity_score,
                imbalance=imbalance or 0,
                spread=spread or 0,
                decision_id=decision_id,
                event_id=_event_id,
            )
            self._log_decision(signal, debug_info)
            return signal

        # Priority 0: Cooldown active
        if cooldown_remaining > 0:
            debug_info['trigger_primary'] = 'cooldown_active'
            debug_info['trigger_secondary'] = [t for t in triggers_true if t != 'cooldown_active']
            decision_id = _generate_decision_id(ts_ms, token_id, taker_wallet, 'cooldown')
            signal = Signal(
                action=Action.PULL,
                confidence=0.9,
                reason=f"Cooldown active ({cooldown_remaining:.1f}s remaining)",
                spread_multiplier=2.0,
                size_multiplier=0.0,
                token_id=token_id,
                taker_wallet=taker_wallet,
                toxicity_score=toxicity_score,
                imbalance=imbalance or 0,
                spread=spread or 0,
                decision_id=decision_id,
                event_id=_event_id,
            )
            self._log_decision(signal, debug_info)
            return signal

        # Priority 1: Known toxic wallet with high confidence
        if tox_data and tox_confidence > 0.5 and toxicity_score > self.TOXIC_THRESHOLD:
            self.toxic_flow.record(token_id, toxicity_score, 1.0)
            self.cooldown_until[token_id] = now + self.PULL_COOLDOWN_SECONDS
            self.pulls_triggered += 1

            debug_info['trigger_primary'] = 'toxic_wallet'
            debug_info['trigger_secondary'] = [t for t in triggers_true if t != 'toxic_wallet']
            debug_info['threshold_toxic'] = self.TOXIC_THRESHOLD
            decision_id = _generate_decision_id(ts_ms, token_id, taker_wallet, 'toxic_wallet')
            signal = Signal(
                action=Action.PULL,
                confidence=tox_confidence,
                reason=f"Toxic wallet (score={toxicity_score:.1f})",
                spread_multiplier=2.0,
                size_multiplier=0.0,
                token_id=token_id,
                taker_wallet=taker_wallet,
                toxicity_score=toxicity_score,
                imbalance=imbalance or 0,
                spread=spread or 0,
                decision_id=decision_id,
                event_id=_event_id,
            )
            self._log_decision(signal, debug_info)
            return signal

        # Priority 1.5: Toxic flow burst
        if toxicity_score > 2.0:
            self.toxic_flow.record(token_id, toxicity_score, 1.0)
            # Refresh burst info after recording
            burst_info = self.toxic_flow.get_intensity(token_id)
            debug_info['burst_count'] = burst_info['count']
            debug_info['burst_intensity'] = round(burst_info['intensity'], 2)

            if self.toxic_flow.should_escalate(token_id, self.TOXIC_BURST_COUNT, self.TOXIC_INTENSITY_THRESHOLD):
                self.cooldown_until[token_id] = now + self.PULL_COOLDOWN_SECONDS
                self.escalations_triggered += 1

                debug_info['trigger_primary'] = 'toxic_burst'
                debug_info['trigger_secondary'] = [t for t in triggers_true if t != 'toxic_burst']
                debug_info['threshold_burst_count'] = self.TOXIC_BURST_COUNT
                debug_info['threshold_burst_intensity'] = self.TOXIC_INTENSITY_THRESHOLD
                decision_id = _generate_decision_id(ts_ms, token_id, taker_wallet, 'toxic_burst')
                signal = Signal(
                    action=Action.PULL,
                    confidence=0.8,
                    reason=f"Toxic burst ({burst_info['count']} trades, intensity={burst_info['intensity']:.1f})",
                    spread_multiplier=2.0,
                    size_multiplier=0.0,
                    token_id=token_id,
                    taker_wallet=taker_wallet,
                    toxicity_score=toxicity_score,
                    imbalance=imbalance or 0,
                    spread=spread or 0,
                    decision_id=decision_id,
                    event_id=_event_id,
                )
                self._log_decision(signal, debug_info)
                return signal

        # Priority 2: Extreme imbalance + moderate toxicity
        if imbalance is not None and abs(imbalance) > self.IMBALANCE_THRESHOLD:
            if toxicity_score > 2.0:
                self.widens_triggered += 1
                skew = Action.SKEW_BID if imbalance > 0 else Action.SKEW_ASK

                debug_info['trigger_primary'] = 'imbalance_toxicity'
                debug_info['trigger_secondary'] = [t for t in triggers_true if t != 'imbalance_toxicity']
                debug_info['threshold_imbalance'] = self.IMBALANCE_THRESHOLD
                decision_id = _generate_decision_id(ts_ms, token_id, taker_wallet, 'imbalance')
                signal = Signal(
                    action=skew,
                    confidence=0.7,
                    reason=f"High imbalance ({imbalance:.2f}) + toxicity ({toxicity_score:.1f})",
                    spread_multiplier=1.5,
                    size_multiplier=0.5,
                    token_id=token_id,
                    taker_wallet=taker_wallet,
                    toxicity_score=toxicity_score,
                    imbalance=imbalance,
                    spread=spread or 0,
                    decision_id=decision_id,
                    event_id=_event_id,
                )
                self._log_decision(signal, debug_info)
                return signal

        # Priority 3: Near expiry
        if expiry_bucket == '<10m':
            self.widens_triggered += 1

            debug_info['trigger_primary'] = 'near_expiry'
            debug_info['trigger_secondary'] = [t for t in triggers_true if t != 'near_expiry']
            decision_id = _generate_decision_id(ts_ms, token_id, taker_wallet, 'near_expiry')
            signal = Signal(
                action=Action.WIDEN,
                confidence=0.8,
                reason="Near expiry (<10m)",
                spread_multiplier=1.5,
                size_multiplier=0.7,
                token_id=token_id,
                taker_wallet=taker_wallet,
                toxicity_score=toxicity_score,
                imbalance=imbalance or 0,
                spread=spread or 0,
                decision_id=decision_id,
                event_id=_event_id,
            )
            self._log_decision(signal, debug_info)
            return signal

        # Priority 4: Dumb money = opportunity
        if tox_data and tox_confidence > 0.5 and toxicity_score < self.DUMB_THRESHOLD:
            debug_info['trigger_primary'] = 'dumb_money'
            debug_info['trigger_secondary'] = [t for t in triggers_true if t != 'dumb_money']
            debug_info['threshold_dumb'] = self.DUMB_THRESHOLD
            decision_id = _generate_decision_id(ts_ms, token_id, taker_wallet, 'dumb_money')
            signal = Signal(
                action=Action.TIGHTEN,
                confidence=tox_confidence,
                reason=f"Dumb money (score={toxicity_score:.1f})",
                spread_multiplier=0.8,
                size_multiplier=1.5,
                token_id=token_id,
                taker_wallet=taker_wallet,
                toxicity_score=toxicity_score,
                imbalance=imbalance or 0,
                spread=spread or 0,
                decision_id=decision_id,
                event_id=_event_id,
            )
            self._log_decision(signal, debug_info)
            return signal

        # Default: Normal quoting
        return Signal(
            action=Action.QUOTE,
            confidence=0.5,
            reason="Normal conditions",
            spread_multiplier=1.0,
            size_multiplier=1.0,
            token_id=token_id,
            taker_wallet=taker_wallet,
            toxicity_score=toxicity_score,
            imbalance=imbalance or 0,
            spread=spread or 0
        )

    def evaluate_preemptive(self, token_id: str) -> Signal:
        """
        Evaluate without a specific taker (for preemptive quoting decisions).
        Based purely on book state and market context.
        """
        spread = self.book_store.get_spread(token_id)
        imbalance = self.book_store.get_imbalance(token_id)

        ctx = self.market_contexts.get(token_id, {})
        expiry_bucket = ctx.get('expiry_bucket', 'no_expiry')

        # Near expiry
        if expiry_bucket == '<10m':
            return Signal(
                action=Action.WIDEN,
                confidence=0.7,
                reason="Preemptive: near expiry",
                spread_multiplier=1.5,
                size_multiplier=0.7,
                token_id=token_id,
                imbalance=imbalance or 0,
                spread=spread or 0
            )

        # Extreme imbalance
        if imbalance is not None and abs(imbalance) > 0.7:
            return Signal(
                action=Action.SKEW_BID if imbalance > 0 else Action.SKEW_ASK,
                confidence=0.6,
                reason=f"Preemptive: extreme imbalance ({imbalance:.2f})",
                spread_multiplier=1.3,
                size_multiplier=0.8,
                token_id=token_id,
                imbalance=imbalance,
                spread=spread or 0
            )

        # Normal
        return Signal(
            action=Action.QUOTE,
            confidence=0.5,
            reason="Preemptive: normal",
            spread_multiplier=1.0,
            size_multiplier=1.0,
            token_id=token_id,
            imbalance=imbalance or 0,
            spread=spread or 0
        )

    def get_stats(self) -> dict:
        return {
            'signals_generated': self.signals_generated,
            'pulls_triggered': self.pulls_triggered,
            'escalations_triggered': self.escalations_triggered,
            'widens_triggered': self.widens_triggered,
            'toxicity_contexts_loaded': len(self.toxicity.scores),
            'market_contexts_set': len(self.market_contexts),
            'active_cooldowns': sum(1 for t in self.pull_cooldowns.values()
                                   if time.time() - t < self.PULL_COOLDOWN_SECONDS),
        }


# ============ Example Usage ============

if __name__ == '__main__':
    from realtime.book_store import BookStore

    # Initialize
    store = BookStore()
    engine = SignalEngine(store)

    # Simulate some scenarios
    print("Signal Engine Demo\n")

    # Set up a test token context
    test_token = "test_token_123"
    engine.set_market_context(test_token, 'crypto_15m', '<10m')

    # Simulate a trade from unknown wallet
    signal = engine.evaluate(test_token, "0xunknown_wallet", "BUY")
    print(f"Unknown wallet: {signal.action.value}")
    print(f"  Reason: {signal.reason}")
    print(f"  Spread mult: {signal.spread_multiplier}")

    # Change context to normal expiry
    engine.set_market_context(test_token, 'crypto_15m', '>30m')
    signal = engine.evaluate(test_token, "0xunknown_wallet", "BUY")
    print(f"\nSame wallet, normal expiry: {signal.action.value}")
    print(f"  Reason: {signal.reason}")

    # Stats
    print(f"\nEngine stats: {engine.get_stats()}")
