"""
Alchemy Live Observer - Async Intent Detection

Observes pending transactions from toxic wallets.
Feeds ToxicFlowTracker, NOT SignalEngine directly.

Key rule: Alchemy can EXTEND a PULL, never INITIATE one alone.

This runs in a separate thread and emits intent events.
The signal engine consumes these to extend cooldowns.

Usage:
    from realtime.alchemy_observer import AlchemyObserver

    observer = AlchemyObserver(
        api_key='...',
        toxic_wallets={'0x...'},
        toxic_flow_tracker=engine.toxic_flow
    )
    observer.start()
"""

import os
import time
import json
import threading
from typing import Callable, Dict, List, Optional, Set
from dataclasses import dataclass
from collections import defaultdict
import websocket


# Polymarket contracts to watch
POLYMARKET_CONTRACTS = {
    '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e'.lower(): 'ctf_exchange',
    '0x4d97dcd97ec945f40cf65f87097ace5ea0476045'.lower(): 'neg_risk_ctf_exchange',
}


@dataclass
class IntentEvent:
    """Detected intent from pending transaction"""
    wallet: str
    contract: str
    tx_hash: str
    timestamp: float
    confidence: float = 0.6  # Pending = uncertain
    estimated_size: float = 0  # Would need calldata decode


class AlchemyObserver:
    """
    Async observer for toxic wallet pending transactions.

    Architecture:
    - Runs in background thread
    - Watches pending txs to Polymarket contracts
    - Filters by known toxic wallets
    - Emits intent events to ToxicFlowTracker

    Key constraint: Alchemy can EXTEND, never INITIATE.
    """

    def __init__(self,
                 api_key: str = None,
                 toxic_wallets: Set[str] = None,
                 toxic_flow_tracker=None,  # ToxicFlowTracker instance
                 cooldown_manager: Dict[str, float] = None,  # pull_cooldowns dict
                 network: str = 'polygon-mainnet'):
        """
        Args:
            api_key: Alchemy API key
            toxic_wallets: Set of wallet addresses to watch
            toxic_flow_tracker: ToxicFlowTracker to feed intent events
            cooldown_manager: Dict of {token_id: last_pull_time} to extend
            network: Alchemy network
        """
        self.api_key = api_key or os.environ.get('ALCHEMY_API_KEY', '')
        self.network = network
        self.ws_url = f'wss://{network}.g.alchemy.com/v2/{self.api_key}'

        self.toxic_wallets = {w.lower() for w in (toxic_wallets or set())}
        self.toxic_flow_tracker = toxic_flow_tracker
        self.cooldown_manager = cooldown_manager or {}

        # State
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self.thread: Optional[threading.Thread] = None

        # Pending tx tracking (for dedup and reorg detection)
        self.pending_txs: Dict[str, IntentEvent] = {}
        self.confirmed_txs: Set[str] = set()
        self.max_pending_age = 120  # Remove pending after 2 min

        # Cooldown extension settings
        self.COOLDOWN_EXTENSION_SECONDS = 3.0

        # Stats
        self.pending_seen = 0
        self.toxic_detected = 0
        self.cooldowns_extended = 0
        self.last_event = 0

    def add_toxic_wallet(self, address: str):
        """Add wallet to watch list"""
        self.toxic_wallets.add(address.lower())

    def remove_toxic_wallet(self, address: str):
        """Remove wallet from watch list"""
        self.toxic_wallets.discard(address.lower())

    def set_toxic_wallets(self, wallets: Set[str]):
        """Replace entire watch list"""
        self.toxic_wallets = {w.lower() for w in wallets}

    def start(self):
        """Start observer in background"""
        if self.running:
            return

        if not self.api_key:
            print("[AlchemyObserver] No API key, not starting")
            return

        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print(f"[AlchemyObserver] Started, watching {len(self.toxic_wallets)} wallets")

    def stop(self):
        """Stop observer"""
        self.running = False
        if self.ws:
            self.ws.close()

    def _run(self):
        """Main loop with reconnection"""
        while self.running:
            try:
                self._connect()
            except Exception as e:
                print(f"[AlchemyObserver] Error: {e}")

            if self.running:
                print("[AlchemyObserver] Reconnecting in 5s...")
                time.sleep(5)

    def _connect(self):
        """Establish WebSocket connection"""
        print("[AlchemyObserver] Connecting...")

        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        self.ws.run_forever(ping_interval=30, ping_timeout=10)

    def _on_open(self, ws):
        """Subscribe to pending transactions"""
        print("[AlchemyObserver] Connected")

        # Subscribe to pending txs TO Polymarket contracts
        msg = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'eth_subscribe',
            'params': [
                'alchemy_pendingTransactions',
                {
                    'toAddress': list(POLYMARKET_CONTRACTS.keys()),
                    'hashesOnly': False
                }
            ]
        }
        ws.send(json.dumps(msg))

    def _on_message(self, ws, message):
        """Handle incoming message"""
        try:
            data = json.loads(message)

            # Subscription confirmation
            if 'result' in data and data.get('id') == 1:
                print(f"[AlchemyObserver] Subscribed: {data['result'][:20]}...")
                return

            # Pending transaction
            if 'params' in data:
                self._handle_pending(data['params'].get('result', {}))

        except Exception as e:
            print(f"[AlchemyObserver] Message error: {e}")

    def _handle_pending(self, tx: dict):
        """Process pending transaction"""
        self.pending_seen += 1
        self._cleanup_old_pending()

        tx_hash = tx.get('hash', '')
        from_addr = (tx.get('from', '') or '').lower()
        to_addr = (tx.get('to', '') or '').lower()

        # Skip if not from toxic wallet
        if from_addr not in self.toxic_wallets:
            return

        # Skip if already seen
        if tx_hash in self.pending_txs or tx_hash in self.confirmed_txs:
            return

        # Toxic wallet pending tx to Polymarket!
        self.toxic_detected += 1
        self.last_event = time.time()

        contract_type = POLYMARKET_CONTRACTS.get(to_addr, 'unknown')

        event = IntentEvent(
            wallet=from_addr,
            contract=contract_type,
            tx_hash=tx_hash,
            timestamp=time.time(),
            confidence=0.6,  # Pending = uncertain
        )

        self.pending_txs[tx_hash] = event

        print(f"[AlchemyObserver] TOXIC INTENT: {from_addr[:10]}... → {contract_type}")

        # Feed to ToxicFlowTracker
        self._emit_intent(event)

    def _emit_intent(self, event: IntentEvent):
        """
        Emit intent to system components.

        Key rule: EXTEND, never INITIATE.
        """
        # Feed toxic flow tracker (if available)
        # This increases intensity but doesn't trigger PULL alone
        if self.toxic_flow_tracker:
            # Use a moderate toxicity score for pending (uncertain)
            # The tracker will use this for burst detection
            for token_id in self._get_active_tokens():
                self.toxic_flow_tracker.record(
                    token_id,
                    toxicity_score=3.0,  # Moderate (pending = uncertain)
                    volume=1.0
                )

        # Extend existing cooldowns (if available)
        # Only EXTEND, never create new ones
        if self.cooldown_manager:
            now = time.time()
            extended = 0

            for token_id, last_pull in list(self.cooldown_manager.items()):
                # Only extend if cooldown is active (within last 10s)
                if now - last_pull < 10.0:
                    # Extend by adding time
                    self.cooldown_manager[token_id] = max(
                        last_pull,
                        now - 5.0 + self.COOLDOWN_EXTENSION_SECONDS
                    )
                    extended += 1

            if extended > 0:
                self.cooldowns_extended += extended
                print(f"[AlchemyObserver] Extended {extended} cooldowns")

    def _get_active_tokens(self) -> List[str]:
        """Get tokens with recent activity (from cooldown manager)"""
        if not self.cooldown_manager:
            return []

        now = time.time()
        return [
            token_id for token_id, last_pull in self.cooldown_manager.items()
            if now - last_pull < 60  # Active in last minute
        ]

    def _cleanup_old_pending(self):
        """Remove old pending transactions"""
        cutoff = time.time() - self.max_pending_age
        to_remove = [
            h for h, e in self.pending_txs.items()
            if e.timestamp < cutoff
        ]
        for h in to_remove:
            del self.pending_txs[h]

    def mark_confirmed(self, tx_hash: str):
        """Mark a pending tx as confirmed (called by WS handler)"""
        if tx_hash in self.pending_txs:
            del self.pending_txs[tx_hash]
        self.confirmed_txs.add(tx_hash)

        # Limit confirmed set size
        if len(self.confirmed_txs) > 10000:
            # Remove oldest half
            self.confirmed_txs = set(list(self.confirmed_txs)[-5000:])

    def invalidate_tx(self, tx_hash: str):
        """
        Invalidate a tx (e.g., reorg detected).
        Do NOT back-propagate to learning.
        """
        if tx_hash in self.pending_txs:
            print(f"[AlchemyObserver] Invalidating tx {tx_hash[:12]}...")
            del self.pending_txs[tx_hash]
        self.confirmed_txs.discard(tx_hash)

    def _on_error(self, ws, error):
        print(f"[AlchemyObserver] Error: {error}")

    def _on_close(self, ws, code, msg):
        print(f"[AlchemyObserver] Closed: {code}")

    def get_stats(self) -> dict:
        return {
            'running': self.running,
            'wallets_watched': len(self.toxic_wallets),
            'pending_seen': self.pending_seen,
            'toxic_detected': self.toxic_detected,
            'cooldowns_extended': self.cooldowns_extended,
            'pending_tracked': len(self.pending_txs),
            'last_event': self.last_event,
        }


def integrate_with_engine(observer: AlchemyObserver, signal_engine):
    """
    Integrate observer with SignalEngine.

    Connects:
    - observer.toxic_flow_tracker → engine.toxic_flow
    - observer.cooldown_manager → engine.pull_cooldowns
    """
    observer.toxic_flow_tracker = signal_engine.toxic_flow
    observer.cooldown_manager = signal_engine.pull_cooldowns

    # Set toxic wallets from engine's toxicity lookup
    toxic_wallets = set()
    for key, data in signal_engine.toxicity.scores.items():
        wallet = key[0]  # (wallet, market_type, expiry, side)
        if data.get('toxicity_score', 0) > 5.0:
            toxic_wallets.add(wallet)

    observer.set_toxic_wallets(toxic_wallets)
    print(f"[AlchemyObserver] Integrated with {len(toxic_wallets)} toxic wallets")


# ============ Standalone Test ============

if __name__ == '__main__':
    # Test with dummy tracker
    class DummyTracker:
        def record(self, token_id, toxicity_score, volume):
            print(f"  [Tracker] {token_id[:12]}... tox={toxicity_score}")

    observer = AlchemyObserver(
        toxic_wallets={'0x1234567890abcdef1234567890abcdef12345678'},
        toxic_flow_tracker=DummyTracker()
    )

    print("Starting observer...")
    observer.start()

    try:
        while True:
            time.sleep(10)
            print(f"Stats: {observer.get_stats()}")
    except KeyboardInterrupt:
        print("\nStopping...")
        observer.stop()
