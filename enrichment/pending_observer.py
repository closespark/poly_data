"""
Pending Transaction Observer

Async observer for pending transactions from known toxic wallets.
NOT in hot path - runs in background and feeds signals to ToxicFlowTracker.

Key insight:
- Polymarket WS shows confirmed state
- Alchemy shows intent (pending txs)
- We can detect repeat toxic intent BEFORE it hits

Usage:
    from enrichment.pending_observer import PendingObserver

    observer = PendingObserver(
        api_key='your_key',
        toxic_wallets={'0x...', '0x...'},
        on_pending_toxic=callback
    )
    observer.start()  # Runs in background thread
"""

import os
import time
import json
import threading
from typing import Callable, Dict, List, Optional, Set
from dataclasses import dataclass
import websocket


# Polymarket contract addresses to watch
POLYMARKET_CONTRACTS = {
    '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e'.lower(): 'ctf_exchange',
    '0x4d97dcd97ec945f40cf65f87097ace5ea0476045'.lower(): 'neg_risk_ctf_exchange',
}


@dataclass
class PendingTxEvent:
    """Detected pending transaction from a toxic wallet"""
    tx_hash: str
    from_address: str
    to_address: str
    timestamp: float
    contract_type: str  # 'ctf_exchange', 'neg_risk_ctf_exchange', etc.
    estimated_value: float = 0


class PendingObserver:
    """
    Observes pending transactions for toxic wallet activity.

    Uses Alchemy's pending transaction subscription.
    When a known toxic wallet submits a tx to Polymarket contracts,
    we get notified BEFORE it's confirmed.
    """

    def __init__(self,
                 api_key: str = None,
                 toxic_wallets: Set[str] = None,
                 on_pending_toxic: Callable[[PendingTxEvent], None] = None,
                 network: str = 'polygon-mainnet'):
        """
        Args:
            api_key: Alchemy API key
            toxic_wallets: Set of wallet addresses to watch
            on_pending_toxic: Callback when toxic pending tx detected
            network: Alchemy network
        """
        self.api_key = api_key or os.environ.get('ALCHEMY_API_KEY', '')
        self.network = network
        self.ws_url = f'wss://{network}.g.alchemy.com/v2/{self.api_key}'

        # Normalize addresses
        self.toxic_wallets = {w.lower() for w in (toxic_wallets or set())}
        self.on_pending_toxic = on_pending_toxic

        # State
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self.thread: Optional[threading.Thread] = None

        # Stats
        self.pending_txs_seen = 0
        self.toxic_txs_detected = 0
        self.last_event_time = 0

    def add_toxic_wallet(self, address: str):
        """Add a wallet to watch list"""
        self.toxic_wallets.add(address.lower())

    def remove_toxic_wallet(self, address: str):
        """Remove a wallet from watch list"""
        self.toxic_wallets.discard(address.lower())

    def update_toxic_wallets(self, wallets: Set[str]):
        """Replace entire watch list"""
        self.toxic_wallets = {w.lower() for w in wallets}

    def start(self):
        """Start observer in background thread"""
        if self.running:
            return

        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print(f"[PendingObserver] Started watching {len(self.toxic_wallets)} wallets")

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
                print(f"[PendingObserver] Connection error: {e}")

            if self.running:
                print("[PendingObserver] Reconnecting in 5s...")
                time.sleep(5)

    def _connect(self):
        """Establish WebSocket connection"""
        print("[PendingObserver] Connecting to Alchemy...")

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
        print("[PendingObserver] Connected, subscribing to pending txs...")

        # Subscribe to pending transactions
        # Filter by 'to' addresses (Polymarket contracts)
        subscribe_msg = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'eth_subscribe',
            'params': [
                'alchemy_pendingTransactions',
                {
                    # Watch transactions TO Polymarket contracts
                    'toAddress': list(POLYMARKET_CONTRACTS.keys()),
                    'hashesOnly': False  # We want full tx data
                }
            ]
        }

        ws.send(json.dumps(subscribe_msg))

    def _on_message(self, ws, message):
        """Handle incoming message"""
        try:
            data = json.loads(message)

            # Subscription confirmation
            if 'result' in data and data.get('id') == 1:
                print(f"[PendingObserver] Subscribed: {data['result']}")
                return

            # Pending transaction
            if 'params' in data:
                self._handle_pending_tx(data['params'].get('result', {}))

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[PendingObserver] Message error: {e}")

    def _handle_pending_tx(self, tx: dict):
        """Process a pending transaction"""
        self.pending_txs_seen += 1

        from_addr = (tx.get('from', '') or '').lower()
        to_addr = (tx.get('to', '') or '').lower()

        # Check if from a toxic wallet
        if from_addr not in self.toxic_wallets:
            return

        # It's from a toxic wallet!
        self.toxic_txs_detected += 1
        self.last_event_time = time.time()

        # Determine contract type
        contract_type = POLYMARKET_CONTRACTS.get(to_addr, 'unknown')

        # Estimate value (rough, from gas price × gas limit)
        try:
            gas_price = int(tx.get('gasPrice', '0'), 16)
            gas_limit = int(tx.get('gas', '0'), 16)
            # This is just gas cost, not trade value
            # For trade value, we'd need to decode the calldata
        except:
            pass

        event = PendingTxEvent(
            tx_hash=tx.get('hash', ''),
            from_address=from_addr,
            to_address=to_addr,
            timestamp=time.time(),
            contract_type=contract_type,
        )

        print(f"[PendingObserver] TOXIC PENDING: {from_addr[:12]}... → {contract_type}")

        # Callback
        if self.on_pending_toxic:
            try:
                self.on_pending_toxic(event)
            except Exception as e:
                print(f"[PendingObserver] Callback error: {e}")

    def _on_error(self, ws, error):
        """Handle error"""
        print(f"[PendingObserver] Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """Handle close"""
        print(f"[PendingObserver] Closed: {close_status_code}")

    def get_stats(self) -> dict:
        """Get observer statistics"""
        return {
            'running': self.running,
            'wallets_watched': len(self.toxic_wallets),
            'pending_txs_seen': self.pending_txs_seen,
            'toxic_txs_detected': self.toxic_txs_detected,
            'last_event_time': self.last_event_time,
        }


def integrate_with_signal_engine(observer: PendingObserver,
                                 signal_engine) -> Callable:
    """
    Create a callback that integrates pending observer with signal engine.

    When a toxic pending tx is detected:
    - Extend cooldown on that token (if identifiable)
    - Increment toxic flow tracker

    Returns the callback function (also sets it on observer).
    """

    def on_pending_toxic(event: PendingTxEvent):
        """Handle toxic pending transaction"""
        # For now, just extend cooldowns on ALL tokens this wallet trades
        # In production, you'd decode the tx to find specific tokens

        # Extend cooldown by 3 seconds for all active markets
        # This is conservative but safe
        now = time.time()
        extension = 3.0

        # Find tokens this wallet has traded recently
        # (This would require tracking wallet → token mappings)
        # For now, we just log and let the normal flow handle it

        print(f"[Integration] Toxic pending tx from {event.from_address[:12]}... "
              f"Pre-emptive alert!")

        # Could also boost toxic flow tracker here
        # signal_engine.toxic_flow.record(token_id, estimated_toxicity, 1.0)

    observer.on_pending_toxic = on_pending_toxic
    return on_pending_toxic


# ============ Example Usage ============

if __name__ == '__main__':
    # Demo with some fake toxic wallets
    test_toxic_wallets = {
        '0x1234567890abcdef1234567890abcdef12345678',
        '0xabcdef1234567890abcdef1234567890abcdef12',
    }

    def on_toxic(event):
        print(f"ALERT: {event}")

    observer = PendingObserver(
        toxic_wallets=test_toxic_wallets,
        on_pending_toxic=on_toxic
    )

    print("Starting observer...")
    observer.start()

    try:
        while True:
            time.sleep(10)
            stats = observer.get_stats()
            print(f"Stats: {stats}")
    except KeyboardInterrupt:
        print("\nStopping...")
        observer.stop()
