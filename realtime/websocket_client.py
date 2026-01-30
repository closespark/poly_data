"""
Polymarket WebSocket Client

Connects to Polymarket CLOB WebSocket for real-time updates.

Channels:
- market: Order book updates (best bid/ask changes)
- user: User-specific order updates (for execution)

Usage:
    from realtime.websocket_client import PolymarketWebSocket
    from realtime.book_store import BookStore

    store = BookStore()
    ws = PolymarketWebSocket(store)

    # Subscribe to specific tokens
    ws.subscribe_tokens(['token_id_1', 'token_id_2'])

    # Start (blocks)
    ws.run()
"""

import json
import time
import threading
from typing import List, Set, Callable, Optional
import websocket

from realtime.book_store import BookStore


class PolymarketWebSocket:
    """
    WebSocket client for Polymarket CLOB.

    Handles:
    - Connection management
    - Automatic reconnection
    - Subscription management
    - Message routing to BookStore
    """

    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, book_store: BookStore,
                 on_trade: Optional[Callable] = None,
                 reconnect_delay: float = 5.0):
        """
        Args:
            book_store: BookStore instance to update
            on_trade: Optional callback for trade events
            reconnect_delay: Seconds to wait before reconnecting
        """
        self.book_store = book_store
        self.on_trade = on_trade
        self.reconnect_delay = reconnect_delay

        self.subscribed_tokens: Set[str] = set()
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self.lock = threading.Lock()

        # Stats
        self.messages_received = 0
        self.last_message_ts = 0
        self.connect_count = 0

    def subscribe_tokens(self, token_ids: List[str]):
        """Add tokens to subscription list"""
        with self.lock:
            for tid in token_ids:
                self.subscribed_tokens.add(tid)

        # If already connected, send subscription
        if self.ws and self.running:
            self._send_subscriptions(token_ids)

    def unsubscribe_tokens(self, token_ids: List[str]):
        """Remove tokens from subscription"""
        with self.lock:
            for tid in token_ids:
                self.subscribed_tokens.discard(tid)

        # Send unsubscribe if connected
        if self.ws and self.running:
            msg = {
                "type": "unsubscribe",
                "assets_ids": token_ids
            }
            self.ws.send(json.dumps(msg))

    def run(self):
        """Start WebSocket connection (blocks)"""
        self.running = True

        while self.running:
            try:
                self._connect()
            except Exception as e:
                print(f"[WS] Connection error: {e}")

            if self.running:
                print(f"[WS] Reconnecting in {self.reconnect_delay}s...")
                time.sleep(self.reconnect_delay)

    def stop(self):
        """Stop the WebSocket connection"""
        self.running = False
        if self.ws:
            self.ws.close()

    def run_async(self) -> threading.Thread:
        """Start WebSocket in background thread"""
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        return thread

    def _connect(self):
        """Establish WebSocket connection"""
        self.connect_count += 1
        print(f"[WS] Connecting... (attempt {self.connect_count})")

        self.ws = websocket.WebSocketApp(
            self.WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        self.ws.run_forever(ping_interval=30, ping_timeout=10)

    def _on_open(self, ws):
        """Called when connection established"""
        print(f"[WS] Connected")

        # Seed books before subscribing
        with self.lock:
            tokens = list(self.subscribed_tokens)

        if tokens:
            print(f"[WS] Seeding {len(tokens)} books...")
            seeded = self.book_store.seed_books_batch(tokens)
            print(f"[WS] Seeded {seeded} books")

            # Subscribe
            self._send_subscriptions(tokens)

    def _send_subscriptions(self, token_ids: List[str]):
        """Send subscription message"""
        if not token_ids:
            return

        msg = {
            "type": "subscribe",
            "assets_ids": token_ids
        }
        self.ws.send(json.dumps(msg))
        print(f"[WS] Subscribed to {len(token_ids)} tokens")

    def _on_message(self, ws, message):
        """Handle incoming WebSocket message"""
        self.messages_received += 1
        self.last_message_ts = time.time()

        try:
            data = json.loads(message)
            msg_type = data.get('type', data.get('event_type', ''))

            if msg_type in ['book', 'price_change']:
                # Order book update
                self.book_store.apply_update(data)

            elif msg_type == 'trade':
                # Trade event
                if self.on_trade:
                    self.on_trade(data)

            elif msg_type == 'subscribed':
                print(f"[WS] Subscription confirmed")

            elif msg_type == 'error':
                print(f"[WS] Error: {data.get('message', data)}")

            # Periodic stats
            if self.messages_received % 10000 == 0:
                print(f"[WS] {self.messages_received:,} messages received")

        except json.JSONDecodeError:
            print(f"[WS] Invalid JSON: {message[:100]}")
        except Exception as e:
            print(f"[WS] Message handling error: {e}")

    def _on_error(self, ws, error):
        """Handle WebSocket error"""
        print(f"[WS] Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        print(f"[WS] Closed: {close_status_code} - {close_msg}")

    def get_stats(self) -> dict:
        """Get connection statistics"""
        return {
            'messages_received': self.messages_received,
            'last_message_ts': self.last_message_ts,
            'connect_count': self.connect_count,
            'subscribed_tokens': len(self.subscribed_tokens),
            'books_tracked': len(self.book_store.books),
            'running': self.running,
        }


# ============ Example Usage ============

if __name__ == '__main__':
    import csv
    import os

    # Load some tokens from markets.csv
    DATA_DIR = os.environ.get('DATA_DIR', './data')
    markets_path = os.path.join(DATA_DIR, 'markets.csv')

    tokens_to_track = []

    if os.path.exists(markets_path):
        print(f"Loading tokens from {markets_path}...")
        with open(markets_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticker = row.get('ticker', '').lower()
                # Only track crypto 15m markets
                if 'btc' in ticker and '15m' in ticker:
                    if row.get('token1'):
                        tokens_to_track.append(row['token1'])
                    if row.get('token2'):
                        tokens_to_track.append(row['token2'])

                if len(tokens_to_track) >= 20:  # Limit for demo
                    break

        print(f"Found {len(tokens_to_track)} tokens to track")
    else:
        print("No markets.csv found, using hardcoded test token")
        tokens_to_track = [
            "21742633143463906290569050155826241533067272736897614950488156847949938836455"
        ]

    # Initialize
    store = BookStore()
    ws = PolymarketWebSocket(store)

    # Trade callback
    def on_trade(trade):
        print(f"[TRADE] {trade.get('asset_id', '')[:16]}... "
              f"price={trade.get('price')} size={trade.get('size')}")

    ws.on_trade = on_trade

    # Subscribe
    ws.subscribe_tokens(tokens_to_track)

    # Periodic status printer
    def print_status():
        while ws.running:
            time.sleep(10)
            stats = ws.get_stats()
            print(f"\n[STATUS] msgs={stats['messages_received']:,} "
                  f"books={stats['books_tracked']} "
                  f"tokens={stats['subscribed_tokens']}")

            # Print sample book states
            for token_id in list(store.books.keys())[:3]:
                state = store.get_book_state(token_id)
                if state:
                    print(f"  {token_id[:12]}... spread={state['spread']:.4f} "
                          f"imb={state['imbalance']:.2f}")

    status_thread = threading.Thread(target=print_status, daemon=True)
    status_thread.start()

    # Run (blocks)
    print("\nStarting WebSocket connection...")
    try:
        ws.run()
    except KeyboardInterrupt:
        print("\nStopping...")
        ws.stop()
