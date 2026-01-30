"""
Real-Time Book Store

Captures and maintains order book state for Polymarket tokens.

Pattern:
1. On subscribe/reconnect: GET /book to seed in-memory L2 book
2. Apply WebSocket updates to keep current
3. Persist snapshots at configurable intervals

Provides:
- spread at trade time
- depth at best
- microprice
- top-of-book shape
- queue/refresh/churn metrics

Usage:
    from realtime.book_store import BookStore

    store = BookStore()
    store.seed_book(token_id)  # REST call
    store.apply_update(update)  # From WebSocket

    # Query current state
    spread = store.get_spread(token_id)
    microprice = store.get_microprice(token_id)
    depth = store.get_depth_at_level(token_id, level=0)
"""

import time
import json
import requests
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import threading


@dataclass
class PriceLevel:
    """Single price level in the book"""
    price: float
    size: float  # Total size at this level
    orders: int = 1  # Number of orders (if available)


@dataclass
class OrderBook:
    """Full order book for a single token"""
    token_id: str
    bids: List[PriceLevel] = field(default_factory=list)  # Sorted high to low
    asks: List[PriceLevel] = field(default_factory=list)  # Sorted low to high
    last_update_ts: float = 0
    sequence: int = 0  # For detecting gaps

    def best_bid(self) -> Optional[PriceLevel]:
        return self.bids[0] if self.bids else None

    def best_ask(self) -> Optional[PriceLevel]:
        return self.asks[0] if self.asks else None

    def spread(self) -> Optional[float]:
        bb, ba = self.best_bid(), self.best_ask()
        if bb and ba:
            return ba.price - bb.price
        return None

    def mid_price(self) -> Optional[float]:
        bb, ba = self.best_bid(), self.best_ask()
        if bb and ba:
            return (bb.price + ba.price) / 2
        return None

    def microprice(self) -> Optional[float]:
        """
        Size-weighted mid price.
        microprice = (bid_size * ask_price + ask_size * bid_price) / (bid_size + ask_size)
        """
        bb, ba = self.best_bid(), self.best_ask()
        if bb and ba and (bb.size + ba.size) > 0:
            return (bb.size * ba.price + ba.size * bb.price) / (bb.size + ba.size)
        return None

    def imbalance(self) -> Optional[float]:
        """
        Order flow imbalance at top of book.
        Returns: (bid_size - ask_size) / (bid_size + ask_size)
        Range: -1 (all asks) to +1 (all bids)
        """
        bb, ba = self.best_bid(), self.best_ask()
        if bb and ba and (bb.size + ba.size) > 0:
            return (bb.size - ba.size) / (bb.size + ba.size)
        return None

    def depth_at_levels(self, n_levels: int = 5) -> Dict[str, float]:
        """Total size in top N levels"""
        bid_depth = sum(l.size for l in self.bids[:n_levels])
        ask_depth = sum(l.size for l in self.asks[:n_levels])
        return {'bid_depth': bid_depth, 'ask_depth': ask_depth}


class BookStore:
    """
    Manages order books for multiple tokens.
    Thread-safe for concurrent WebSocket updates.
    """

    # Polymarket API endpoints
    BOOK_URL = "https://clob.polymarket.com/book"
    BOOKS_URL = "https://clob.polymarket.com/books"

    def __init__(self, snapshot_interval_seconds: float = 1.0):
        self.books: Dict[str, OrderBook] = {}
        self.snapshot_interval = snapshot_interval_seconds
        self.lock = threading.RLock()

        # Snapshot history for persistence
        self.snapshots: List[dict] = []
        self.max_snapshots = 10000  # Keep last N in memory

        # Tokens needing re-seed due to sequence gaps
        self._needs_reseed: set = set()

    def get_tokens_needing_reseed(self) -> List[str]:
        """Get and clear tokens that need re-seeding due to sequence gaps"""
        with self.lock:
            tokens = list(self._needs_reseed)
            self._needs_reseed.clear()
            return tokens

    def seed_book(self, token_id: str) -> bool:
        """
        Fetch initial book state via REST API.
        Call this on subscribe or reconnect.
        """
        try:
            response = requests.get(
                self.BOOK_URL,
                params={'token_id': token_id},
                timeout=5
            )
            response.raise_for_status()
            data = response.json()

            with self.lock:
                self._parse_book_response(token_id, data)

            return True

        except Exception as e:
            print(f"[BookStore] Failed to seed {token_id}: {e}")
            return False

    def seed_books_batch(self, token_ids: List[str]) -> int:
        """
        Batch fetch multiple books at once.
        More efficient than individual calls.
        """
        try:
            response = requests.post(
                self.BOOKS_URL,
                json=token_ids,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            success_count = 0
            with self.lock:
                for token_id, book_data in data.items():
                    self._parse_book_response(token_id, book_data)
                    success_count += 1

            return success_count

        except Exception as e:
            print(f"[BookStore] Batch seed failed: {e}")
            return 0

    def _parse_book_response(self, token_id: str, data: dict):
        """Parse REST API book response into OrderBook"""
        bids = []
        asks = []

        for bid in data.get('bids', []):
            bids.append(PriceLevel(
                price=float(bid.get('price', 0)),
                size=float(bid.get('size', 0))
            ))

        for ask in data.get('asks', []):
            asks.append(PriceLevel(
                price=float(ask.get('price', 0)),
                size=float(ask.get('size', 0))
            ))

        # Sort: bids high to low, asks low to high
        bids.sort(key=lambda x: -x.price)
        asks.sort(key=lambda x: x.price)

        self.books[token_id] = OrderBook(
            token_id=token_id,
            bids=bids,
            asks=asks,
            last_update_ts=time.time(),
            sequence=data.get('hash', 0)
        )

    def apply_update(self, update: dict) -> bool:
        """
        Apply a WebSocket book update with sequence protection.

        Expected format (from Polymarket WebSocket):
        {
            "asset_id": "token_id",
            "market": "market_id",
            "hash": "sequence_number",
            "changes": [
                {"side": "BUY", "price": "0.55", "size": "100"},
                {"side": "SELL", "price": "0.60", "size": "0"},  # size=0 removes level
            ],
            "timestamp": "..."
        }

        Returns: True if applied, False if stale/gap detected
        """
        token_id = update.get('asset_id', '')
        if not token_id:
            return False

        with self.lock:
            if token_id not in self.books:
                # Book not seeded, ignore update
                return False

            book = self.books[token_id]

            # SEQUENCE PROTECTION: Detect stale or out-of-order updates
            update_seq = update.get('hash', update.get('sequence', 0))
            if update_seq:
                try:
                    update_seq = int(update_seq) if isinstance(update_seq, str) else update_seq
                except:
                    update_seq = 0

                if update_seq and book.sequence:
                    if update_seq <= book.sequence:
                        # Stale update, ignore
                        return False

                    if update_seq > book.sequence + 1:
                        # Gap detected - need to re-seed
                        print(f"[BookStore] Sequence gap on {token_id[:12]}... "
                              f"(had {book.sequence}, got {update_seq}). Re-seeding...")
                        # Schedule re-seed (don't block here)
                        self._needs_reseed.add(token_id)
                        return False

                book.sequence = update_seq

            for change in update.get('changes', []):
                side = change.get('side', '')
                price = float(change.get('price', 0))
                size = float(change.get('size', 0))

                if side == 'BUY':
                    self._apply_level_change(book.bids, price, size, ascending=False)
                elif side == 'SELL':
                    self._apply_level_change(book.asks, price, size, ascending=True)

            book.last_update_ts = time.time()
            return True

    def _apply_level_change(self, levels: List[PriceLevel], price: float,
                            size: float, ascending: bool):
        """Update or remove a price level"""
        # Find existing level
        for i, level in enumerate(levels):
            if abs(level.price - price) < 1e-9:
                if size == 0:
                    # Remove level
                    levels.pop(i)
                else:
                    # Update size
                    level.size = size
                return

        # Level not found, add if size > 0
        if size > 0:
            new_level = PriceLevel(price=price, size=size)
            levels.append(new_level)

            # Re-sort
            if ascending:
                levels.sort(key=lambda x: x.price)
            else:
                levels.sort(key=lambda x: -x.price)

    # ============ Query Methods ============

    def get_spread(self, token_id: str) -> Optional[float]:
        with self.lock:
            book = self.books.get(token_id)
            return book.spread() if book else None

    def get_mid_price(self, token_id: str) -> Optional[float]:
        with self.lock:
            book = self.books.get(token_id)
            return book.mid_price() if book else None

    def get_microprice(self, token_id: str) -> Optional[float]:
        with self.lock:
            book = self.books.get(token_id)
            return book.microprice() if book else None

    def get_imbalance(self, token_id: str) -> Optional[float]:
        with self.lock:
            book = self.books.get(token_id)
            return book.imbalance() if book else None

    def get_depth(self, token_id: str, n_levels: int = 5) -> Optional[Dict]:
        with self.lock:
            book = self.books.get(token_id)
            return book.depth_at_levels(n_levels) if book else None

    def get_book_state(self, token_id: str) -> Optional[dict]:
        """Get full book state for logging/analysis"""
        with self.lock:
            book = self.books.get(token_id)
            if not book:
                return None

            return {
                'token_id': token_id,
                'timestamp': book.last_update_ts,
                'spread': book.spread(),
                'mid_price': book.mid_price(),
                'microprice': book.microprice(),
                'imbalance': book.imbalance(),
                'best_bid': {'price': book.bids[0].price, 'size': book.bids[0].size} if book.bids else None,
                'best_ask': {'price': book.asks[0].price, 'size': book.asks[0].size} if book.asks else None,
                'bid_levels': len(book.bids),
                'ask_levels': len(book.asks),
            }

    # ============ Snapshot/Persistence ============

    def take_snapshot(self, token_ids: List[str] = None):
        """
        Take a snapshot of current book state.
        Called periodically for historical analysis.
        """
        with self.lock:
            ts = time.time()

            if token_ids is None:
                token_ids = list(self.books.keys())

            for token_id in token_ids:
                book = self.books.get(token_id)
                if not book:
                    continue

                snapshot = {
                    'ts': ts,
                    'token_id': token_id,
                    'spread': book.spread(),
                    'mid': book.mid_price(),
                    'microprice': book.microprice(),
                    'imbalance': book.imbalance(),
                    'bb_price': book.bids[0].price if book.bids else None,
                    'bb_size': book.bids[0].size if book.bids else None,
                    'ba_price': book.asks[0].price if book.asks else None,
                    'ba_size': book.asks[0].size if book.asks else None,
                }

                self.snapshots.append(snapshot)

            # Trim old snapshots
            if len(self.snapshots) > self.max_snapshots:
                self.snapshots = self.snapshots[-self.max_snapshots:]

    def get_snapshots(self, token_id: str = None,
                      since_ts: float = 0) -> List[dict]:
        """Get historical snapshots for analysis"""
        with self.lock:
            results = []
            for s in self.snapshots:
                if token_id and s['token_id'] != token_id:
                    continue
                if s['ts'] < since_ts:
                    continue
                results.append(s)
            return results

    def export_snapshots(self, filepath: str):
        """Export snapshots to file"""
        with self.lock:
            with open(filepath, 'w') as f:
                for s in self.snapshots:
                    f.write(json.dumps(s) + '\n')


# ============ Example Integration ============

if __name__ == '__main__':
    # Demo usage
    store = BookStore()

    # Example token (you'd get this from markets.csv)
    test_token = "21742633143463906290569050155826241533067272736897614950488156847949938836455"

    print("Seeding book...")
    if store.seed_book(test_token):
        state = store.get_book_state(test_token)
        print(f"\nBook state:")
        print(f"  Spread: {state['spread']}")
        print(f"  Mid: {state['mid_price']}")
        print(f"  Microprice: {state['microprice']}")
        print(f"  Imbalance: {state['imbalance']}")
        print(f"  Best bid: {state['best_bid']}")
        print(f"  Best ask: {state['best_ask']}")
    else:
        print("Failed to seed book")
