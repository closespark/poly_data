"""
Build the Sweep Table - Canonical Unit of Analysis

Each sweep = one taker aggressing against the book
Multiple orderFilled rows → one sweep row

Output: sweeps.parquet (or sweeps.csv)

Columns:
- sweep_id (transactionHash)
- timestamp
- market_id
- token_id
- token_position (token1/token2)
- direction (BUY/SELL from taker perspective)
- taker_wallet
- total_usdc
- total_tokens
- price_first (best level hit)
- price_last (deepest level hit)
- price_vwap (realized execution)
- price_range (slippage = |last - first|)
- n_matches (fills in sweep)
- unique_makers (distinct LPs consumed)
- depth_touch_lb (lower bound on L1 depth)
- thickness (total_usdc / price_range, book thickness proxy)
- aggression (total_usdc / n_matches)
- time_to_expiry_seconds
- time_to_expiry_bucket
- market_type
- maker_wallets (JSON array)
"""

import csv
import os
import json
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

DATA_DIR = os.environ.get('DATA_DIR', './data')
FLUSH_INTERVAL = 100_000  # Process pending sweeps every N rows
MAX_SWEEP_AGE_SECONDS = 10  # Sweeps older than this are complete

def get_data_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


# ============ Market Context ============

def load_markets() -> Dict[str, dict]:
    """Load markets.csv into lookup dict"""
    markets = {}
    path = get_data_path('markets.csv')
    print(f"Loading markets from {path}...")

    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mid = row.get('id', '')
            if mid:
                markets[mid] = {
                    'token1': row.get('token1', ''),
                    'token2': row.get('token2', ''),
                    'closedTime': row.get('closedTime', ''),
                    'ticker': row.get('ticker', ''),
                    'question': row.get('question', ''),
                }
    print(f"  Loaded {len(markets):,} markets")
    return markets


def build_token_map(markets: Dict) -> Dict[str, Tuple[str, str]]:
    """token_id -> (market_id, 'token1'|'token2')"""
    tmap = {}
    for mid, m in markets.items():
        if m['token1']:
            tmap[m['token1']] = (mid, 'token1')
        if m['token2']:
            tmap[m['token2']] = (mid, 'token2')
    print(f"  Built token map: {len(tmap):,} tokens")
    return tmap


def classify_market(ticker: str, question: str) -> str:
    """Classify market type from metadata"""
    t = (ticker or '').lower()
    q = (question or '').lower()

    crypto = ['btc', 'eth', 'bitcoin', 'ethereum', 'sol', 'crypto']
    if any(kw in t or kw in q for kw in crypto):
        if '15m' in t or '15 min' in q:
            return 'crypto_15m'
        if '1h' in t or '1 hour' in q:
            return 'crypto_1h'
        if '4h' in t or '4 hour' in q:
            return 'crypto_4h'
        return 'crypto_other'

    political = ['president', 'election', 'trump', 'biden', 'vote', 'congress']
    if any(kw in q for kw in political):
        return 'political'

    sports = ['nba', 'nfl', 'mlb', 'game', 'match', 'championship']
    if any(kw in q for kw in sports):
        return 'sports'

    return 'other'


def get_expiry_bucket(trade_ts: int, closed_time: str) -> Tuple[Optional[int], str]:
    """
    Returns: (seconds_to_expiry, bucket_name)
    Buckets: '>30m', '10-30m', '<10m', 'expired', 'no_expiry'
    """
    if not closed_time:
        return None, 'no_expiry'

    try:
        if closed_time.isdigit():
            close_ts = int(closed_time) // 1000
        else:
            close_ts = int(datetime.fromisoformat(
                closed_time.replace('Z', '+00:00')
            ).timestamp())

        tte = close_ts - trade_ts
        if tte < 0:
            return tte, 'expired'
        elif tte < 600:
            return tte, '<10m'
        elif tte < 1800:
            return tte, '10-30m'
        else:
            return tte, '>30m'
    except:
        return None, 'parse_error'


# ============ Fill Processing ============

def get_fill_price_and_amounts(row: dict) -> Optional[Tuple[float, float, float, str]]:
    """
    Extract normalized price and amounts from a fill row.

    Returns: (price, usdc_amount, token_amount, direction)
    - price: probability 0-1
    - usdc_amount: in actual USDC (not micro)
    - token_amount: in actual tokens
    - direction: 'BUY' if taker buying tokens, 'SELL' if taker selling tokens
    """
    try:
        maker_amount = float(row.get('makerAmountFilled', 0))
        taker_amount = float(row.get('takerAmountFilled', 0))
        maker_asset = row.get('makerAssetId', '')
        taker_asset = row.get('takerAssetId', '')

        if maker_amount == 0 or taker_amount == 0:
            return None

        # USDC has short asset ID, tokens have long IDs
        maker_is_usdc = len(maker_asset) < 10
        taker_is_usdc = len(taker_asset) < 10

        if maker_is_usdc:
            # Maker gave USDC, taker gave tokens
            # Taker is SELLING tokens
            usdc = maker_amount / 1e6  # Convert from micro
            tokens = taker_amount / 1e6
            direction = 'SELL'
            token_id = taker_asset
        elif taker_is_usdc:
            # Taker gave USDC, maker gave tokens
            # Taker is BUYING tokens
            usdc = taker_amount / 1e6
            tokens = maker_amount / 1e6
            direction = 'BUY'
            token_id = maker_asset
        else:
            # Neither is USDC? Shouldn't happen
            return None

        if tokens == 0:
            return None

        price = usdc / tokens

        # Sanity: price should be 0-2 (allow some edge cases)
        if not (0 < price < 2):
            return None

        return (price, usdc, tokens, direction, token_id)

    except (ValueError, TypeError):
        return None


# ============ Sweep Builder ============

class SweepBuilder:
    """
    Aggregates fills by transactionHash into sweeps.
    Computes all sweep-level features.
    """

    def __init__(self, markets: Dict, token_map: Dict):
        self.markets = markets
        self.token_map = token_map

        # Pending: {tx_hash: {'fills': [...], 'first_ts': int}}
        self.pending: Dict[str, dict] = defaultdict(lambda: {'fills': [], 'first_ts': float('inf')})

        # Completed sweeps ready to write
        self.completed: List[dict] = []

        # Stats
        self.total_fills = 0
        self.total_sweeps = 0

    def add_fill(self, row: dict):
        """Add a fill row to pending sweeps"""
        tx_hash = row.get('transactionHash', '')
        if not tx_hash:
            return

        ts = int(row.get('timestamp', 0))
        self.pending[tx_hash]['fills'].append(row)
        self.pending[tx_hash]['first_ts'] = min(self.pending[tx_hash]['first_ts'], ts)
        self.total_fills += 1

    def flush(self, current_ts: int, force: bool = False):
        """
        Process sweeps that are old enough to be complete.
        """
        max_age = 0 if force else MAX_SWEEP_AGE_SECONDS
        to_remove = []

        for tx_hash, data in self.pending.items():
            age = current_ts - data['first_ts']
            if age > max_age or force:
                sweep = self._build_sweep(tx_hash, data['fills'])
                if sweep:
                    self.completed.append(sweep)
                    self.total_sweeps += 1
                to_remove.append(tx_hash)

        for tx_hash in to_remove:
            del self.pending[tx_hash]

    def _build_sweep(self, tx_hash: str, fills: List[dict]) -> Optional[dict]:
        """
        Build a single sweep record from its fills.
        """
        if not fills:
            return None

        # Extract price/amount from each fill
        fill_data = []
        for f in fills:
            parsed = get_fill_price_and_amounts(f)
            if parsed:
                price, usdc, tokens, direction, token_id = parsed
                fill_data.append({
                    'price': price,
                    'usdc': usdc,
                    'tokens': tokens,
                    'direction': direction,
                    'token_id': token_id,
                    'maker': f.get('maker', ''),
                })

        if not fill_data:
            return None

        # All fills in a sweep should have same direction and token
        # Use majority/first as canonical
        direction = fill_data[0]['direction']
        token_id = fill_data[0]['token_id']
        taker = fills[0].get('taker', '')
        timestamp = int(fills[0].get('timestamp', 0))

        # Sort fills by price (best first for direction)
        if direction == 'BUY':
            fill_data.sort(key=lambda x: x['price'])  # Lowest first (best for buyer)
        else:
            fill_data.sort(key=lambda x: -x['price'])  # Highest first (best for seller)

        # Compute sweep features
        n_matches = len(fill_data)
        unique_makers = len(set(f['maker'] for f in fill_data))
        maker_wallets = list(set(f['maker'] for f in fill_data))

        prices = [f['price'] for f in fill_data]
        usdc_amounts = [f['usdc'] for f in fill_data]
        token_amounts = [f['tokens'] for f in fill_data]

        total_usdc = sum(usdc_amounts)
        total_tokens = sum(token_amounts)

        price_first = prices[0]
        price_last = prices[-1]
        price_vwap = total_usdc / total_tokens if total_tokens > 0 else price_first
        price_range = abs(price_last - price_first)

        # Depth at touch lower bound = size of first fill
        depth_touch_lb = usdc_amounts[0]

        # Thickness proxy (avoid div by zero)
        thickness = total_usdc / price_range if price_range > 0.0001 else total_usdc * 10000

        # Aggression = average fill size
        aggression = total_usdc / n_matches

        # Get market context
        market_id = ''
        token_position = ''
        market_type = 'unknown'
        tte_seconds = None
        tte_bucket = 'unknown'

        if token_id in self.token_map:
            market_id, token_position = self.token_map[token_id]
            market_data = self.markets.get(market_id, {})
            market_type = classify_market(
                market_data.get('ticker', ''),
                market_data.get('question', '')
            )
            tte_seconds, tte_bucket = get_expiry_bucket(
                timestamp,
                market_data.get('closedTime', '')
            )

        return {
            'sweep_id': tx_hash,
            'timestamp': timestamp,
            'market_id': market_id,
            'token_id': token_id,
            'token_position': token_position,
            'direction': direction,
            'taker_wallet': taker,
            'total_usdc': round(total_usdc, 6),
            'total_tokens': round(total_tokens, 6),
            'price_first': round(price_first, 6),
            'price_last': round(price_last, 6),
            'price_vwap': round(price_vwap, 6),
            'price_range': round(price_range, 6),
            'n_matches': n_matches,
            'unique_makers': unique_makers,
            'depth_touch_lb': round(depth_touch_lb, 6),
            'thickness': round(thickness, 2),
            'aggression': round(aggression, 6),
            'time_to_expiry_seconds': tte_seconds,
            'time_to_expiry_bucket': tte_bucket,
            'market_type': market_type,
            'maker_wallets': json.dumps(maker_wallets),
        }

    def get_completed(self) -> List[dict]:
        """Get and clear completed sweeps"""
        result = self.completed
        self.completed = []
        return result


# ============ Main ============

def main():
    print("=" * 60)
    print("SWEEP TABLE BUILDER")
    print("=" * 60)

    # Load context
    markets = load_markets()
    token_map = build_token_map(markets)

    # Initialize builder
    builder = SweepBuilder(markets, token_map)

    # Output file
    output_path = get_data_path('sweeps.csv')
    print(f"\nOutput: {output_path}")

    # Define columns
    columns = [
        'sweep_id', 'timestamp', 'market_id', 'token_id', 'token_position',
        'direction', 'taker_wallet', 'total_usdc', 'total_tokens',
        'price_first', 'price_last', 'price_vwap', 'price_range',
        'n_matches', 'unique_makers', 'depth_touch_lb', 'thickness', 'aggression',
        'time_to_expiry_seconds', 'time_to_expiry_bucket', 'market_type',
        'maker_wallets'
    ]

    # Process orderFilled.csv
    orders_path = get_data_path('orderFilled.csv')
    print(f"\nReading {orders_path}...")

    last_ts = 0
    row_count = 0
    written_count = 0

    with open(orders_path, 'r') as infile, \
         open(output_path, 'w', newline='') as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=columns)
        writer.writeheader()

        for row in reader:
            row_count += 1

            # Track timestamp for flush decisions
            ts = int(row.get('timestamp', 0))
            last_ts = max(last_ts, ts)

            # Add fill
            builder.add_fill(row)

            # Periodic flush
            if row_count % FLUSH_INTERVAL == 0:
                builder.flush(last_ts)

                # Write completed sweeps
                for sweep in builder.get_completed():
                    writer.writerow(sweep)
                    written_count += 1

                print(f"  {row_count:,} fills → {builder.total_sweeps:,} sweeps "
                      f"(pending: {len(builder.pending):,})")

        # Final flush
        builder.flush(last_ts, force=True)
        for sweep in builder.get_completed():
            writer.writerow(sweep)
            written_count += 1

    print(f"\n{'=' * 60}")
    print(f"COMPLETE")
    print(f"{'=' * 60}")
    print(f"Total fills processed: {row_count:,}")
    print(f"Total sweeps written:  {written_count:,}")
    print(f"Avg fills per sweep:   {row_count / max(written_count, 1):.2f}")
    print(f"Output: {output_path}")

    # Quick stats on output
    print(f"\n--- Sample Sweep Stats ---")
    with open(output_path, 'r') as f:
        reader = csv.DictReader(f)
        sweeps = list(reader)

    if sweeps:
        # Market type distribution
        market_types = defaultdict(int)
        expiry_buckets = defaultdict(int)
        directions = defaultdict(int)
        total_volume = 0

        for s in sweeps:
            market_types[s['market_type']] += 1
            expiry_buckets[s['time_to_expiry_bucket']] += 1
            directions[s['direction']] += 1
            total_volume += float(s['total_usdc'] or 0)

        print(f"\nBy market type:")
        for mt, cnt in sorted(market_types.items(), key=lambda x: -x[1])[:10]:
            print(f"  {mt}: {cnt:,}")

        print(f"\nBy expiry bucket:")
        for eb, cnt in sorted(expiry_buckets.items(), key=lambda x: -x[1]):
            print(f"  {eb}: {cnt:,}")

        print(f"\nBy direction:")
        for d, cnt in directions.items():
            print(f"  {d}: {cnt:,}")

        print(f"\nTotal volume (USDC): ${total_volume:,.2f}")


if __name__ == '__main__':
    main()
