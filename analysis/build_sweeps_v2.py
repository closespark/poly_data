"""
Build the Sweep Table v2 - With Critical Fixes

Fixes applied:
1. USDC detection via explicit asset ID (not string length)
2. Per-maker volume attribution (not split evenly)
3. Normalized slippage (price_range / price_first)

Output: sweeps.csv

Schema:
- sweep_id (transactionHash)
- timestamp
- market_id
- token_id
- token_position
- direction (BUY/SELL from taker)
- taker_wallet
- total_usdc
- total_tokens
- price_first
- price_last
- price_vwap
- price_range
- normalized_slippage (price_range / price_first)
- n_matches
- unique_makers
- depth_touch_lb
- thickness
- aggression
- time_to_expiry_seconds
- time_to_expiry_bucket
- market_type
- maker_volumes (JSON: {wallet: usdc_amount})
"""

import csv
import os
import json
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

DATA_DIR = os.environ.get('DATA_DIR', './data')
FLUSH_INTERVAL = 100_000
MAX_SWEEP_AGE_SECONDS = 10

# CRITICAL FIX #1: Explicit USDC asset ID
# Polymarket USDC is represented as "0" in the orderFilled data
USDC_ASSET_ID = "0"

def get_data_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


# ============ Market Context ============

def load_markets() -> Dict[str, dict]:
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
    tmap = {}
    for mid, m in markets.items():
        if m['token1']:
            tmap[m['token1']] = (mid, 'token1')
        if m['token2']:
            tmap[m['token2']] = (mid, 'token2')
    print(f"  Built token map: {len(tmap):,} tokens")
    return tmap


def classify_market(ticker: str, question: str) -> str:
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

def parse_fill(row: dict) -> Optional[dict]:
    """
    Parse a single fill row with CORRECT USDC detection.

    Returns dict with:
    - price
    - usdc_amount
    - token_amount
    - direction (BUY/SELL from taker perspective)
    - token_id
    - maker
    """
    try:
        maker_amount = float(row.get('makerAmountFilled', 0))
        taker_amount = float(row.get('takerAmountFilled', 0))
        maker_asset = row.get('makerAssetId', '')
        taker_asset = row.get('takerAssetId', '')
        maker = row.get('maker', '')

        if maker_amount == 0 or taker_amount == 0:
            return None

        # CRITICAL FIX #1: Explicit USDC check
        maker_is_usdc = (maker_asset == USDC_ASSET_ID)
        taker_is_usdc = (taker_asset == USDC_ASSET_ID)

        if maker_is_usdc and not taker_is_usdc:
            # Maker gave USDC, taker gave tokens
            # Taker is SELLING tokens (receiving USDC)
            usdc = maker_amount / 1e6
            tokens = taker_amount / 1e6
            direction = 'SELL'
            token_id = taker_asset
        elif taker_is_usdc and not maker_is_usdc:
            # Taker gave USDC, maker gave tokens
            # Taker is BUYING tokens (paying USDC)
            usdc = taker_amount / 1e6
            tokens = maker_amount / 1e6
            direction = 'BUY'
            token_id = maker_asset
        else:
            # Both USDC or neither - shouldn't happen in normal trading
            return None

        if tokens == 0:
            return None

        price = usdc / tokens

        # Sanity: prediction market prices should be 0-1 (allow small buffer)
        if not (0 < price < 1.5):
            return None

        return {
            'price': price,
            'usdc': usdc,
            'tokens': tokens,
            'direction': direction,
            'token_id': token_id,
            'maker': maker,
        }

    except (ValueError, TypeError):
        return None


# ============ Sweep Builder ============

class SweepBuilder:
    def __init__(self, markets: Dict, token_map: Dict):
        self.markets = markets
        self.token_map = token_map
        self.pending: Dict[str, dict] = defaultdict(lambda: {'fills': [], 'first_ts': float('inf')})
        self.completed: List[dict] = []
        self.total_fills = 0
        self.total_sweeps = 0

    def add_fill(self, row: dict):
        tx_hash = row.get('transactionHash', '')
        if not tx_hash:
            return

        ts = int(row.get('timestamp', 0))
        self.pending[tx_hash]['fills'].append(row)
        self.pending[tx_hash]['first_ts'] = min(self.pending[tx_hash]['first_ts'], ts)
        self.total_fills += 1

    def flush(self, current_ts: int, force: bool = False):
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
        if not fills:
            return None

        # Parse each fill
        parsed_fills = []
        for f in fills:
            parsed = parse_fill(f)
            if parsed:
                parsed_fills.append(parsed)

        if not parsed_fills:
            return None

        # Canonical values from first fill
        direction = parsed_fills[0]['direction']
        token_id = parsed_fills[0]['token_id']
        taker = fills[0].get('taker', '')
        timestamp = int(fills[0].get('timestamp', 0))

        # Sort by price (best first for direction)
        if direction == 'BUY':
            parsed_fills.sort(key=lambda x: x['price'])  # Lowest = best for buyer
        else:
            parsed_fills.sort(key=lambda x: -x['price'])  # Highest = best for seller

        # CRITICAL FIX #2: Track per-maker volume
        maker_volumes: Dict[str, float] = defaultdict(float)
        for f in parsed_fills:
            maker_volumes[f['maker']] += f['usdc']

        # Aggregate stats
        n_matches = len(parsed_fills)
        unique_makers = len(maker_volumes)

        prices = [f['price'] for f in parsed_fills]
        usdc_amounts = [f['usdc'] for f in parsed_fills]
        token_amounts = [f['tokens'] for f in parsed_fills]

        total_usdc = sum(usdc_amounts)
        total_tokens = sum(token_amounts)

        price_first = prices[0]
        price_last = prices[-1]
        price_vwap = total_usdc / total_tokens if total_tokens > 0 else price_first
        price_range = abs(price_last - price_first)

        # CRITICAL FIX #3: Normalized slippage
        normalized_slippage = price_range / price_first if price_first > 0 else 0

        # Depth at touch = size of first fill (lower bound)
        depth_touch_lb = usdc_amounts[0]

        # Thickness proxy
        thickness = total_usdc / price_range if price_range > 0.0001 else total_usdc * 10000

        # Aggression = average fill size
        aggression = total_usdc / n_matches

        # Market context
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
            'normalized_slippage': round(normalized_slippage, 6),
            'n_matches': n_matches,
            'unique_makers': unique_makers,
            'depth_touch_lb': round(depth_touch_lb, 6),
            'thickness': round(thickness, 2),
            'aggression': round(aggression, 6),
            'time_to_expiry_seconds': tte_seconds,
            'time_to_expiry_bucket': tte_bucket,
            'market_type': market_type,
            'maker_volumes': json.dumps({k: round(v, 4) for k, v in maker_volumes.items()}),
        }

    def get_completed(self) -> List[dict]:
        result = self.completed
        self.completed = []
        return result


# ============ Main ============

def main():
    print("=" * 60)
    print("SWEEP TABLE BUILDER v2")
    print("=" * 60)
    print("\nFixes applied:")
    print("  1. USDC detection via explicit asset ID")
    print("  2. Per-maker volume attribution")
    print("  3. Normalized slippage (price_range / price_first)")

    markets = load_markets()
    token_map = build_token_map(markets)

    builder = SweepBuilder(markets, token_map)

    output_path = get_data_path('sweeps.csv')
    print(f"\nOutput: {output_path}")

    columns = [
        'sweep_id', 'timestamp', 'market_id', 'token_id', 'token_position',
        'direction', 'taker_wallet', 'total_usdc', 'total_tokens',
        'price_first', 'price_last', 'price_vwap', 'price_range', 'normalized_slippage',
        'n_matches', 'unique_makers', 'depth_touch_lb', 'thickness', 'aggression',
        'time_to_expiry_seconds', 'time_to_expiry_bucket', 'market_type',
        'maker_volumes'
    ]

    orders_path = get_data_path('orderFilled.csv')
    print(f"\nReading {orders_path}...")

    last_ts = 0
    row_count = 0
    written_count = 0
    skipped_fills = 0

    with open(orders_path, 'r') as infile, \
         open(output_path, 'w', newline='') as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=columns)
        writer.writeheader()

        for row in reader:
            row_count += 1
            ts = int(row.get('timestamp', 0))
            last_ts = max(last_ts, ts)

            builder.add_fill(row)

            if row_count % FLUSH_INTERVAL == 0:
                builder.flush(last_ts)

                for sweep in builder.get_completed():
                    writer.writerow(sweep)
                    written_count += 1

                print(f"  {row_count:,} fills → {builder.total_sweeps:,} sweeps")

        # Final flush
        builder.flush(last_ts, force=True)
        for sweep in builder.get_completed():
            writer.writerow(sweep)
            written_count += 1

    print(f"\n{'=' * 60}")
    print("COMPLETE")
    print(f"{'=' * 60}")
    print(f"Total fills processed: {row_count:,}")
    print(f"Total sweeps written:  {written_count:,}")
    print(f"Avg fills per sweep:   {row_count / max(written_count, 1):.2f}")
    print(f"Output: {output_path}")

    # Stats
    print(f"\n--- Distribution Analysis ---")
    with open(output_path, 'r') as f:
        reader = csv.DictReader(f)
        sweeps = list(reader)

    if sweeps:
        market_types = defaultdict(int)
        directions = defaultdict(int)
        total_volume = 0
        multi_match = 0

        for s in sweeps:
            market_types[s['market_type']] += 1
            directions[s['direction']] += 1
            total_volume += float(s['total_usdc'] or 0)
            if int(s['n_matches'] or 1) > 1:
                multi_match += 1

        print(f"\nMarket types:")
        for mt, cnt in sorted(market_types.items(), key=lambda x: -x[1])[:10]:
            print(f"  {mt}: {cnt:,} ({cnt/len(sweeps)*100:.1f}%)")

        print(f"\nDirections:")
        for d, cnt in directions.items():
            print(f"  {d}: {cnt:,} ({cnt/len(sweeps)*100:.1f}%)")

        print(f"\nTotal volume: ${total_volume:,.2f}")
        print(f"Multi-match sweeps: {multi_match:,} ({multi_match/len(sweeps)*100:.1f}%)")


if __name__ == '__main__':
    main()
