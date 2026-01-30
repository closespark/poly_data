"""
Wallet Toxicity Scorer v1

Uses orderFilled.csv directly (not trades.csv) to:
1. Group fills by transactionHash (sweep detection)
2. Compute sweep-derived context features
3. Calculate markout at multiple horizons
4. Score each wallet's toxicity

Key insight: Each transactionHash can have multiple fills (book walk).
This gives us context without needing book snapshots.

Output: wallet_toxicity.csv

Columns:
- wallet_id
- market_type
- expiry_bucket
- role (taker/maker)
- avg_markout_1s
- avg_markout_5s
- avg_markout_30s
- avg_markout_60s
- avg_sweep_depth (how many levels they typically sweep)
- avg_sweep_slippage
- trade_count
- total_volume_usdc
- toxicity_score (composite)
"""

import csv
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import statistics

DATA_DIR = os.environ.get('DATA_DIR', './data')

def get_data_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


def load_markets() -> Dict[str, dict]:
    """Load markets for context (ticker, closedTime, tokens)"""
    markets = {}
    with open(get_data_path('markets.csv'), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            market_id = row.get('id', '')
            if market_id:
                markets[market_id] = {
                    'token1': row.get('token1', ''),
                    'token2': row.get('token2', ''),
                    'closedTime': row.get('closedTime', ''),
                    'ticker': row.get('ticker', ''),
                    'question': row.get('question', ''),
                }
    return markets


def build_token_to_market(markets: Dict) -> Dict[str, Tuple[str, str]]:
    """token_id -> (market_id, 'token1'/'token2')"""
    token_map = {}
    for mid, m in markets.items():
        if m['token1']:
            token_map[m['token1']] = (mid, 'token1')
        if m['token2']:
            token_map[m['token2']] = (mid, 'token2')
    return token_map


def classify_market_type(ticker: str, question: str) -> str:
    """Same as research_spine classifier"""
    ticker_lower = (ticker or '').lower()
    question_lower = (question or '').lower()

    crypto_keywords = ['btc', 'eth', 'bitcoin', 'ethereum', 'crypto', 'sol']
    is_crypto = any(kw in ticker_lower or kw in question_lower for kw in crypto_keywords)

    if is_crypto:
        if '15m' in ticker_lower or '15 min' in question_lower:
            return 'crypto_15m'
        elif '1h' in ticker_lower or '1 hour' in question_lower:
            return 'crypto_1h'
        elif '4h' in ticker_lower or '4 hour' in question_lower:
            return 'crypto_4h'
        return 'crypto_other'

    political = ['president', 'election', 'trump', 'biden', 'vote']
    if any(kw in question_lower for kw in political):
        return 'political'

    return 'other'


def get_expiry_bucket(trade_ts: int, closed_time: str) -> str:
    """Bucket time-to-expiry"""
    if not closed_time:
        return 'no_expiry'
    try:
        if closed_time.isdigit():
            close_ts = int(closed_time) // 1000  # ms -> s
        else:
            close_ts = int(datetime.fromisoformat(closed_time.replace('Z', '+00:00')).timestamp())

        tte = close_ts - trade_ts
        if tte < 0:
            return 'expired'
        elif tte < 600:
            return '<10m'
        elif tte < 1800:
            return '10-30m'
        else:
            return '>30m'
    except:
        return 'parse_error'


def compute_fill_price(maker_amount: float, taker_amount: float,
                       maker_asset: str, taker_asset: str) -> Optional[float]:
    """
    Compute fill price from amounts.
    USDC has assetId = 0 (or very small number)
    Price = USDC / tokens (probability 0-1)
    """
    # Determine which side is USDC
    # USDC assetId is typically 0 or a specific known value
    # Tokens have large assetIds (the condition token IDs)

    maker_is_usdc = len(maker_asset) < 10  # Heuristic: USDC has short ID

    if maker_is_usdc:
        # Maker gave USDC, taker gave tokens
        usdc = maker_amount
        tokens = taker_amount
    else:
        # Taker gave USDC, maker gave tokens
        usdc = taker_amount
        tokens = maker_amount

    if tokens == 0:
        return None

    # Amounts are in micro-units (6 decimals for USDC, 6 for tokens typically)
    price = usdc / tokens

    # Sanity check: price should be 0-1 for prediction markets
    if 0 < price < 2:  # Allow some slippage beyond 1
        return price
    return None


class SweepAggregator:
    """
    Groups fills by transactionHash to detect sweeps.
    Computes sweep-level features.
    """

    def __init__(self):
        # {tx_hash: [fill_rows]}
        self.pending_sweeps: Dict[str, List[dict]] = defaultdict(list)
        self.processed_sweeps: List[dict] = []

    def add_fill(self, fill: dict):
        """Add a fill row to pending sweeps"""
        tx_hash = fill.get('transactionHash', '')
        if tx_hash:
            self.pending_sweeps[tx_hash].append(fill)

    def flush_old_sweeps(self, current_ts: int, max_age_seconds: int = 60):
        """
        Process sweeps that are old enough to be complete.
        In practice, all fills for a tx should arrive together,
        but we use a time window for safety.
        """
        to_remove = []

        for tx_hash, fills in self.pending_sweeps.items():
            if not fills:
                continue

            # Get oldest fill timestamp
            oldest_ts = min(int(f.get('timestamp', 0)) for f in fills)

            if current_ts - oldest_ts > max_age_seconds:
                # Process this sweep
                sweep_data = self._compute_sweep_features(tx_hash, fills)
                if sweep_data:
                    self.processed_sweeps.append(sweep_data)
                to_remove.append(tx_hash)

        for tx_hash in to_remove:
            del self.pending_sweeps[tx_hash]

    def _compute_sweep_features(self, tx_hash: str, fills: List[dict]) -> Optional[dict]:
        """
        Compute features for a single sweep (all fills in one tx).
        """
        if not fills:
            return None

        # Sort by... we don't have sequence, but fills should be in order
        # For now, trust the order they came in

        n_matches = len(fills)
        unique_makers = len(set(f.get('maker', '') for f in fills))

        # Identify taker (should be same across all fills in tx)
        taker = fills[0].get('taker', '')

        # Compute prices for each fill
        prices = []
        usdc_amounts = []
        token_amounts = []

        for f in fills:
            try:
                maker_amount = float(f.get('makerAmountFilled', 0))
                taker_amount = float(f.get('takerAmountFilled', 0))
                maker_asset = f.get('makerAssetId', '')
                taker_asset = f.get('takerAssetId', '')

                price = compute_fill_price(maker_amount, taker_amount, maker_asset, taker_asset)
                if price is not None:
                    prices.append(price)

                # Track USDC volume
                if len(maker_asset) < 10:  # maker is USDC
                    usdc_amounts.append(maker_amount / 1e6)  # Convert from micro
                else:
                    usdc_amounts.append(taker_amount / 1e6)

            except (ValueError, TypeError):
                continue

        if not prices:
            return None

        # Sweep features
        price_first = prices[0]
        price_last = prices[-1]
        price_vwap = sum(p * u for p, u in zip(prices, usdc_amounts)) / sum(usdc_amounts) if usdc_amounts else price_first

        sweep_slippage = abs(price_last - price_first)
        total_usdc = sum(usdc_amounts)

        # Determine side (BUY if taker receives tokens, SELL if taker gives tokens)
        # Heuristic: if taker asset is USDC-like (short ID), they're buying tokens
        taker_asset = fills[0].get('takerAssetId', '')
        side = 'BUY' if len(taker_asset) < 10 else 'SELL'

        # Get token info for market lookup
        # The non-USDC asset in the trade
        if len(fills[0].get('makerAssetId', '')) > 10:
            token_id = fills[0].get('makerAssetId', '')
        else:
            token_id = fills[0].get('takerAssetId', '')

        return {
            'tx_hash': tx_hash,
            'timestamp': int(fills[0].get('timestamp', 0)),
            'taker': taker,
            'makers': [f.get('maker', '') for f in fills],
            'token_id': token_id,
            'side': side,
            'n_matches': n_matches,
            'unique_makers': unique_makers,
            'price_first': price_first,
            'price_vwap': price_vwap,
            'price_last': price_last,
            'sweep_slippage': sweep_slippage,
            'total_usdc': total_usdc,
        }


class MarkoutCalculator:
    """
    Calculates markout (price movement after trade) for toxicity scoring.

    Maintains a price series per token and computes forward returns.
    """

    def __init__(self, horizons_seconds: List[int] = [1, 5, 30, 60]):
        self.horizons = horizons_seconds
        # {token_id: [(timestamp, price), ...]}
        self.price_series: Dict[str, List[Tuple[int, float]]] = defaultdict(list)
        # {token_id: {timestamp: price}} for fast lookup
        self.price_index: Dict[str, Dict[int, float]] = defaultdict(dict)

    def add_price(self, token_id: str, timestamp: int, price: float):
        """Record a price observation"""
        self.price_series[token_id].append((timestamp, price))
        self.price_index[token_id][timestamp] = price

    def get_price_at(self, token_id: str, timestamp: int, tolerance_seconds: int = 2) -> Optional[float]:
        """Get price at or near a timestamp"""
        index = self.price_index.get(token_id, {})

        # Exact match
        if timestamp in index:
            return index[timestamp]

        # Search within tolerance
        for delta in range(1, tolerance_seconds + 1):
            if timestamp + delta in index:
                return index[timestamp + delta]
            if timestamp - delta in index:
                return index[timestamp - delta]

        return None

    def compute_markouts(self, token_id: str, timestamp: int,
                         entry_price: float) -> Dict[int, Optional[float]]:
        """
        Compute markout at each horizon.
        Markout = future_price - entry_price (for buys)
        Positive markout = price moved in your favor after entry
        """
        results = {}

        for horizon in self.horizons:
            future_price = self.get_price_at(token_id, timestamp + horizon)
            if future_price is not None:
                results[horizon] = future_price - entry_price
            else:
                results[horizon] = None

        return results


class WalletScorer:
    """
    Aggregates sweep data by wallet to compute toxicity scores.
    """

    def __init__(self):
        # {(wallet, market_type, expiry_bucket, role): [sweep_stats]}
        self.wallet_stats: Dict[Tuple, List[dict]] = defaultdict(list)

    def add_sweep(self, sweep: dict, market_type: str, expiry_bucket: str,
                  markouts: Dict[int, Optional[float]]):
        """Record a sweep for the taker and all makers"""

        # Taker stats
        taker_key = (sweep['taker'], market_type, expiry_bucket, 'taker')
        self.wallet_stats[taker_key].append({
            'n_matches': sweep['n_matches'],
            'sweep_slippage': sweep['sweep_slippage'],
            'total_usdc': sweep['total_usdc'],
            'side': sweep['side'],
            'markouts': markouts,
        })

        # Maker stats (each maker in the sweep)
        for maker in sweep['makers']:
            maker_key = (maker, market_type, expiry_bucket, 'maker')
            # For makers, markout interpretation is flipped
            # If price goes up after they sold, that's adverse for them
            flipped_markouts = {k: -v if v is not None else None
                               for k, v in markouts.items()}
            self.wallet_stats[maker_key].append({
                'n_matches': 1,  # Their individual fill
                'sweep_slippage': 0,  # Not meaningful for makers
                'total_usdc': sweep['total_usdc'] / len(sweep['makers']),  # Approximate
                'side': 'SELL' if sweep['side'] == 'BUY' else 'BUY',
                'markouts': flipped_markouts,
            })

    def compute_toxicity_scores(self) -> List[dict]:
        """
        Compute final toxicity scores for each wallet context.
        """
        results = []

        for (wallet, market_type, expiry_bucket, role), stats in self.wallet_stats.items():
            if len(stats) < 5:  # Minimum trades for reliable scoring
                continue

            # Aggregate markouts
            markout_1s = [s['markouts'].get(1) for s in stats if s['markouts'].get(1) is not None]
            markout_5s = [s['markouts'].get(5) for s in stats if s['markouts'].get(5) is not None]
            markout_30s = [s['markouts'].get(30) for s in stats if s['markouts'].get(30) is not None]
            markout_60s = [s['markouts'].get(60) for s in stats if s['markouts'].get(60) is not None]

            # Sweep depth (for takers)
            sweep_depths = [s['n_matches'] for s in stats]
            sweep_slippages = [s['sweep_slippage'] for s in stats]
            total_volumes = [s['total_usdc'] for s in stats]

            # Compute averages
            avg_markout_1s = statistics.mean(markout_1s) if markout_1s else None
            avg_markout_5s = statistics.mean(markout_5s) if markout_5s else None
            avg_markout_30s = statistics.mean(markout_30s) if markout_30s else None
            avg_markout_60s = statistics.mean(markout_60s) if markout_60s else None

            # Composite toxicity score
            # Higher = more toxic (for makers to avoid)
            # Uses 5s markout as primary signal
            if avg_markout_5s is not None:
                # Normalize: typical markout might be -0.01 to +0.01
                # Scale to roughly -100 to +100
                toxicity_score = avg_markout_5s * 1000
            else:
                toxicity_score = 0

            results.append({
                'wallet_id': wallet,
                'market_type': market_type,
                'expiry_bucket': expiry_bucket,
                'role': role,
                'avg_markout_1s': avg_markout_1s,
                'avg_markout_5s': avg_markout_5s,
                'avg_markout_30s': avg_markout_30s,
                'avg_markout_60s': avg_markout_60s,
                'avg_sweep_depth': statistics.mean(sweep_depths),
                'avg_sweep_slippage': statistics.mean(sweep_slippages),
                'trade_count': len(stats),
                'total_volume_usdc': sum(total_volumes),
                'toxicity_score': toxicity_score,
            })

        return results


def process_ordersfilled():
    """
    Main processing loop.
    Reads orderFilled.csv, groups by tx, computes sweeps, calculates markouts.
    """
    print("Loading markets...")
    markets = load_markets()
    token_map = build_token_to_market(markets)
    print(f"Loaded {len(markets):,} markets, {len(token_map):,} tokens")

    # Initialize components
    sweep_agg = SweepAggregator()
    markout_calc = MarkoutCalculator()
    wallet_scorer = WalletScorer()

    orders_path = get_data_path('orderFilled.csv')
    print(f"\nProcessing {orders_path}...")

    row_count = 0
    last_ts = 0

    with open(orders_path, 'r') as f:
        reader = csv.DictReader(f)

        for row in reader:
            row_count += 1

            # Add to sweep aggregator
            sweep_agg.add_fill(row)

            # Track price for markout calculation
            try:
                ts = int(row.get('timestamp', 0))
                maker_amount = float(row.get('makerAmountFilled', 0))
                taker_amount = float(row.get('takerAmountFilled', 0))
                maker_asset = row.get('makerAssetId', '')
                taker_asset = row.get('takerAssetId', '')

                price = compute_fill_price(maker_amount, taker_amount, maker_asset, taker_asset)

                # Determine token_id
                token_id = taker_asset if len(taker_asset) > 10 else maker_asset

                if price is not None and token_id:
                    markout_calc.add_price(token_id, ts, price)

                last_ts = max(last_ts, ts)

            except (ValueError, TypeError):
                pass

            # Periodically flush old sweeps and compute their stats
            if row_count % 100_000 == 0:
                sweep_agg.flush_old_sweeps(last_ts)

                # Process completed sweeps
                for sweep in sweep_agg.processed_sweeps:
                    # Get market context
                    market_info = token_map.get(sweep['token_id'])
                    if market_info:
                        market_id, token_pos = market_info
                        market_data = markets.get(market_id, {})
                        market_type = classify_market_type(
                            market_data.get('ticker', ''),
                            market_data.get('question', '')
                        )
                        expiry_bucket = get_expiry_bucket(
                            sweep['timestamp'],
                            market_data.get('closedTime', '')
                        )
                    else:
                        market_type = 'unknown'
                        expiry_bucket = 'unknown'

                    # Compute markouts
                    markouts = markout_calc.compute_markouts(
                        sweep['token_id'],
                        sweep['timestamp'],
                        sweep['price_vwap']
                    )

                    # Record for wallet scoring
                    wallet_scorer.add_sweep(sweep, market_type, expiry_bucket, markouts)

                # Clear processed sweeps
                sweep_agg.processed_sweeps = []

                print(f"  Processed {row_count:,} rows, {len(wallet_scorer.wallet_stats):,} wallet contexts")

    # Final flush
    sweep_agg.flush_old_sweeps(last_ts + 1000)
    for sweep in sweep_agg.processed_sweeps:
        market_info = token_map.get(sweep['token_id'])
        if market_info:
            market_id, token_pos = market_info
            market_data = markets.get(market_id, {})
            market_type = classify_market_type(
                market_data.get('ticker', ''),
                market_data.get('question', '')
            )
            expiry_bucket = get_expiry_bucket(
                sweep['timestamp'],
                market_data.get('closedTime', '')
            )
        else:
            market_type = 'unknown'
            expiry_bucket = 'unknown'

        markouts = markout_calc.compute_markouts(
            sweep['token_id'],
            sweep['timestamp'],
            sweep['price_vwap']
        )
        wallet_scorer.add_sweep(sweep, market_type, expiry_bucket, markouts)

    print(f"\nTotal rows processed: {row_count:,}")
    print(f"Unique wallet contexts: {len(wallet_scorer.wallet_stats):,}")

    # Compute final scores
    print("\nComputing toxicity scores...")
    results = wallet_scorer.compute_toxicity_scores()
    print(f"Wallets with sufficient data: {len(results):,}")

    # Write output
    output_path = get_data_path('wallet_toxicity.csv')
    print(f"\nWriting to {output_path}...")

    fieldnames = [
        'wallet_id', 'market_type', 'expiry_bucket', 'role',
        'avg_markout_1s', 'avg_markout_5s', 'avg_markout_30s', 'avg_markout_60s',
        'avg_sweep_depth', 'avg_sweep_slippage',
        'trade_count', 'total_volume_usdc', 'toxicity_score'
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print("Done!")

    # Print most toxic wallets
    print("\n--- Top 20 Most Toxic Takers (crypto_15m, <10m expiry) ---")
    toxic_takers = sorted(
        [r for r in results if r['role'] == 'taker'
         and r['market_type'] == 'crypto_15m'
         and r['expiry_bucket'] == '<10m'],
        key=lambda x: x['toxicity_score'],
        reverse=True
    )[:20]

    for t in toxic_takers:
        print(f"  {t['wallet_id'][:16]}... score={t['toxicity_score']:.1f} "
              f"markout_5s={t['avg_markout_5s']:.4f} trades={t['trade_count']}")


if __name__ == '__main__':
    process_ordersfilled()
