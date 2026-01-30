"""
Wallet Toxicity Scorer v2 - Sweep-Aware

Uses sweeps.csv (not raw fills) to compute wallet toxicity.

Key insight: Toxicity is context-dependent.
Same wallet may be toxic in crypto_15m but not in political markets.

Output: wallet_toxicity.parquet (or csv)

Schema:
- wallet
- market_type
- expiry_bucket
- role (taker)
- trade_count
- total_volume_usdc
- avg_sweep_size
- avg_n_matches (book depth consumed)
- avg_price_range (slippage)
- avg_thickness
- avg_markout_1s
- avg_markout_5s
- avg_markout_30s
- markout_stddev_5s
- win_rate_5s (% of trades with positive markout)
- toxicity_score (composite)
- confidence (based on sample size)
"""

import csv
import os
import json
import statistics
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

DATA_DIR = os.environ.get('DATA_DIR', './data')

def get_data_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


class PriceSeries:
    """
    Maintains price series per token for markout calculation.
    Memory-efficient: keeps only recent prices.
    """

    def __init__(self, max_age_seconds: int = 120):
        self.max_age = max_age_seconds
        # {token_id: [(timestamp, price), ...]}
        self.series: Dict[str, List[Tuple[int, float]]] = defaultdict(list)

    def add(self, token_id: str, timestamp: int, price: float):
        """Add a price observation"""
        self.series[token_id].append((timestamp, price))

    def get_price_at(self, token_id: str, target_ts: int,
                     tolerance: int = 5) -> Optional[float]:
        """
        Get price at or near target timestamp.
        Returns closest price within tolerance.
        """
        if token_id not in self.series:
            return None

        prices = self.series[token_id]
        best_price = None
        best_diff = float('inf')

        for ts, price in prices:
            diff = abs(ts - target_ts)
            if diff <= tolerance and diff < best_diff:
                best_diff = diff
                best_price = price

        return best_price

    def cleanup(self, current_ts: int):
        """Remove old prices to save memory"""
        cutoff = current_ts - self.max_age
        for token_id in list(self.series.keys()):
            self.series[token_id] = [
                (ts, p) for ts, p in self.series[token_id]
                if ts > cutoff
            ]
            if not self.series[token_id]:
                del self.series[token_id]


class WalletAggregator:
    """
    Aggregates sweep-level stats by wallet context.
    """

    def __init__(self):
        # {(wallet, market_type, expiry_bucket): [sweep_stats]}
        self.stats: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)

    def add_sweep(self, sweep: dict, markouts: Dict[int, Optional[float]]):
        """Record a sweep for the taker"""
        key = (
            sweep['taker_wallet'],
            sweep['market_type'],
            sweep['time_to_expiry_bucket']
        )

        self.stats[key].append({
            'total_usdc': float(sweep['total_usdc'] or 0),
            'n_matches': int(sweep['n_matches'] or 1),
            'price_range': float(sweep['price_range'] or 0),
            'thickness': float(sweep['thickness'] or 0),
            'direction': sweep['direction'],
            'markout_1s': markouts.get(1),
            'markout_5s': markouts.get(5),
            'markout_30s': markouts.get(30),
        })

    def compute_scores(self, min_trades: int = 10) -> List[dict]:
        """
        Compute final toxicity scores.

        Toxicity = average markout after this wallet trades
        Positive = price moves in direction of their trade (informed)
        Negative = price moves against them (noise)
        """
        results = []

        for (wallet, market_type, expiry_bucket), sweeps in self.stats.items():
            if len(sweeps) < min_trades:
                continue

            # Volume stats
            volumes = [s['total_usdc'] for s in sweeps]
            matches = [s['n_matches'] for s in sweeps]
            ranges = [s['price_range'] for s in sweeps]
            thicknesses = [s['thickness'] for s in sweeps if s['thickness'] > 0]

            # Markout stats (filter None)
            m1 = [s['markout_1s'] for s in sweeps if s['markout_1s'] is not None]
            m5 = [s['markout_5s'] for s in sweeps if s['markout_5s'] is not None]
            m30 = [s['markout_30s'] for s in sweeps if s['markout_30s'] is not None]

            # Compute aggregates
            avg_markout_1s = statistics.mean(m1) if len(m1) >= 5 else None
            avg_markout_5s = statistics.mean(m5) if len(m5) >= 5 else None
            avg_markout_30s = statistics.mean(m30) if len(m30) >= 5 else None

            markout_stddev_5s = statistics.stdev(m5) if len(m5) >= 10 else None

            # Win rate: % of trades with positive markout at 5s
            if len(m5) >= 5:
                win_rate_5s = sum(1 for m in m5 if m > 0) / len(m5)
            else:
                win_rate_5s = None

            # Toxicity score (main signal)
            # Scale: multiply by 10000 for readability
            # +10 = very toxic (10 bps average markout)
            # -10 = very dumb money
            if avg_markout_5s is not None:
                toxicity_score = avg_markout_5s * 10000
            else:
                toxicity_score = 0

            # Confidence: based on sample size and variance
            # Higher trade count + lower variance = higher confidence
            confidence = min(len(sweeps) / 100, 1.0)  # Cap at 1.0 for 100+ trades
            if markout_stddev_5s and avg_markout_5s:
                # Sharpe-like adjustment
                if markout_stddev_5s > 0:
                    t_stat = abs(avg_markout_5s) / (markout_stddev_5s / (len(m5) ** 0.5))
                    if t_stat > 2:  # Statistically significant
                        confidence = min(confidence * 1.5, 1.0)

            results.append({
                'wallet': wallet,
                'market_type': market_type,
                'expiry_bucket': expiry_bucket,
                'role': 'taker',
                'trade_count': len(sweeps),
                'total_volume_usdc': sum(volumes),
                'avg_sweep_size': statistics.mean(volumes),
                'avg_n_matches': statistics.mean(matches),
                'avg_price_range': statistics.mean(ranges),
                'avg_thickness': statistics.mean(thicknesses) if thicknesses else 0,
                'avg_markout_1s': avg_markout_1s,
                'avg_markout_5s': avg_markout_5s,
                'avg_markout_30s': avg_markout_30s,
                'markout_stddev_5s': markout_stddev_5s,
                'win_rate_5s': win_rate_5s,
                'toxicity_score': toxicity_score,
                'confidence': confidence,
            })

        return results


def main():
    print("=" * 60)
    print("WALLET TOXICITY SCORER v2 (Sweep-Aware)")
    print("=" * 60)

    sweeps_path = get_data_path('sweeps.csv')
    output_path = get_data_path('wallet_toxicity.csv')

    # Check if sweeps.csv exists
    if not os.path.exists(sweeps_path):
        print(f"ERROR: {sweeps_path} not found")
        print("Run build_sweeps.py first!")
        return

    print(f"\nReading sweeps from {sweeps_path}...")

    # Initialize
    price_series = PriceSeries(max_age_seconds=120)
    aggregator = WalletAggregator()

    # First pass: build price series
    print("\nPass 1: Building price series...")
    sweep_count = 0

    with open(sweeps_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sweep_count += 1
            token_id = row.get('token_id', '')
            timestamp = int(row.get('timestamp', 0))
            price = float(row.get('price_vwap', 0) or 0)

            if token_id and timestamp and price > 0:
                price_series.add(token_id, timestamp, price)

            if sweep_count % 1_000_000 == 0:
                print(f"  {sweep_count:,} sweeps indexed")

    print(f"  Total: {sweep_count:,} sweeps")
    print(f"  Tokens tracked: {len(price_series.series):,}")

    # Second pass: compute markouts and aggregate
    print("\nPass 2: Computing markouts and aggregating...")
    processed = 0

    with open(sweeps_path, 'r') as f:
        reader = csv.DictReader(f)

        for row in reader:
            processed += 1

            token_id = row.get('token_id', '')
            timestamp = int(row.get('timestamp', 0))
            entry_price = float(row.get('price_vwap', 0) or 0)
            direction = row.get('direction', '')

            if not token_id or not timestamp or entry_price <= 0:
                continue

            # Compute markouts at various horizons
            markouts = {}
            for horizon in [1, 5, 30]:
                future_price = price_series.get_price_at(
                    token_id,
                    timestamp + horizon,
                    tolerance=horizon  # Allow more tolerance for longer horizons
                )

                if future_price is not None:
                    # Markout = price change in direction of trade
                    raw_markout = future_price - entry_price
                    if direction == 'SELL':
                        raw_markout = -raw_markout  # Flip for sells
                    markouts[horizon] = raw_markout
                else:
                    markouts[horizon] = None

            # Add to aggregator
            aggregator.add_sweep(row, markouts)

            if processed % 1_000_000 == 0:
                print(f"  {processed:,} sweeps processed")

    print(f"  Total processed: {processed:,}")
    print(f"  Unique wallet contexts: {len(aggregator.stats):,}")

    # Compute final scores
    print("\nComputing toxicity scores...")
    results = aggregator.compute_scores(min_trades=10)
    print(f"  Wallets with sufficient data: {len(results):,}")

    # Write output
    print(f"\nWriting to {output_path}...")

    columns = [
        'wallet', 'market_type', 'expiry_bucket', 'role',
        'trade_count', 'total_volume_usdc', 'avg_sweep_size',
        'avg_n_matches', 'avg_price_range', 'avg_thickness',
        'avg_markout_1s', 'avg_markout_5s', 'avg_markout_30s',
        'markout_stddev_5s', 'win_rate_5s',
        'toxicity_score', 'confidence'
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for r in results:
            # Format floats
            for key in ['avg_sweep_size', 'avg_n_matches', 'avg_price_range',
                       'avg_thickness', 'avg_markout_1s', 'avg_markout_5s',
                       'avg_markout_30s', 'markout_stddev_5s', 'win_rate_5s',
                       'toxicity_score', 'confidence']:
                if r[key] is not None:
                    r[key] = round(r[key], 6)
            writer.writerow(r)

    print("Done!")

    # ========== Analysis ==========
    print("\n" + "=" * 60)
    print("ANALYSIS")
    print("=" * 60)

    # Most toxic takers in crypto_15m
    crypto_15m_takers = [
        r for r in results
        if r['market_type'] == 'crypto_15m' and r['confidence'] > 0.3
    ]

    if crypto_15m_takers:
        print("\n--- Top 15 MOST TOXIC Takers (crypto_15m, high confidence) ---")
        print("(These wallets consistently move price in their direction)")
        toxic = sorted(crypto_15m_takers, key=lambda x: x['toxicity_score'], reverse=True)[:15]
        for t in toxic:
            print(f"  {t['wallet'][:12]}...  "
                  f"toxicity={t['toxicity_score']:+.1f}  "
                  f"win_rate={t['win_rate_5s']*100:.0f}%  "
                  f"trades={t['trade_count']}  "
                  f"vol=${t['total_volume_usdc']:,.0f}")

        print("\n--- Top 15 LEAST TOXIC Takers (crypto_15m, high confidence) ---")
        print("(These wallets consistently move price AGAINST their direction)")
        dumb = sorted(crypto_15m_takers, key=lambda x: x['toxicity_score'])[:15]
        for t in dumb:
            print(f"  {t['wallet'][:12]}...  "
                  f"toxicity={t['toxicity_score']:+.1f}  "
                  f"win_rate={t['win_rate_5s']*100:.0f}%  "
                  f"trades={t['trade_count']}  "
                  f"vol=${t['total_volume_usdc']:,.0f}")

    # By expiry bucket
    print("\n--- Toxicity by Expiry Bucket ---")
    by_bucket = defaultdict(list)
    for r in results:
        if r['market_type'] == 'crypto_15m':
            by_bucket[r['expiry_bucket']].append(r['toxicity_score'])

    for bucket in ['<10m', '10-30m', '>30m']:
        scores = by_bucket.get(bucket, [])
        if scores:
            avg = statistics.mean(scores)
            print(f"  {bucket}: avg_toxicity={avg:+.2f} (n={len(scores)})")

    # Structural LPs (makers who appear most often)
    print("\n--- Identifying Structural LPs ---")
    print("(Makers who appear in many sweeps - potential permanent liquidity)")

    # Count maker appearances from sweeps
    maker_counts = defaultdict(int)
    maker_volume = defaultdict(float)

    with open(sweeps_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                makers = json.loads(row.get('maker_wallets', '[]'))
                vol = float(row.get('total_usdc', 0) or 0)
                for m in makers:
                    maker_counts[m] += 1
                    maker_volume[m] += vol / len(makers)  # Split volume
            except:
                pass

    top_makers = sorted(maker_counts.items(), key=lambda x: -x[1])[:20]
    print("\nTop 20 most active makers:")
    for maker, count in top_makers:
        vol = maker_volume.get(maker, 0)
        print(f"  {maker[:12]}...  fills={count:,}  vol=${vol:,.0f}")


if __name__ == '__main__':
    main()
