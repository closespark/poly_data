"""
Wallet Toxicity Scorer v3 - Production Grade

All critical fixes applied:
1. Uses sweeps.csv (one price per sweep, not per fill)
2. Per-maker volume from maker_volumes JSON
3. Confidence-weighted toxicity scoring

v1.1 Toxicity Formula:
toxicity_score = avg_markout_5s * log1p(trade_count) * log1p(total_volume_usdc) * (1 + avg_n_matches/3)

Output: wallet_toxicity.csv

Taker schema:
- wallet
- market_type
- expiry_bucket
- side (BUY/SELL - side-conditional toxicity)
- role (taker)
- trade_count
- total_volume_usdc
- avg_sweep_size
- avg_n_matches
- avg_normalized_slippage
- avg_markout_1s
- avg_markout_5s
- avg_markout_30s
- markout_stddev_5s
- win_rate_5s
- toxicity_score (v1.1 confidence-weighted)
- confidence

Maker schema (separate):
- wallet
- market_type
- expiry_bucket
- role (maker)
- fill_count
- total_volume_usdc
- avg_fill_size
- adverse_selection_1s
- adverse_selection_5s
- adverse_selection_30s
"""

import csv
import os
import json
import math
import statistics
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

DATA_DIR = os.environ.get('DATA_DIR', './data')

def get_data_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


class PriceSeries:
    """
    Maintains ONE price per sweep per token for markout calculation.
    Memory-efficient ring buffer per token.
    """

    def __init__(self, max_prices_per_token: int = 50000):
        self.max_per_token = max_prices_per_token
        # {token_id: [(timestamp, price), ...]}
        self.series: Dict[str, List[Tuple[int, float]]] = defaultdict(list)

    def add(self, token_id: str, timestamp: int, price: float):
        """Add ONE sweep price (not fill-level)"""
        if not token_id or timestamp <= 0 or price <= 0:
            return

        self.series[token_id].append((timestamp, price))

        # Trim if too large (keep most recent)
        if len(self.series[token_id]) > self.max_per_token:
            self.series[token_id] = self.series[token_id][-self.max_per_token:]

    def get_price_at(self, token_id: str, target_ts: int, tolerance: int = 5) -> Optional[float]:
        """Get closest price within tolerance seconds"""
        if token_id not in self.series:
            return None

        best_price = None
        best_diff = float('inf')

        for ts, price in self.series[token_id]:
            diff = abs(ts - target_ts)
            if diff <= tolerance and diff < best_diff:
                best_diff = diff
                best_price = price

        return best_price


class TakerAggregator:
    """
    Aggregates taker stats by (wallet, market_type, expiry_bucket, side).
    Side-conditional toxicity is important.
    """

    def __init__(self):
        # Key: (wallet, market_type, expiry_bucket, side)
        self.stats: Dict[Tuple[str, str, str, str], List[dict]] = defaultdict(list)

    def add_sweep(self, sweep: dict, markouts: Dict[int, Optional[float]]):
        key = (
            sweep['taker_wallet'],
            sweep['market_type'],
            sweep['time_to_expiry_bucket'],
            sweep['direction']
        )

        self.stats[key].append({
            'total_usdc': float(sweep['total_usdc'] or 0),
            'n_matches': int(sweep['n_matches'] or 1),
            'normalized_slippage': float(sweep['normalized_slippage'] or 0),
            'markout_1s': markouts.get(1),
            'markout_5s': markouts.get(5),
            'markout_30s': markouts.get(30),
        })

    def compute_scores(self, min_trades: int = 10) -> List[dict]:
        results = []

        for (wallet, market_type, expiry_bucket, side), sweeps in self.stats.items():
            if len(sweeps) < min_trades:
                continue

            # Aggregates
            volumes = [s['total_usdc'] for s in sweeps]
            matches = [s['n_matches'] for s in sweeps]
            slippages = [s['normalized_slippage'] for s in sweeps]

            # Markouts (filter None)
            m1 = [s['markout_1s'] for s in sweeps if s['markout_1s'] is not None]
            m5 = [s['markout_5s'] for s in sweeps if s['markout_5s'] is not None]
            m30 = [s['markout_30s'] for s in sweeps if s['markout_30s'] is not None]

            # Means
            avg_markout_1s = statistics.mean(m1) if len(m1) >= 5 else None
            avg_markout_5s = statistics.mean(m5) if len(m5) >= 5 else None
            avg_markout_30s = statistics.mean(m30) if len(m30) >= 5 else None

            markout_stddev_5s = statistics.stdev(m5) if len(m5) >= 10 else None

            # Win rate
            win_rate_5s = sum(1 for m in m5 if m > 0) / len(m5) if len(m5) >= 5 else None

            total_volume = sum(volumes)
            avg_n_matches = statistics.mean(matches)

            # v1.1 Toxicity Score
            # toxicity = markout * log(count) * log(volume) * (1 + depth/3)
            if avg_markout_5s is not None:
                toxicity_score = (
                    avg_markout_5s
                    * math.log1p(len(sweeps))
                    * math.log1p(total_volume)
                    * (1 + avg_n_matches / 3)
                    * 100  # Scale for readability
                )
            else:
                toxicity_score = 0

            # Confidence (0-1 scale)
            # Based on sample size and statistical significance
            confidence = min(len(sweeps) / 100, 1.0)
            if markout_stddev_5s and avg_markout_5s and len(m5) >= 10:
                se = markout_stddev_5s / math.sqrt(len(m5))
                if se > 0:
                    t_stat = abs(avg_markout_5s) / se
                    if t_stat > 2.5:  # p < 0.01
                        confidence = min(confidence * 1.5, 1.0)
                    elif t_stat < 1:  # Not significant
                        confidence *= 0.5

            results.append({
                'wallet': wallet,
                'market_type': market_type,
                'expiry_bucket': expiry_bucket,
                'side': side,
                'role': 'taker',
                'trade_count': len(sweeps),
                'total_volume_usdc': total_volume,
                'avg_sweep_size': statistics.mean(volumes),
                'avg_n_matches': avg_n_matches,
                'avg_normalized_slippage': statistics.mean(slippages),
                'avg_markout_1s': avg_markout_1s,
                'avg_markout_5s': avg_markout_5s,
                'avg_markout_30s': avg_markout_30s,
                'markout_stddev_5s': markout_stddev_5s,
                'win_rate_5s': win_rate_5s,
                'toxicity_score': toxicity_score,
                'confidence': confidence,
            })

        return results


class MakerAggregator:
    """
    Aggregates maker stats - SEPARATE from taker stats.
    Uses actual per-maker volume from maker_volumes JSON.
    """

    def __init__(self):
        # Key: (wallet, market_type, expiry_bucket)
        self.stats: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)

    def add_sweep(self, sweep: dict, markouts: Dict[int, Optional[float]]):
        """
        Add maker stats from a sweep.
        Each maker gets their ACTUAL volume from maker_volumes.
        """
        try:
            maker_volumes = json.loads(sweep.get('maker_volumes', '{}'))
        except:
            return

        market_type = sweep['market_type']
        expiry_bucket = sweep['time_to_expiry_bucket']
        direction = sweep['direction']

        for maker_wallet, maker_volume in maker_volumes.items():
            key = (maker_wallet, market_type, expiry_bucket)

            # For makers, adverse markout is OPPOSITE of taker direction
            # If taker BUYs and price goes up, maker who SOLD is adversely selected
            adverse_1s = -markouts.get(1) if markouts.get(1) is not None else None
            adverse_5s = -markouts.get(5) if markouts.get(5) is not None else None
            adverse_30s = -markouts.get(30) if markouts.get(30) is not None else None

            self.stats[key].append({
                'volume_usdc': maker_volume,
                'adverse_1s': adverse_1s,
                'adverse_5s': adverse_5s,
                'adverse_30s': adverse_30s,
                'taker_direction': direction,
            })

    def compute_scores(self, min_fills: int = 20) -> List[dict]:
        results = []

        for (wallet, market_type, expiry_bucket), fills in self.stats.items():
            if len(fills) < min_fills:
                continue

            volumes = [f['volume_usdc'] for f in fills]

            # Adverse selection (weighted by volume would be better, but keep simple)
            a1 = [f['adverse_1s'] for f in fills if f['adverse_1s'] is not None]
            a5 = [f['adverse_5s'] for f in fills if f['adverse_5s'] is not None]
            a30 = [f['adverse_30s'] for f in fills if f['adverse_30s'] is not None]

            avg_adverse_1s = statistics.mean(a1) if len(a1) >= 5 else None
            avg_adverse_5s = statistics.mean(a5) if len(a5) >= 5 else None
            avg_adverse_30s = statistics.mean(a30) if len(a30) >= 5 else None

            results.append({
                'wallet': wallet,
                'market_type': market_type,
                'expiry_bucket': expiry_bucket,
                'role': 'maker',
                'fill_count': len(fills),
                'total_volume_usdc': sum(volumes),
                'avg_fill_size': statistics.mean(volumes),
                'adverse_selection_1s': avg_adverse_1s,
                'adverse_selection_5s': avg_adverse_5s,
                'adverse_selection_30s': avg_adverse_30s,
            })

        return results


def main():
    print("=" * 60)
    print("WALLET TOXICITY SCORER v3 - Production Grade")
    print("=" * 60)
    print("\nFeatures:")
    print("  - One price per sweep (not per fill)")
    print("  - Per-maker volume from maker_volumes JSON")
    print("  - Side-conditional toxicity (BUY vs SELL)")
    print("  - v1.1 confidence-weighted scoring")

    sweeps_path = get_data_path('sweeps.csv')

    if not os.path.exists(sweeps_path):
        print(f"\nERROR: {sweeps_path} not found")
        print("Run build_sweeps_v2.py first!")
        return

    # Initialize
    price_series = PriceSeries()
    taker_agg = TakerAggregator()
    maker_agg = MakerAggregator()

    # Pass 1: Build price series (one price per sweep)
    print(f"\n[Pass 1] Building price series from sweeps...")
    sweep_count = 0

    with open(sweeps_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sweep_count += 1

            token_id = row.get('token_id', '')
            timestamp = int(row.get('timestamp', 0))
            price_vwap = float(row.get('price_vwap', 0) or 0)

            # ONE price per sweep (CRITICAL FIX)
            price_series.add(token_id, timestamp, price_vwap)

            if sweep_count % 1_000_000 == 0:
                print(f"  {sweep_count:,} sweeps indexed")

    print(f"  Total: {sweep_count:,} sweeps")
    print(f"  Tokens tracked: {len(price_series.series):,}")

    # Pass 2: Compute markouts and aggregate
    print(f"\n[Pass 2] Computing markouts and aggregating by wallet...")
    processed = 0

    with open(sweeps_path, 'r') as f:
        reader = csv.DictReader(f)

        for row in reader:
            processed += 1

            token_id = row.get('token_id', '')
            timestamp = int(row.get('timestamp', 0))
            entry_price = float(row.get('price_vwap', 0) or 0)
            direction = row.get('direction', '')

            if not token_id or timestamp <= 0 or entry_price <= 0:
                continue

            # Markouts
            markouts = {}
            for horizon in [1, 5, 30]:
                future_price = price_series.get_price_at(
                    token_id,
                    timestamp + horizon,
                    tolerance=max(2, horizon // 2)
                )

                if future_price is not None:
                    raw_markout = future_price - entry_price
                    # Flip for SELL (negative markout if price goes up)
                    if direction == 'SELL':
                        raw_markout = -raw_markout
                    markouts[horizon] = raw_markout

            # Aggregate
            taker_agg.add_sweep(row, markouts)
            maker_agg.add_sweep(row, markouts)

            if processed % 1_000_000 == 0:
                print(f"  {processed:,} sweeps processed")

    print(f"  Taker contexts: {len(taker_agg.stats):,}")
    print(f"  Maker contexts: {len(maker_agg.stats):,}")

    # Compute scores
    print(f"\n[Pass 3] Computing toxicity scores...")
    taker_results = taker_agg.compute_scores(min_trades=10)
    maker_results = maker_agg.compute_scores(min_fills=20)

    print(f"  Takers with sufficient data: {len(taker_results):,}")
    print(f"  Makers with sufficient data: {len(maker_results):,}")

    # Write taker output
    taker_output = get_data_path('wallet_toxicity_takers.csv')
    print(f"\nWriting taker toxicity to {taker_output}...")

    taker_columns = [
        'wallet', 'market_type', 'expiry_bucket', 'side', 'role',
        'trade_count', 'total_volume_usdc', 'avg_sweep_size',
        'avg_n_matches', 'avg_normalized_slippage',
        'avg_markout_1s', 'avg_markout_5s', 'avg_markout_30s',
        'markout_stddev_5s', 'win_rate_5s',
        'toxicity_score', 'confidence'
    ]

    with open(taker_output, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=taker_columns)
        writer.writeheader()
        for r in taker_results:
            for k in r:
                if isinstance(r[k], float):
                    r[k] = round(r[k], 6)
            writer.writerow(r)

    # Write maker output
    maker_output = get_data_path('wallet_toxicity_makers.csv')
    print(f"Writing maker adverse selection to {maker_output}...")

    maker_columns = [
        'wallet', 'market_type', 'expiry_bucket', 'role',
        'fill_count', 'total_volume_usdc', 'avg_fill_size',
        'adverse_selection_1s', 'adverse_selection_5s', 'adverse_selection_30s'
    ]

    with open(maker_output, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=maker_columns)
        writer.writeheader()
        for r in maker_results:
            for k in r:
                if isinstance(r[k], float):
                    r[k] = round(r[k], 6)
            writer.writerow(r)

    # ========== ANALYSIS ==========
    print("\n" + "=" * 60)
    print("ANALYSIS")
    print("=" * 60)

    # Top toxic takers by market type
    for mtype in ['crypto_15m', 'crypto_1h', 'political']:
        filtered = [r for r in taker_results
                   if r['market_type'] == mtype and r['confidence'] > 0.3]

        if not filtered:
            continue

        print(f"\n--- {mtype.upper()}: Top 10 Most Toxic Takers ---")
        toxic = sorted(filtered, key=lambda x: x['toxicity_score'], reverse=True)[:10]
        for t in toxic:
            print(f"  {t['wallet'][:10]}... {t['side']:4s} "
                  f"tox={t['toxicity_score']:+8.1f} "
                  f"win={t['win_rate_5s']*100 if t['win_rate_5s'] else 0:4.0f}% "
                  f"n={t['trade_count']:5d} "
                  f"vol=${t['total_volume_usdc']:,.0f}")

        print(f"\n--- {mtype.upper()}: Top 10 Dumb Money ---")
        dumb = sorted(filtered, key=lambda x: x['toxicity_score'])[:10]
        for t in dumb:
            print(f"  {t['wallet'][:10]}... {t['side']:4s} "
                  f"tox={t['toxicity_score']:+8.1f} "
                  f"win={t['win_rate_5s']*100 if t['win_rate_5s'] else 0:4.0f}% "
                  f"n={t['trade_count']:5d} "
                  f"vol=${t['total_volume_usdc']:,.0f}")

    # Maker adverse selection by context
    print(f"\n--- Maker Adverse Selection by Expiry Bucket (crypto_15m) ---")
    maker_15m = [r for r in maker_results if r['market_type'] == 'crypto_15m']

    by_bucket = defaultdict(list)
    for m in maker_15m:
        if m['adverse_selection_5s'] is not None:
            by_bucket[m['expiry_bucket']].append(m['adverse_selection_5s'])

    for bucket in ['<10m', '10-30m', '>30m']:
        vals = by_bucket.get(bucket, [])
        if vals:
            avg = statistics.mean(vals)
            print(f"  {bucket}: avg_adverse_selection_5s = {avg*10000:+.2f} bps (n={len(vals)})")

    # Structural LPs
    print(f"\n--- Top 20 Structural LPs (by fill count) ---")
    lp_summary = defaultdict(lambda: {'fills': 0, 'volume': 0, 'adverse': []})

    for m in maker_results:
        wallet = m['wallet']
        lp_summary[wallet]['fills'] += m['fill_count']
        lp_summary[wallet]['volume'] += m['total_volume_usdc']
        if m['adverse_selection_5s'] is not None:
            lp_summary[wallet]['adverse'].append(m['adverse_selection_5s'])

    top_lps = sorted(lp_summary.items(), key=lambda x: -x[1]['fills'])[:20]
    for wallet, stats in top_lps:
        avg_adv = statistics.mean(stats['adverse']) * 10000 if stats['adverse'] else 0
        print(f"  {wallet[:12]}...  fills={stats['fills']:,}  "
              f"vol=${stats['volume']:,.0f}  adverse={avg_adv:+.1f}bps")

    print(f"\n{'=' * 60}")
    print("COMPLETE")
    print(f"{'=' * 60}")
    print(f"\nOutputs:")
    print(f"  {taker_output}")
    print(f"  {maker_output}")


if __name__ == '__main__':
    main()
