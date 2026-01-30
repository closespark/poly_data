"""
Decision Analysis - Post-hoc analysis of signal engine decisions

Run against JSONL logs from signal_engine.decisions logger.

Implements:
A. PULL effectiveness (lift over control)
B. Risk score calibration curve
C. Decay sanity check (cross-regime leakage)
D. Book integrity correlation

Usage:
    python analysis/decision_analysis.py decisions.jsonl trades.jsonl

Where:
    decisions.jsonl = output from signal_engine.decisions logger
    trades.jsonl = trade events with timestamps (for outcome measurement)
"""

import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import os

# Try pandas, fall back to pure python
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("[Warning] pandas not available, using pure Python (slower)")


@dataclass
class Decision:
    """Parsed decision log entry"""
    decision_id: str
    ts: float
    action: str
    token_id: str
    taker: str
    risk_score: float
    effective_tox: float
    decay: float
    book_age_ms: float
    book_ok: bool
    cooldown_remaining_s: float
    trigger_primary: str
    market_type: str
    expiry_bucket: str
    burst_intensity: float


@dataclass
class Trade:
    """Trade event for outcome measurement"""
    ts: float
    token_id: str
    taker: str
    side: str
    toxicity: float  # If known


def load_decisions(filepath: str) -> List[Decision]:
    """Load decisions from JSONL"""
    decisions = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                # Skip health events
                if d.get('event') == 'health':
                    continue
                # Skip cooldown extension events
                if d.get('event') == 'cooldown_extended':
                    continue

                decisions.append(Decision(
                    decision_id=d.get('decision_id', ''),
                    ts=d.get('ts', 0),
                    action=d.get('action', ''),
                    token_id=d.get('token_id', ''),
                    taker=d.get('taker', ''),
                    risk_score=d.get('risk_score', 0),
                    effective_tox=d.get('effective_tox', 0),
                    decay=d.get('decay', 1.0),
                    book_age_ms=d.get('book_age_ms', 0),
                    book_ok=d.get('book_ok', True),
                    cooldown_remaining_s=d.get('cooldown_remaining_s', 0),
                    trigger_primary=d.get('trigger_primary', ''),
                    market_type=d.get('market_type', ''),
                    expiry_bucket=d.get('expiry_bucket', ''),
                    burst_intensity=d.get('burst_intensity', 0),
                ))
            except (json.JSONDecodeError, KeyError) as e:
                continue
    return decisions


def load_trades(filepath: str) -> List[Trade]:
    """Load trades from JSONL (format TBD based on your trade logging)"""
    trades = []
    if not os.path.exists(filepath):
        return trades
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
                trades.append(Trade(
                    ts=t.get('ts', 0),
                    token_id=t.get('token_id', ''),
                    taker=t.get('taker', ''),
                    side=t.get('side', ''),
                    toxicity=t.get('toxicity', 0),
                ))
            except:
                continue
    return trades


# ============ Analysis A: PULL Effectiveness ============

def analyze_pull_effectiveness(decisions: List[Decision], trades: List[Trade],
                               windows: List[float] = [5.0, 10.0]) -> dict:
    """
    For each PULL decision, count toxic trades in t+0 to t+window.
    Compare to non-PULL decisions with similar risk_score.

    Returns lift = 1 - (post_pull_toxic / control_toxic)
    """
    # Index trades by token for fast lookup
    trades_by_token: Dict[str, List[Trade]] = defaultdict(list)
    for t in trades:
        trades_by_token[t.token_id].append(t)

    # Sort trades by time
    for token_id in trades_by_token:
        trades_by_token[token_id].sort(key=lambda x: x.ts)

    # Bucket decisions by risk_score (0.05 buckets)
    def risk_bucket(score: float) -> float:
        return round(score * 20) / 20  # 0.05 increments

    results = {}

    for window in windows:
        pull_toxic_counts = []
        control_toxic_counts = []

        # Group non-PULL decisions by (risk_bucket, market_type, expiry)
        control_groups: Dict[Tuple, List[Decision]] = defaultdict(list)

        for d in decisions:
            if d.action != 'PULL':
                key = (risk_bucket(d.risk_score), d.market_type, d.expiry_bucket)
                control_groups[key].append(d)

        # For each PULL, count toxic trades and find control
        for d in decisions:
            if d.action != 'PULL':
                continue

            # Count toxic trades in window
            token_trades = trades_by_token.get(d.token_id, [])
            toxic_count = 0
            for t in token_trades:
                if d.ts < t.ts <= d.ts + window:
                    if t.toxicity > 2.0:  # "toxic" threshold
                        toxic_count += 1

            pull_toxic_counts.append(toxic_count)

            # Find control group
            key = (risk_bucket(d.risk_score), d.market_type, d.expiry_bucket)
            controls = control_groups.get(key, [])

            # Sample control toxic counts
            for c in controls[:5]:  # Limit to avoid O(n²)
                c_toxic = 0
                c_trades = trades_by_token.get(c.token_id, [])
                for t in c_trades:
                    if c.ts < t.ts <= c.ts + window:
                        if t.toxicity > 2.0:
                            c_toxic += 1
                control_toxic_counts.append(c_toxic)

        # Calculate lift
        avg_pull = sum(pull_toxic_counts) / len(pull_toxic_counts) if pull_toxic_counts else 0
        avg_control = sum(control_toxic_counts) / len(control_toxic_counts) if control_toxic_counts else 0

        lift = 1 - (avg_pull / avg_control) if avg_control > 0 else 0

        results[f'window_{int(window)}s'] = {
            'pull_count': len(pull_toxic_counts),
            'control_count': len(control_toxic_counts),
            'avg_post_pull_toxic': round(avg_pull, 3),
            'avg_control_toxic': round(avg_control, 3),
            'lift': round(lift, 3),
            'interpretation': 'good' if lift > 0.3 else 'needs_tuning',
        }

    return results


# ============ Analysis B: Risk Score Calibration ============

def analyze_risk_calibration(decisions: List[Decision], trades: List[Trade]) -> dict:
    """
    Bucket by risk_score, show:
    - % PULL
    - Avg future markout (if available)
    - Avg re-hit count
    """
    # Bucket into 0.1 increments
    buckets: Dict[float, List[Decision]] = defaultdict(list)

    for d in decisions:
        bucket = round(d.risk_score * 10) / 10
        buckets[bucket].append(d)

    results = {}

    for bucket in sorted(buckets.keys()):
        decs = buckets[bucket]
        pull_count = sum(1 for d in decs if d.action == 'PULL')
        total = len(decs)

        results[f'risk_{bucket:.1f}'] = {
            'count': total,
            'pull_pct': round(pull_count / total * 100, 1) if total > 0 else 0,
            'avg_effective_tox': round(sum(d.effective_tox for d in decs) / total, 2) if total > 0 else 0,
            'avg_burst_intensity': round(sum(d.burst_intensity for d in decs) / total, 2) if total > 0 else 0,
        }

    # Check monotonicity
    pull_pcts = [results[k]['pull_pct'] for k in sorted(results.keys())]
    is_monotonic = all(pull_pcts[i] <= pull_pcts[i+1] for i in range(len(pull_pcts)-1))

    return {
        'buckets': results,
        'is_monotonic': is_monotonic,
        'interpretation': 'good' if is_monotonic else 'weights_may_be_peaky',
    }


# ============ Analysis C: Decay Sanity Check ============

def analyze_decay_leakage(decisions: List[Decision]) -> dict:
    """
    Check cases where trigger_primary = toxic_wallet but decay < 1.0
    Verify these aren't dominating PULLs.
    """
    toxic_pulls = [d for d in decisions if d.action == 'PULL' and d.trigger_primary == 'toxic_wallet']

    if not toxic_pulls:
        return {'toxic_pull_count': 0, 'message': 'No toxic wallet PULLs found'}

    # Group by decay level
    full_decay = [d for d in toxic_pulls if d.decay >= 0.99]
    partial_decay = [d for d in toxic_pulls if 0.7 <= d.decay < 0.99]
    heavy_decay = [d for d in toxic_pulls if d.decay < 0.7]

    total = len(toxic_pulls)

    results = {
        'toxic_pull_count': total,
        'full_match_pct': round(len(full_decay) / total * 100, 1),
        'partial_decay_pct': round(len(partial_decay) / total * 100, 1),
        'heavy_decay_pct': round(len(heavy_decay) / total * 100, 1),
    }

    # If heavy decay dominates, fallback rules may be too aggressive
    if results['heavy_decay_pct'] > 30:
        results['interpretation'] = 'fallback_too_aggressive'
        results['recommendation'] = 'Consider tightening decay thresholds'
    else:
        results['interpretation'] = 'healthy'

    return results


# ============ Analysis D: Book Integrity Correlation ============

def analyze_book_integrity(decisions: List[Decision]) -> dict:
    """
    Group decisions by book_age_ms buckets.
    Check decision distribution.
    """
    # Buckets: 0-250ms, 250-1000ms, 1000ms+
    buckets = {
        'fresh_0_250ms': [],
        'moderate_250_1000ms': [],
        'stale_1000ms_plus': [],
    }

    for d in decisions:
        if d.book_age_ms < 0:
            continue  # Unseeded
        elif d.book_age_ms < 250:
            buckets['fresh_0_250ms'].append(d)
        elif d.book_age_ms < 1000:
            buckets['moderate_250_1000ms'].append(d)
        else:
            buckets['stale_1000ms_plus'].append(d)

    results = {}

    for bucket_name, decs in buckets.items():
        if not decs:
            results[bucket_name] = {'count': 0}
            continue

        total = len(decs)
        pull_count = sum(1 for d in decs if d.action == 'PULL')
        widen_count = sum(1 for d in decs if d.action == 'WIDEN')
        book_integrity_count = sum(1 for d in decs if d.trigger_primary == 'book_integrity')

        results[bucket_name] = {
            'count': total,
            'pull_pct': round(pull_count / total * 100, 1),
            'widen_pct': round(widen_count / total * 100, 1),
            'book_integrity_trigger_pct': round(book_integrity_count / total * 100, 1),
            'avg_risk_score': round(sum(d.risk_score for d in decs) / total, 3),
        }

    # Check if stale books correlate with more defensive actions
    fresh_pull = results.get('fresh_0_250ms', {}).get('pull_pct', 0)
    stale_pull = results.get('stale_1000ms_plus', {}).get('pull_pct', 0)

    return {
        'buckets': results,
        'stale_vs_fresh_pull_ratio': round(stale_pull / fresh_pull, 2) if fresh_pull > 0 else 'N/A',
        'interpretation': 'integrity_gate_working' if stale_pull > fresh_pull else 'check_gate_thresholds',
    }


# ============ Analysis E: Cooldown Health ============

def analyze_cooldown_health(decisions: List[Decision]) -> dict:
    """
    Check for cooldown starvation:
    - What % of decisions have cooldown_remaining > 0?
    - Are we getting enough QUOTE opportunities?
    """
    total = len(decisions)
    if total == 0:
        return {'message': 'No decisions to analyze'}

    in_cooldown = sum(1 for d in decisions if d.cooldown_remaining_s > 0)
    quotes = sum(1 for d in decisions if d.action == 'QUOTE')
    pulls = sum(1 for d in decisions if d.action == 'PULL')

    cooldown_pct = round(in_cooldown / total * 100, 1)
    quote_pct = round(quotes / total * 100, 1)

    results = {
        'total_decisions': total,
        'in_cooldown_pct': cooldown_pct,
        'quote_pct': quote_pct,
        'pull_pct': round(pulls / total * 100, 1),
    }

    # Starvation check
    if cooldown_pct > 50:
        results['interpretation'] = 'cooldown_starvation'
        results['recommendation'] = 'Reduce PULL_COOLDOWN_SECONDS or MAX_COOLDOWN_EXTENSION'
    elif quote_pct < 30:
        results['interpretation'] = 'low_quote_opportunity'
        results['recommendation'] = 'Check if thresholds are too aggressive'
    else:
        results['interpretation'] = 'healthy'

    return results


# ============ Main ============

def run_all_analyses(decisions_file: str, trades_file: str = None):
    """Run all analyses and print results"""
    print("=" * 60)
    print("DECISION ANALYSIS")
    print("=" * 60)

    print(f"\nLoading decisions from {decisions_file}...")
    decisions = load_decisions(decisions_file)
    print(f"  Loaded {len(decisions):,} decisions")

    trades = []
    if trades_file and os.path.exists(trades_file):
        print(f"Loading trades from {trades_file}...")
        trades = load_trades(trades_file)
        print(f"  Loaded {len(trades):,} trades")

    if not decisions:
        print("\nNo decisions to analyze!")
        return

    # A. PULL Effectiveness
    print("\n" + "=" * 60)
    print("A. PULL EFFECTIVENESS")
    print("=" * 60)
    if trades:
        results = analyze_pull_effectiveness(decisions, trades)
        for window, data in results.items():
            print(f"\n{window}:")
            for k, v in data.items():
                print(f"  {k}: {v}")
    else:
        print("  (Skipped - no trade data for outcome measurement)")

    # B. Risk Score Calibration
    print("\n" + "=" * 60)
    print("B. RISK SCORE CALIBRATION")
    print("=" * 60)
    results = analyze_risk_calibration(decisions, trades)
    print(f"\nMonotonic: {results['is_monotonic']} ({results['interpretation']})")
    print("\nBuckets:")
    for bucket, data in results['buckets'].items():
        print(f"  {bucket}: count={data['count']}, pull_pct={data['pull_pct']}%, "
              f"avg_tox={data['avg_effective_tox']}, avg_burst={data['avg_burst_intensity']}")

    # C. Decay Sanity Check
    print("\n" + "=" * 60)
    print("C. DECAY SANITY CHECK")
    print("=" * 60)
    results = analyze_decay_leakage(decisions)
    for k, v in results.items():
        print(f"  {k}: {v}")

    # D. Book Integrity
    print("\n" + "=" * 60)
    print("D. BOOK INTEGRITY CORRELATION")
    print("=" * 60)
    results = analyze_book_integrity(decisions)
    print(f"\nStale vs Fresh PULL ratio: {results['stale_vs_fresh_pull_ratio']}")
    print(f"Interpretation: {results['interpretation']}")
    print("\nBuckets:")
    for bucket, data in results['buckets'].items():
        if data.get('count', 0) > 0:
            print(f"  {bucket}: count={data['count']}, pull_pct={data['pull_pct']}%, "
                  f"widen_pct={data['widen_pct']}%, book_trigger={data['book_integrity_trigger_pct']}%")

    # E. Cooldown Health
    print("\n" + "=" * 60)
    print("E. COOLDOWN HEALTH")
    print("=" * 60)
    results = analyze_cooldown_health(decisions)
    for k, v in results.items():
        print(f"  {k}: {v}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    action_counts = defaultdict(int)
    trigger_counts = defaultdict(int)
    for d in decisions:
        action_counts[d.action] += 1
        if d.trigger_primary:
            trigger_counts[d.trigger_primary] += 1

    print("\nAction distribution:")
    for action, count in sorted(action_counts.items(), key=lambda x: -x[1]):
        print(f"  {action}: {count:,} ({count/len(decisions)*100:.1f}%)")

    print("\nTrigger distribution:")
    for trigger, count in sorted(trigger_counts.items(), key=lambda x: -x[1]):
        print(f"  {trigger}: {count:,} ({count/len(decisions)*100:.1f}%)")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python decision_analysis.py <decisions.jsonl> [trades.jsonl]")
        print("\nExample:")
        print("  python analysis/decision_analysis.py logs/decisions.jsonl logs/trades.jsonl")
        sys.exit(1)

    decisions_file = sys.argv[1]
    trades_file = sys.argv[2] if len(sys.argv) > 2 else None

    run_all_analyses(decisions_file, trades_file)
