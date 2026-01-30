# Signal Engine Tuning Guide

## Architecture Summary

Three orthogonal axes:
1. **Outcome-based skill** (toxicity scorer) - "what has this wallet done?"
2. **Identity-based priors** (Alchemy enrichment) - "who is this wallet?"
3. **Intent-based pressure** (Alchemy live observer) - "what is this wallet doing right now?"

## Key Thresholds

| Parameter | Default | Location | Effect |
|-----------|---------|----------|--------|
| `TOXIC_THRESHOLD` | 5.0 | SignalEngine | Above this → PULL |
| `DUMB_THRESHOLD` | -3.0 | SignalEngine | Below this → TIGHTEN |
| `IMBALANCE_THRESHOLD` | 0.5 | SignalEngine | OFI trigger level |
| `TOXIC_BURST_COUNT` | 3 | SignalEngine | N trades in window → escalate |
| `TOXIC_INTENSITY_THRESHOLD` | 15.0 | SignalEngine | Composite burst threshold |
| `PULL_COOLDOWN_SECONDS` | 5.0 | SignalEngine | Base cooldown after PULL |
| `MAX_COOLDOWN_EXTENSION` | 10.0 | SignalEngine | Cap on total cooldown |
| `BOOK_STALE_THRESHOLD` | 30.0 | SignalEngine | Seconds → book_stale |
| `BEHAVIOR_MULT_MIN` | 0.8 | ToxicityLookup | Floor on enrichment |
| `BEHAVIOR_MULT_MAX` | 1.5 | ToxicityLookup | Cap on enrichment |

## Analysis Workflow

```bash
# Run decision analysis
python analysis/decision_analysis.py logs/decisions.jsonl logs/trades.jsonl
```

### A. PULL Effectiveness
- **Good**: lift > 0.3 (PULL reduces toxic trades by 30%+)
- **Action if low**: Increase `PULL_COOLDOWN_SECONDS` or lower `TOXIC_THRESHOLD`

### B. Risk Score Calibration
- **Good**: Monotonic increase in PULL% as risk_score rises
- **Action if peaky**: Adjust weights in risk_score formula (currently 0.6/0.3/0.1)

### C. Decay Sanity Check
- **Good**: <30% of PULLs at heavy decay (<0.7)
- **Action if high**: Tighten decay factors in `_apply_decay()`

### D. Book Integrity
- **Good**: Stale books trigger more WIDENs
- **Action if not**: Lower `BOOK_STALE_THRESHOLD`

### E. Cooldown Health
- **Good**: <50% decisions in cooldown, >30% QUOTE opportunities
- **Action if starved**: Reduce cooldown durations

## Failure Modes

### 1. Cooldown Starvation
**Symptom**: `cooldown_remaining_s` almost always > 0, few QUOTE opportunities

**Fix**:
- Reduce `PULL_COOLDOWN_SECONDS`
- Reduce `MAX_COOLDOWN_EXTENSION`
- Add per-token-per-minute cooldown cap

### 2. Burst Amplification from Alchemy Noise
**Symptom**: `burst_intensity` spikes without fills, many extensions without re-hits

**Fix**:
- Require confirmed fill before Alchemy can extend again
- Discount intent confidence when no execution follows

### 3. Enrichment Over-weighting
**Symptom**: Same wallets always trigger despite fading markouts

**Fix**:
- Decay `behavior_multiplier` over time unless reinforced by outcomes
- Tighten bounds (`BEHAVIOR_MULT_MIN`/`MAX`)

### 4. Book Staleness Blindspot
**Symptom**: Bad decisions correlated with high `book_age_ms`

**Fix**:
- Lower `BOOK_STALE_THRESHOLD`
- Consider more aggressive WIDEN when book_age > 500ms

## Log Streams

### decisions (signal_engine.decisions)
Every non-QUOTE decision with full breakdown:
```json
{
  "decision_id": "a3f8c2e1b9d04567",
  "event_id": "trade_12345",
  "action": "PULL",
  "risk_score": 0.612,
  "trigger_primary": "toxic_wallet",
  "trigger_secondary": [],
  "book_age_ms": 142.3,
  ...
}
```

### health (signal_engine.health)
Every 30s:
```json
{
  "event": "health",
  "msg_rate_per_min": 1250.5,
  "avg_book_age_ms": 85.2,
  "active_cooldowns": 3,
  ...
}
```

## Tuning Mode Checklist

- [ ] Run with logging enabled for 1 day
- [ ] Run `decision_analysis.py`
- [ ] Check PULL effectiveness (lift > 0.3?)
- [ ] Check risk calibration (monotonic?)
- [ ] Check decay distribution (heavy < 30%?)
- [ ] Check book correlation (stale = more defensive?)
- [ ] Check cooldown health (not starved?)
- [ ] Adjust ONE threshold at a time
- [ ] Re-run analysis, compare

## Key Invariants (Never Violate)

1. **Alchemy EXTENDS, never INITIATES** - `extend_cooldown()` returns False if no active cooldown
2. **Enrichment bounded** - `[0.8, 1.5]` prevents identity from dominating outcomes
3. **Book integrity gate first** - Always check before toxicity evaluation
4. **Confidence damps everything** - Low confidence → low effective toxicity
