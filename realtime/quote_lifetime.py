"""
Quote Lifetime Adaptation

Tracks quote behavior and adapts lifetime based on regime:
- Toxic regimes → shorter lifetime (avoid getting picked off)
- Benign regimes → longer lifetime (collect more rebates)

Metrics tracked:
- Time-at-best before fill
- Cancels before fill
- Fills immediately after reseed (danger signal)

Usage:
    from realtime.quote_lifetime import QuoteLifetimeAdapter

    adapter = QuoteLifetimeAdapter()

    # Record quote events
    adapter.record_fill(token_id, time_at_best=1.2, was_toxic=True)
    adapter.record_cancel(token_id, time_at_best=0.5)

    # Get recommended lifetime
    lifetime = adapter.get_lifetime(token_id, base_lifetime=5.0)
"""

import time
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import statistics


@dataclass
class QuoteEvent:
    """Single quote lifecycle event"""
    timestamp: float
    time_at_best: float  # Seconds quote was at best bid/ask
    outcome: str  # 'fill', 'cancel', 'reseed_fill'
    was_toxic: bool = False


class QuoteLifetimeAdapter:
    """
    Adapts quote lifetime based on observed fill patterns.

    Core insight:
    - In toxic regimes, you get filled quickly (adverse selection)
    - In benign regimes, quotes rest longer before fills
    - Shorten lifetime when fills are fast + toxic
    - Lengthen when flow is slow + benign
    """

    def __init__(self,
                 window_seconds: float = 300.0,  # 5 minute rolling window
                 min_events: int = 10):
        self.window = window_seconds
        self.min_events = min_events

        # {token_id: [QuoteEvent, ...]}
        self.events: Dict[str, List[QuoteEvent]] = defaultdict(list)

        # Baseline lifetimes (will adapt from these)
        self.BASE_LIFETIME_SECONDS = 5.0
        self.MIN_LIFETIME = 0.5  # Never go below 500ms
        self.MAX_LIFETIME = 30.0  # Never go above 30s

        # Danger thresholds
        self.FAST_FILL_THRESHOLD = 1.0  # Fill < 1s = suspicious
        self.RESEED_FILL_DANGER_WINDOW = 2.0  # Fill within 2s of reseed = very bad

    def record_fill(self, token_id: str, time_at_best: float,
                    was_toxic: bool = False, after_reseed: bool = False):
        """Record a fill event"""
        outcome = 'reseed_fill' if after_reseed else 'fill'
        self._add_event(token_id, time_at_best, outcome, was_toxic)

    def record_cancel(self, token_id: str, time_at_best: float):
        """Record a cancel event (we pulled before fill)"""
        self._add_event(token_id, time_at_best, 'cancel', False)

    def _add_event(self, token_id: str, time_at_best: float,
                   outcome: str, was_toxic: bool):
        """Add event and cleanup old ones"""
        now = time.time()
        self.events[token_id].append(QuoteEvent(
            timestamp=now,
            time_at_best=time_at_best,
            outcome=outcome,
            was_toxic=was_toxic
        ))
        self._cleanup(token_id, now)

    def _cleanup(self, token_id: str, now: float):
        """Remove events outside window"""
        cutoff = now - self.window
        self.events[token_id] = [
            e for e in self.events[token_id] if e.timestamp > cutoff
        ]

    def get_lifetime(self, token_id: str,
                     base_lifetime: float = None) -> float:
        """
        Get recommended quote lifetime for a token.

        Returns adapted lifetime in seconds.
        """
        base = base_lifetime or self.BASE_LIFETIME_SECONDS
        now = time.time()
        self._cleanup(token_id, now)

        events = self.events.get(token_id, [])
        if len(events) < self.min_events:
            # Not enough data, use base
            return base

        # Analyze fill patterns
        fills = [e for e in events if e.outcome in ('fill', 'reseed_fill')]
        if not fills:
            # No fills = lengthen (we're not getting hit)
            return min(base * 1.5, self.MAX_LIFETIME)

        # Metrics
        fill_times = [f.time_at_best for f in fills]
        avg_fill_time = statistics.mean(fill_times)
        toxic_fill_rate = sum(1 for f in fills if f.was_toxic) / len(fills)
        reseed_fill_rate = sum(1 for f in fills if f.outcome == 'reseed_fill') / len(fills)
        fast_fill_rate = sum(1 for f in fills if f.time_at_best < self.FAST_FILL_THRESHOLD) / len(fills)

        # Compute multiplier
        multiplier = 1.0

        # Fast fills → shorten
        if fast_fill_rate > 0.3:
            multiplier *= 0.7

        # Toxic fills → shorten
        if toxic_fill_rate > 0.2:
            multiplier *= 0.8

        # Reseed fills (very dangerous) → shorten significantly
        if reseed_fill_rate > 0.1:
            multiplier *= 0.5

        # Slow fills + low toxicity → lengthen
        if avg_fill_time > 3.0 and toxic_fill_rate < 0.1:
            multiplier *= 1.3

        # Apply multiplier with bounds
        lifetime = base * multiplier
        lifetime = max(self.MIN_LIFETIME, min(lifetime, self.MAX_LIFETIME))

        return lifetime

    def get_regime(self, token_id: str) -> str:
        """
        Classify current regime for a token.

        Returns: 'toxic', 'benign', 'neutral', 'unknown'
        """
        now = time.time()
        self._cleanup(token_id, now)

        events = self.events.get(token_id, [])
        if len(events) < self.min_events:
            return 'unknown'

        fills = [e for e in events if e.outcome in ('fill', 'reseed_fill')]
        if not fills:
            return 'benign'

        toxic_rate = sum(1 for f in fills if f.was_toxic) / len(fills)
        fast_rate = sum(1 for f in fills if f.time_at_best < self.FAST_FILL_THRESHOLD) / len(fills)

        if toxic_rate > 0.3 or fast_rate > 0.4:
            return 'toxic'
        elif toxic_rate < 0.1 and fast_rate < 0.2:
            return 'benign'
        else:
            return 'neutral'

    def get_stats(self, token_id: str) -> dict:
        """Get detailed stats for a token"""
        now = time.time()
        self._cleanup(token_id, now)

        events = self.events.get(token_id, [])
        fills = [e for e in events if e.outcome in ('fill', 'reseed_fill')]

        if not fills:
            return {
                'event_count': len(events),
                'fill_count': 0,
                'regime': self.get_regime(token_id),
                'recommended_lifetime': self.get_lifetime(token_id),
            }

        fill_times = [f.time_at_best for f in fills]

        return {
            'event_count': len(events),
            'fill_count': len(fills),
            'avg_fill_time': statistics.mean(fill_times),
            'median_fill_time': statistics.median(fill_times),
            'toxic_fill_rate': sum(1 for f in fills if f.was_toxic) / len(fills),
            'fast_fill_rate': sum(1 for f in fills if f.time_at_best < self.FAST_FILL_THRESHOLD) / len(fills),
            'reseed_fill_rate': sum(1 for f in fills if f.outcome == 'reseed_fill') / len(fills),
            'regime': self.get_regime(token_id),
            'recommended_lifetime': self.get_lifetime(token_id),
        }


# Global adapter instance (can be replaced)
_adapter: Optional[QuoteLifetimeAdapter] = None


def get_adapter() -> QuoteLifetimeAdapter:
    """Get or create global adapter"""
    global _adapter
    if _adapter is None:
        _adapter = QuoteLifetimeAdapter()
    return _adapter
