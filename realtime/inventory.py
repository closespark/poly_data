"""
Inventory-Aware Modifiers

Lightweight inventory tracking that:
- Caps size_multiplier when inventory is skewed
- Adds mild skew on top of toxicity skew
- Does NOT try to optimize or predict

This is defensive, not speculative.

Usage:
    from realtime.inventory import InventoryManager

    inv = InventoryManager()

    # Record trades
    inv.record_fill(token_id, side='BUY', size=100.0)
    inv.record_fill(token_id, side='SELL', size=50.0)

    # Get modifiers
    mods = inv.get_modifiers(token_id)
    # mods = {
    #     'size_multiplier': 0.7,  # Reduce size when skewed
    #     'skew_direction': 'SELL',  # Favor selling to reduce inventory
    #     'skew_bps': 5,  # How much to skew quotes
    # }
"""

import time
from collections import defaultdict
from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class Position:
    """Inventory position for a single token"""
    token_id: str
    net_position: float = 0.0  # Positive = long, Negative = short
    total_bought: float = 0.0
    total_sold: float = 0.0
    last_update: float = 0.0


class InventoryManager:
    """
    Tracks inventory and provides defensive modifiers.

    Philosophy:
    - We're market makers, not position takers
    - Large inventory = risk
    - Goal is to stay flat, not to be right about direction
    """

    def __init__(self,
                 max_position_usd: float = 1000.0,  # Max position before aggressive reduction
                 target_position_usd: float = 0.0):  # Target (usually 0)
        self.max_position = max_position_usd
        self.target_position = target_position_usd

        # {token_id: Position}
        self.positions: Dict[str, Position] = {}

        # Risk limits
        self.HARD_LIMIT_MULTIPLIER = 3.0  # At 3x max, stop quoting that side
        self.SIZE_REDUCTION_START = 0.5  # Start reducing size at 50% of max

    def record_fill(self, token_id: str, side: str, size_usd: float):
        """
        Record a fill.

        Args:
            token_id: Token traded
            side: 'BUY' or 'SELL' (from our perspective)
            size_usd: USD value of fill
        """
        if token_id not in self.positions:
            self.positions[token_id] = Position(token_id=token_id)

        pos = self.positions[token_id]

        if side == 'BUY':
            pos.net_position += size_usd
            pos.total_bought += size_usd
        elif side == 'SELL':
            pos.net_position -= size_usd
            pos.total_sold += size_usd

        pos.last_update = time.time()

    def get_position(self, token_id: str) -> float:
        """Get current net position (positive = long)"""
        if token_id not in self.positions:
            return 0.0
        return self.positions[token_id].net_position

    def get_modifiers(self, token_id: str) -> dict:
        """
        Get inventory-based modifiers for quoting.

        Returns:
            size_multiplier: Scale quote size (0.0 to 1.0)
            skew_direction: 'BUY', 'SELL', or None
            skew_bps: Basis points to skew (0-20 typically)
            at_limit: True if at hard limit (should PULL that side)
            position_pct: Position as % of max (-100 to +100)
        """
        pos = self.get_position(token_id)
        pos_pct = (pos / self.max_position) * 100 if self.max_position > 0 else 0

        # Default: no modification
        result = {
            'size_multiplier': 1.0,
            'skew_direction': None,
            'skew_bps': 0,
            'at_limit': False,
            'position_pct': pos_pct,
        }

        abs_pos = abs(pos)
        abs_pct = abs(pos_pct)

        # At hard limit
        if abs_pos >= self.max_position * self.HARD_LIMIT_MULTIPLIER:
            result['at_limit'] = True
            result['size_multiplier'] = 0.0
            result['skew_direction'] = 'SELL' if pos > 0 else 'BUY'
            result['skew_bps'] = 20  # Aggressive skew
            return result

        # Size reduction (linear from SIZE_REDUCTION_START to max)
        if abs_pos > self.max_position * self.SIZE_REDUCTION_START:
            # Linear reduction: 1.0 at 50%, 0.3 at 100%
            reduction_range = self.max_position * (1.0 - self.SIZE_REDUCTION_START)
            excess = abs_pos - (self.max_position * self.SIZE_REDUCTION_START)
            reduction = min(excess / reduction_range, 1.0)
            result['size_multiplier'] = max(0.3, 1.0 - (reduction * 0.7))

        # Skew (proportional to position)
        if abs_pct > 20:  # Start skewing at 20% of max
            result['skew_direction'] = 'SELL' if pos > 0 else 'BUY'
            # 5 bps per 20% of max, capped at 15 bps
            result['skew_bps'] = min(int(abs_pct / 20) * 5, 15)

        return result

    def should_quote_side(self, token_id: str, side: str) -> bool:
        """
        Check if we should quote a specific side.

        Args:
            side: 'BUY' or 'SELL'

        Returns:
            True if OK to quote, False if at limit
        """
        mods = self.get_modifiers(token_id)

        if not mods['at_limit']:
            return True

        # At limit - only quote the side that reduces position
        if mods['skew_direction'] == side:
            return True  # This side reduces our risk

        return False  # This side increases our risk - don't quote

    def get_all_positions(self) -> Dict[str, dict]:
        """Get summary of all positions"""
        result = {}
        for token_id, pos in self.positions.items():
            result[token_id] = {
                'net_position': pos.net_position,
                'total_bought': pos.total_bought,
                'total_sold': pos.total_sold,
                'last_update': pos.last_update,
                'modifiers': self.get_modifiers(token_id),
            }
        return result

    def get_total_exposure(self) -> float:
        """Get total absolute exposure across all tokens"""
        return sum(abs(p.net_position) for p in self.positions.values())

    def reset_position(self, token_id: str):
        """Reset position for a token (e.g., after manual hedge)"""
        if token_id in self.positions:
            self.positions[token_id].net_position = 0.0

    def reset_all(self):
        """Reset all positions"""
        self.positions.clear()


# ============ Integration helper ============

def apply_inventory_modifiers(signal, inventory_mods: dict):
    """
    Apply inventory modifiers to a signal.

    Args:
        signal: Signal object from SignalEngine
        inventory_mods: Output from InventoryManager.get_modifiers()

    Returns:
        Modified signal (mutates in place and returns)
    """
    # Cap size multiplier
    signal.size_multiplier = min(
        signal.size_multiplier,
        inventory_mods['size_multiplier']
    )

    # Add skew if not already skewing
    if inventory_mods['skew_direction'] and inventory_mods['skew_bps'] > 0:
        # If signal already has a skew, use the more aggressive one
        # This is defensive - when in doubt, reduce risk
        pass  # For now, just let signal engine handle skew
        # In production, you'd adjust bid/ask prices here

    return signal


# Global instance
_inventory: Optional[InventoryManager] = None


def get_inventory_manager() -> InventoryManager:
    """Get or create global inventory manager"""
    global _inventory
    if _inventory is None:
        _inventory = InventoryManager()
    return _inventory
