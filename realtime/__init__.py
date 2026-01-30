"""
Real-time market making infrastructure for Polymarket.

Components:
- BookStore: Order book state management
- PolymarketWebSocket: WebSocket client for real-time updates
- SignalEngine: Trading signal generation

Usage:
    from realtime import BookStore, PolymarketWebSocket, SignalEngine

    # Initialize
    store = BookStore()
    engine = SignalEngine(store, toxicity_path='data/wallet_toxicity_takers.csv')
    ws = PolymarketWebSocket(store)

    # On trade event
    def on_trade(trade):
        signal = engine.evaluate(
            trade['asset_id'],
            trade['taker'],
            trade.get('side')
        )
        if signal.action == Action.PULL:
            cancel_all_quotes()

    ws.on_trade = on_trade
    ws.subscribe_tokens(my_tokens)
    ws.run()
"""

from realtime.book_store import BookStore, OrderBook, PriceLevel
from realtime.websocket_client import PolymarketWebSocket
from realtime.signal_engine import (
    SignalEngine, Signal, Action, ToxicityLookup, ToxicFlowTracker, EnrichmentLookup
)
from realtime.alchemy_observer import AlchemyObserver, integrate_with_engine
from realtime.quote_lifetime import QuoteLifetimeAdapter, get_adapter as get_lifetime_adapter
from realtime.inventory import InventoryManager, get_inventory_manager, apply_inventory_modifiers

__all__ = [
    # Book state
    'BookStore',
    'OrderBook',
    'PriceLevel',

    # WebSocket
    'PolymarketWebSocket',

    # Signal generation
    'SignalEngine',
    'Signal',
    'Action',
    'ToxicityLookup',
    'ToxicFlowTracker',
    'EnrichmentLookup',

    # Alchemy observer
    'AlchemyObserver',
    'integrate_with_engine',

    # Quote lifetime
    'QuoteLifetimeAdapter',
    'get_lifetime_adapter',

    # Inventory
    'InventoryManager',
    'get_inventory_manager',
    'apply_inventory_modifiers',
]
