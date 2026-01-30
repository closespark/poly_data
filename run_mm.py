#!/usr/bin/env python3
"""
Polymarket Market Making System - Main Runner

This script orchestrates the full system:
1. Loads market context from markets.csv
2. Loads wallet toxicity from offline analysis
3. Connects to Polymarket WebSocket
4. Generates real-time signals based on flow

Usage:
    python run_mm.py --market-type crypto_15m --max-tokens 50

This is a SIGNAL GENERATION system, not an execution system.
It outputs signals that a separate execution layer would act on.
"""

import argparse
import csv
import os
import sys
import time
import json
from datetime import datetime
from collections import defaultdict

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from realtime import (
    BookStore, PolymarketWebSocket, SignalEngine, Action,
    QuoteLifetimeAdapter, InventoryManager, apply_inventory_modifiers,
    AlchemyObserver, integrate_with_engine
)


def load_markets(data_dir: str, market_type_filter: str = None) -> dict:
    """
    Load markets.csv and build token -> context mapping.

    Returns: {token_id: {market_id, market_type, expiry_bucket, question}}
    """
    markets_path = os.path.join(data_dir, 'markets.csv')
    token_contexts = {}

    print(f"Loading markets from {markets_path}...")

    with open(markets_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = (row.get('ticker', '') or '').lower()
            question = (row.get('question', '') or '').lower()
            closed_time = row.get('closedTime', '')

            # Classify market type
            if 'btc' in ticker or 'eth' in ticker or 'bitcoin' in question:
                if '15m' in ticker:
                    market_type = 'crypto_15m'
                elif '1h' in ticker:
                    market_type = 'crypto_1h'
                elif '4h' in ticker:
                    market_type = 'crypto_4h'
                else:
                    market_type = 'crypto_other'
            elif any(kw in question for kw in ['president', 'election', 'trump', 'biden']):
                market_type = 'political'
            else:
                market_type = 'other'

            # Apply filter
            if market_type_filter and market_type != market_type_filter:
                continue

            # Calculate expiry bucket (rough, will be refined in real-time)
            if closed_time:
                try:
                    if closed_time.isdigit():
                        close_ts = int(closed_time) // 1000
                    else:
                        close_ts = int(datetime.fromisoformat(
                            closed_time.replace('Z', '+00:00')
                        ).timestamp())

                    now = time.time()
                    tte = close_ts - now

                    if tte < 0:
                        expiry_bucket = 'expired'
                    elif tte < 600:
                        expiry_bucket = '<10m'
                    elif tte < 1800:
                        expiry_bucket = '10-30m'
                    else:
                        expiry_bucket = '>30m'
                except:
                    expiry_bucket = 'no_expiry'
            else:
                expiry_bucket = 'no_expiry'

            # Skip expired markets
            if expiry_bucket == 'expired':
                continue

            # Add both tokens
            for token_key in ['token1', 'token2']:
                token_id = row.get(token_key, '')
                if token_id:
                    token_contexts[token_id] = {
                        'market_id': row.get('id', ''),
                        'market_type': market_type,
                        'expiry_bucket': expiry_bucket,
                        'question': row.get('question', '')[:100],
                        'ticker': row.get('ticker', ''),
                    }

    print(f"Loaded {len(token_contexts):,} tokens")
    return token_contexts


def main():
    parser = argparse.ArgumentParser(description='Polymarket Market Making Signal Generator')
    parser.add_argument('--data-dir', default=os.environ.get('DATA_DIR', './data'),
                       help='Data directory')
    parser.add_argument('--market-type', default='crypto_15m',
                       help='Market type to track (crypto_15m, crypto_1h, political, etc)')
    parser.add_argument('--max-tokens', type=int, default=100,
                       help='Maximum number of tokens to track')
    parser.add_argument('--signal-log', default='signals.jsonl',
                       help='Output file for signals')
    parser.add_argument('--enable-alchemy', action='store_true',
                       help='Enable Alchemy pending tx observer (requires ALCHEMY_API_KEY)')

    args = parser.parse_args()

    print("=" * 60)
    print("POLYMARKET MARKET MAKING SIGNAL GENERATOR")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  Data dir: {args.data_dir}")
    print(f"  Market type: {args.market_type}")
    print(f"  Max tokens: {args.max_tokens}")
    print(f"  Signal log: {args.signal_log}")

    # Load markets
    token_contexts = load_markets(args.data_dir, args.market_type)

    if not token_contexts:
        print(f"\nNo tokens found for market type '{args.market_type}'")
        return

    # Limit tokens
    tokens_to_track = list(token_contexts.keys())[:args.max_tokens]
    print(f"\nTracking {len(tokens_to_track)} tokens")

    # Initialize components
    print("\nInitializing components...")
    store = BookStore()
    engine = SignalEngine(
        store,
        toxicity_path=os.path.join(args.data_dir, 'wallet_toxicity_takers.csv')
    )

    # Quote lifetime adapter
    lifetime_adapter = QuoteLifetimeAdapter()

    # Inventory manager
    inventory = InventoryManager(max_position_usd=1000.0)

    # Set market contexts
    for token_id in tokens_to_track:
        ctx = token_contexts[token_id]
        engine.set_market_context(
            token_id,
            ctx['market_type'],
            ctx['expiry_bucket']
        )

    # Signal stats
    signal_counts = defaultdict(int)
    signal_log_file = open(args.signal_log, 'a')

    def on_trade(trade):
        """Handle trade event - generate signal"""
        token_id = trade.get('asset_id', '')
        taker = trade.get('taker', trade.get('maker', ''))  # Fallback to maker
        side = trade.get('side', '')
        size_usd = float(trade.get('size', 0) or 0)

        if token_id not in tokens_to_track:
            return

        # Generate base signal
        signal = engine.evaluate(token_id, taker, side)

        # Apply inventory modifiers
        inv_mods = inventory.get_modifiers(token_id)
        signal = apply_inventory_modifiers(signal, inv_mods)

        # Track fill for lifetime adaptation
        was_toxic = signal.toxicity_score > 2.0
        fill_time = 1.0  # Placeholder - would need quote tracking for real value
        lifetime_adapter.record_fill(token_id, fill_time, was_toxic)

        # Track fill for inventory
        if side in ('BUY', 'SELL'):
            # Note: This is the TAKER's side. If we were filled, we're on opposite side
            our_side = 'SELL' if side == 'BUY' else 'BUY'
            inventory.record_fill(token_id, our_side, size_usd)

        signal_counts[signal.action.value] += 1

        # Get recommended quote lifetime
        lifetime = lifetime_adapter.get_lifetime(token_id)

        # Log non-trivial signals
        if signal.action != Action.QUOTE or inv_mods['position_pct'] > 50:
            ctx = token_contexts.get(token_id, {})
            log_entry = {
                'ts': time.time(),
                'token_id': token_id[:20],
                'ticker': ctx.get('ticker', ''),
                'taker': taker[:12] if taker else '',
                'action': signal.action.value,
                'reason': signal.reason,
                'toxicity': signal.toxicity_score,
                'spread_mult': signal.spread_multiplier,
                'size_mult': signal.size_multiplier,
                'confidence': signal.confidence,
                'inv_position_pct': inv_mods['position_pct'],
                'inv_skew': inv_mods['skew_direction'],
                'quote_lifetime': lifetime,
                'regime': lifetime_adapter.get_regime(token_id),
            }

            # Print and log
            inv_info = f"inv={inv_mods['position_pct']:+.0f}%" if abs(inv_mods['position_pct']) > 20 else ""
            print(f"[SIGNAL] {signal.action.value:8s} | {ctx.get('ticker', '')[:20]:20s} | "
                  f"tox={signal.toxicity_score:+6.1f} | {inv_info:10s} | {signal.reason}")

            signal_log_file.write(json.dumps(log_entry) + '\n')
            signal_log_file.flush()

    # Initialize WebSocket
    ws = PolymarketWebSocket(store, on_trade=on_trade)
    ws.subscribe_tokens(tokens_to_track)

    # Optional: Alchemy live observer
    # Feeds ToxicFlowTracker, EXTENDS cooldowns (never initiates)
    alchemy_observer = None
    if args.enable_alchemy:
        alchemy_key = os.environ.get('ALCHEMY_API_KEY', '')
        if alchemy_key:
            print("\nInitializing Alchemy observer...")

            alchemy_observer = AlchemyObserver(api_key=alchemy_key)

            # Integrate with signal engine
            # This connects: observer → toxic_flow_tracker, pull_cooldowns
            integrate_with_engine(alchemy_observer, engine)

            alchemy_observer.start()
        else:
            print("\nWarning: --enable-alchemy set but ALCHEMY_API_KEY not found")

    # Status printer
    def print_status():
        while ws.running:
            time.sleep(30)

            ws_stats = ws.get_stats()
            engine_stats = engine.get_stats()
            total_exposure = inventory.get_total_exposure()

            print(f"\n{'='*50}")
            print(f"[STATUS] {datetime.now().strftime('%H:%M:%S')}")
            print(f"  Messages: {ws_stats['messages_received']:,}")
            print(f"  Books tracked: {ws_stats['books_tracked']}")
            print(f"  Signals generated: {engine_stats['signals_generated']:,}")
            print(f"  Signal breakdown: {dict(signal_counts)}")
            print(f"  Active cooldowns: {engine_stats['active_cooldowns']}")
            print(f"  Escalations: {engine_stats['escalations_triggered']}")
            print(f"  Total exposure: ${total_exposure:,.2f}")

            # Show top positions
            positions = inventory.get_all_positions()
            if positions:
                top_pos = sorted(positions.items(),
                               key=lambda x: abs(x[1]['net_position']),
                               reverse=True)[:5]
                if top_pos:
                    print(f"  Top positions:")
                    for tok, pos in top_pos:
                        ctx = token_contexts.get(tok, {})
                        ticker = ctx.get('ticker', tok[:12])[:15]
                        print(f"    {ticker}: ${pos['net_position']:+,.2f}")

            # Alchemy stats if enabled
            if alchemy_observer:
                alchemy_stats = alchemy_observer.get_stats()
                print(f"  Alchemy pending seen: {alchemy_stats['pending_seen']:,}")
                print(f"  Toxic intent detected: {alchemy_stats['toxic_detected']}")
                print(f"  Cooldowns extended: {alchemy_stats['cooldowns_extended']}")

            print(f"{'='*50}\n")

    import threading
    status_thread = threading.Thread(target=print_status, daemon=True)
    status_thread.start()

    # Run
    print("\nStarting WebSocket connection...")
    print("Press Ctrl+C to stop\n")

    try:
        ws.run()
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        ws.stop()
        signal_log_file.close()

        # Final stats
        print(f"\nFinal Statistics:")
        print(f"  Total signals: {engine.get_stats()['signals_generated']:,}")
        print(f"  Breakdown: {dict(signal_counts)}")
        print(f"  Signals logged to: {args.signal_log}")


if __name__ == '__main__':
    main()
