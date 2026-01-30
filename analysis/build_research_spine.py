"""
Build the canonical research spine table.

Joins trades.csv + markets.csv to create a single denormalized table
with all context needed for microstructure analysis.

Output: research_spine.parquet

Columns:
- trade_id (synthetic, for dedup)
- timestamp
- market_id
- token_id (which outcome token was traded)
- side (BUY/SELL from taker perspective)
- price
- size_usd
- size_tokens
- maker_wallet
- taker_wallet
- time_to_expiry_seconds
- time_to_expiry_bucket (30m, 10-30m, <10m, expired, no_expiry)
- market_type (crypto_15m, crypto_1h, crypto_4h, political, sports, other)
- question (for debugging/exploration)
- tx_hash

Designed for memory efficiency - processes in chunks.
"""

import csv
import os
import json
from datetime import datetime, timezone
from typing import Optional
import re

# Try to import pyarrow for parquet, fall back to CSV if not available
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False
    print("Warning: pyarrow not installed. Output will be CSV instead of parquet.")

# Configuration
DATA_DIR = os.environ.get('DATA_DIR', './data')
CHUNK_SIZE = 100_000

def get_data_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


def classify_market_type(ticker: str, question: str) -> str:
    """
    Classify market into type buckets.

    Returns: crypto_15m, crypto_1h, crypto_4h, crypto_other, political, sports, other
    """
    if not ticker:
        ticker = ""
    if not question:
        question = ""

    ticker_lower = ticker.lower()
    question_lower = question.lower()

    # Crypto detection
    crypto_keywords = ['btc', 'eth', 'bitcoin', 'ethereum', 'crypto', 'sol', 'solana']
    is_crypto = any(kw in ticker_lower or kw in question_lower for kw in crypto_keywords)

    if is_crypto:
        # Time bucket detection
        if '15m' in ticker_lower or '15 min' in question_lower:
            return 'crypto_15m'
        elif '1h' in ticker_lower or '1 hour' in question_lower:
            return 'crypto_1h'
        elif '4h' in ticker_lower or '4 hour' in question_lower:
            return 'crypto_4h'
        else:
            return 'crypto_other'

    # Political
    political_keywords = ['president', 'election', 'trump', 'biden', 'vote', 'congress', 'senate']
    if any(kw in question_lower for kw in political_keywords):
        return 'political'

    # Sports
    sports_keywords = ['nba', 'nfl', 'mlb', 'soccer', 'football', 'basketball', 'game', 'match', 'championship']
    if any(kw in question_lower for kw in sports_keywords):
        return 'sports'

    return 'other'


def calculate_expiry_bucket(trade_timestamp: str, closed_time: str) -> tuple[Optional[int], str]:
    """
    Calculate time to expiry and bucket.

    Returns: (seconds_to_expiry, bucket_name)
    Bucket names: '>30m', '10-30m', '<10m', 'expired', 'no_expiry'
    """
    if not closed_time or closed_time == '':
        return None, 'no_expiry'

    try:
        # Parse trade timestamp (format: 2024-01-15 12:30:45 or similar)
        trade_dt = None
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']:
            try:
                trade_dt = datetime.strptime(trade_timestamp.split('.')[0].replace('T', ' '), '%Y-%m-%d %H:%M:%S')
                break
            except:
                continue

        if trade_dt is None:
            return None, 'parse_error'

        # Parse closed time (might be unix timestamp or ISO format)
        if closed_time.isdigit():
            close_dt = datetime.fromtimestamp(int(closed_time) / 1000)  # Assuming milliseconds
        else:
            close_dt = datetime.strptime(closed_time.split('.')[0].replace('T', ' '), '%Y-%m-%d %H:%M:%S')

        seconds_to_expiry = (close_dt - trade_dt).total_seconds()

        if seconds_to_expiry < 0:
            return int(seconds_to_expiry), 'expired'
        elif seconds_to_expiry < 600:  # < 10 minutes
            return int(seconds_to_expiry), '<10m'
        elif seconds_to_expiry < 1800:  # 10-30 minutes
            return int(seconds_to_expiry), '10-30m'
        else:
            return int(seconds_to_expiry), '>30m'

    except Exception as e:
        return None, 'parse_error'


def load_markets() -> dict:
    """
    Load markets.csv into a lookup dictionary.

    Returns: {market_id: {token1, token2, closedTime, ticker, question, ...}}
    """
    markets = {}
    markets_path = get_data_path('markets.csv')

    print(f"Loading markets from {markets_path}...")

    with open(markets_path, 'r') as f:
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
                    'volume': row.get('volume', ''),
                    'neg_risk': row.get('neg_risk', ''),
                }

    print(f"Loaded {len(markets):,} markets")
    return markets


def build_token_to_market_map(markets: dict) -> dict:
    """
    Build reverse lookup: token_id -> (market_id, token_position)
    token_position is 'token1' or 'token2'
    """
    token_map = {}
    for market_id, market_data in markets.items():
        if market_data['token1']:
            token_map[market_data['token1']] = (market_id, 'token1')
        if market_data['token2']:
            token_map[market_data['token2']] = (market_id, 'token2')

    print(f"Built token map with {len(token_map):,} tokens")
    return token_map


def process_trades():
    """
    Main processing function.
    Reads trades.csv in chunks, enriches with market data, outputs research spine.
    """
    trades_path = get_data_path('trades.csv')
    output_path = get_data_path('research_spine.parquet' if HAS_PARQUET else 'research_spine.csv')

    # Load markets
    markets = load_markets()
    token_map = build_token_to_market_map(markets)

    # Define output schema
    columns = [
        'trade_id',
        'timestamp',
        'market_id',
        'token_id',
        'token_position',
        'side',
        'price',
        'size_usd',
        'size_tokens',
        'maker_wallet',
        'taker_wallet',
        'time_to_expiry_seconds',
        'time_to_expiry_bucket',
        'market_type',
        'question',
        'tx_hash'
    ]

    print(f"\nProcessing trades from {trades_path}...")
    print(f"Output will be written to {output_path}")

    # Process in chunks
    trade_id = 0
    total_processed = 0
    total_enriched = 0

    # For parquet, we'll accumulate batches
    all_rows = []

    with open(trades_path, 'r') as f:
        reader = csv.DictReader(f)

        chunk = []
        for row in reader:
            trade_id += 1

            # Get market context
            market_id = row.get('market_id', '')
            market_data = markets.get(market_id, {})

            # Determine token_id from nonusdc_side
            nonusdc_side = row.get('nonusdc_side', '')
            token_id = ''
            token_position = ''
            if nonusdc_side == 'token1':
                token_id = market_data.get('token1', '')
                token_position = 'token1'
            elif nonusdc_side == 'token2':
                token_id = market_data.get('token2', '')
                token_position = 'token2'

            # Calculate time to expiry
            timestamp = row.get('timestamp', '')
            closed_time = market_data.get('closedTime', '')
            tte_seconds, tte_bucket = calculate_expiry_bucket(timestamp, closed_time)

            # Classify market type
            market_type = classify_market_type(
                market_data.get('ticker', ''),
                market_data.get('question', '')
            )

            # Build output row
            output_row = {
                'trade_id': trade_id,
                'timestamp': timestamp,
                'market_id': market_id,
                'token_id': token_id,
                'token_position': token_position,
                'side': row.get('taker_direction', ''),  # Taker is aggressor
                'price': row.get('price', ''),
                'size_usd': row.get('usd_amount', ''),
                'size_tokens': row.get('token_amount', ''),
                'maker_wallet': row.get('maker', ''),
                'taker_wallet': row.get('taker', ''),
                'time_to_expiry_seconds': tte_seconds if tte_seconds is not None else '',
                'time_to_expiry_bucket': tte_bucket,
                'market_type': market_type,
                'question': market_data.get('question', '')[:200],  # Truncate for storage
                'tx_hash': row.get('transactionHash', '')
            }

            all_rows.append(output_row)
            total_processed += 1
            if market_id and market_data:
                total_enriched += 1

            # Progress update
            if total_processed % 1_000_000 == 0:
                print(f"  Processed {total_processed:,} trades, enriched {total_enriched:,}")

    print(f"\nTotal processed: {total_processed:,}")
    print(f"Total enriched: {total_enriched:,}")
    print(f"Enrichment rate: {total_enriched/total_processed*100:.1f}%")

    # Write output
    if HAS_PARQUET:
        print(f"\nWriting parquet to {output_path}...")
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pylist(all_rows)
        pq.write_table(table, output_path, compression='snappy')
    else:
        print(f"\nWriting CSV to {output_path}...")
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(all_rows)

    print(f"Done! Output written to {output_path}")

    # Print sample stats
    print("\n--- Sample Statistics ---")
    market_types = {}
    expiry_buckets = {}
    for row in all_rows[:100000]:  # Sample first 100k
        mt = row['market_type']
        eb = row['time_to_expiry_bucket']
        market_types[mt] = market_types.get(mt, 0) + 1
        expiry_buckets[eb] = expiry_buckets.get(eb, 0) + 1

    print("\nMarket types (first 100k trades):")
    for mt, count in sorted(market_types.items(), key=lambda x: -x[1]):
        print(f"  {mt}: {count:,}")

    print("\nExpiry buckets (first 100k trades):")
    for eb, count in sorted(expiry_buckets.items(), key=lambda x: -x[1]):
        print(f"  {eb}: {count:,}")


if __name__ == '__main__':
    process_trades()
