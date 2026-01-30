import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from poly_utils.utils import get_markets, update_missing_tokens
from poly_utils.config import get_data_path

import subprocess
import csv
import json
from datetime import datetime, timezone

import pandas as pd

# Process in chunks to avoid memory issues
CHUNK_SIZE = 100_000


def load_markets_lookup():
    """Load markets and create a lookup dict for token -> (market_id, side)"""
    markets_df = get_markets()

    # Create lookup: asset_id -> (market_id, side)
    lookup = {}
    for row in markets_df.iter_rows(named=True):
        market_id = row['id']
        if row['token1']:
            lookup[str(row['token1'])] = (market_id, 'token1')
        if row['token2']:
            lookup[str(row['token2'])] = (market_id, 'token2')

    return lookup


def process_chunk(chunk_df, markets_lookup):
    """Process a chunk of order data into trades"""
    results = []

    for row in chunk_df.iter_rows(named=True):
        maker_asset_id = str(row['makerAssetId'])
        taker_asset_id = str(row['takerAssetId'])

        # Find non-USDC asset
        if maker_asset_id != "0":
            nonusdc_asset_id = maker_asset_id
        else:
            nonusdc_asset_id = taker_asset_id

        # Look up market
        if nonusdc_asset_id not in markets_lookup:
            continue  # Skip unknown markets

        market_id, side = markets_lookup[nonusdc_asset_id]

        # Determine assets
        maker_asset = "USDC" if maker_asset_id == "0" else side
        taker_asset = "USDC" if taker_asset_id == "0" else side

        # Calculate amounts
        maker_amount = row['makerAmountFilled'] / 10**6
        taker_amount = row['takerAmountFilled'] / 10**6

        # Determine directions
        if taker_asset == "USDC":
            taker_direction = "BUY"
            maker_direction = "SELL"
            usd_amount = taker_amount
            token_amount = maker_amount
            price = taker_amount / maker_amount if maker_amount > 0 else 0
        else:
            taker_direction = "SELL"
            maker_direction = "BUY"
            usd_amount = maker_amount
            token_amount = taker_amount
            price = maker_amount / taker_amount if taker_amount > 0 else 0

        # Non-USDC side
        nonusdc_side = maker_asset if maker_asset != "USDC" else taker_asset

        results.append({
            'timestamp': row['timestamp'],
            'market_id': market_id,
            'maker': row['maker'],
            'taker': row['taker'],
            'nonusdc_side': nonusdc_side,
            'maker_direction': maker_direction,
            'taker_direction': taker_direction,
            'price': price,
            'usd_amount': usd_amount,
            'token_amount': token_amount,
            'transactionHash': row['transactionHash']
        })

    return results


def get_timestamp_from_line(filepath, line_num):
    """Get the timestamp from a specific line in the goldsky file."""
    try:
        result = subprocess.run(
            ['sed', '-n', f'{line_num}p', filepath],
            capture_output=True, text=True
        )
        if result.stdout:
            parts = result.stdout.strip().split(',')
            if parts:
                ts = int(parts[0])
                return datetime.fromtimestamp(ts, tz=timezone.utc)
    except:
        pass
    return None


def process_live():
    processed_file = get_data_path('processed/trades.csv')
    goldsky_file = get_data_path("goldsky/orderFilled.csv")
    checkpoint_file = get_data_path('processed/.goldsky_checkpoint')

    print("=" * 60)
    print("🔄 Processing Live Trades (Chunked)")
    print(f"   Current time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    # Get last processed position from checkpoint file (most reliable)
    start_line = 0
    last_processed_time = None

    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            try:
                checkpoint_data = json.load(f)
                start_line = checkpoint_data.get('line', 0)
                last_ts = checkpoint_data.get('last_timestamp')
                if last_ts:
                    last_processed_time = datetime.fromtimestamp(last_ts, tz=timezone.utc)
                print(f"✓ Found checkpoint: line {start_line:,}")
                if last_processed_time:
                    print(f"   Last processed: {last_processed_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            except json.JSONDecodeError:
                # Old format - just a line number
                f.seek(0)
                checkpoint_data = f.read().strip()
                if checkpoint_data:
                    try:
                        start_line = int(checkpoint_data)
                        print(f"✓ Found checkpoint (legacy): line {start_line:,}")
                    except ValueError:
                        print(f"⚠ Invalid checkpoint data, will verify from processed file")

    # Verify checkpoint against processed file - use row count, not grep (grep is too slow on large files)
    if os.path.exists(processed_file) and start_line == 0:
        print(f"✓ Found existing processed file: {processed_file}")

        # Get last line info for logging
        result = subprocess.run(['tail', '-n', '1', processed_file], capture_output=True, text=True)
        last_line = result.stdout.strip()
        if last_line and ',' in last_line:
            splitted = last_line.split(',')
            print(f"📍 Last processed: {splitted[0]}")
            print(f"   Last hash: {splitted[-1][:16]}...")

        # Use row count as checkpoint (fast and reliable)
        print(f"\n🔍 Counting processed rows...")
        result = subprocess.run(['wc', '-l', processed_file], capture_output=True, text=True)
        processed_count = int(result.stdout.strip().split()[0]) - 1  # minus header
        if processed_count > 0:
            print(f"✓ Processed file has {processed_count:,} rows")
            start_line = processed_count
    elif not os.path.exists(processed_file):
        print("⚠ No existing processed file found - processing from beginning")

    # Load markets lookup (small, fits in memory)
    print(f"\n📂 Loading markets lookup...")
    markets_lookup = load_markets_lookup()
    print(f"✓ Loaded {len(markets_lookup):,} token mappings")

    # Count total lines for progress
    print(f"\n📂 Counting rows in {goldsky_file}...")
    result = subprocess.run(['wc', '-l', goldsky_file], capture_output=True, text=True)
    total_lines = int(result.stdout.strip().split()[0]) - 1  # minus header
    print(f"✓ Total rows: {total_lines:,}")

    # Check if already fully processed
    if start_line >= total_lines:
        print(f"\n✅ Already fully processed ({start_line:,} >= {total_lines:,} rows)")
        print("   Nothing new to process.")
        return

    print(f"✓ Resuming from line {start_line:,} ({total_lines - start_line:,} new rows to process)")

    # Ensure output directory exists
    processed_dir = get_data_path('processed')
    if not os.path.isdir(processed_dir):
        os.makedirs(processed_dir, exist_ok=True)

    # Process in chunks using CSV reader (memory efficient)
    print(f"\n⚙️  Processing in chunks of {CHUNK_SIZE:,}...")

    rows_processed = 0
    rows_written = 0
    chunk_buffer = []

    # Determine if we need to write header
    write_header = not os.path.exists(processed_file)

    with open(goldsky_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)

        for i, row in enumerate(reader):
            # Skip to start position
            if i < start_line:
                continue

            # Convert timestamp
            try:
                ts = pd.to_datetime(int(row['timestamp']), unit='s')
                row['timestamp'] = ts
            except:
                continue

            # Convert amounts to int
            try:
                row['makerAmountFilled'] = int(row['makerAmountFilled'])
                row['takerAmountFilled'] = int(row['takerAmountFilled'])
            except:
                continue

            chunk_buffer.append(row)

            # Process chunk when buffer is full
            if len(chunk_buffer) >= CHUNK_SIZE:
                processed = process_chunk_from_dicts(chunk_buffer, markets_lookup)
                if processed:
                    write_results(processed_file, processed, write_header)
                    write_header = False
                    rows_written += len(processed)

                rows_processed += len(chunk_buffer)

                # Save checkpoint BEFORE clearing buffer
                last_ts = chunk_buffer[-1]['timestamp'] if chunk_buffer else None
                checkpoint = {
                    'line': i + 1,
                    'last_timestamp': last_ts.timestamp() if hasattr(last_ts, 'timestamp') and last_ts else None
                }
                with open(checkpoint_file, 'w') as f:
                    json.dump(checkpoint, f)

                chunk_buffer = []

                # Progress update
                pct = (i / total_lines) * 100 if total_lines > 0 else 0
                print(f"   Processed {rows_processed:,} rows ({pct:.1f}%) - Written {rows_written:,} trades")

    # Process remaining buffer
    if chunk_buffer:
        processed = process_chunk_from_dicts(chunk_buffer, markets_lookup)
        if processed:
            write_results(processed_file, processed, write_header)
            rows_written += len(processed)
        rows_processed += len(chunk_buffer)

    # Save final checkpoint with timestamp
    # Get last timestamp from the goldsky file
    result = subprocess.run(['tail', '-n', '1', goldsky_file], capture_output=True, text=True)
    last_ts = None
    if result.stdout:
        try:
            last_ts = int(result.stdout.strip().split(',')[0])
        except:
            pass

    checkpoint = {
        'line': total_lines,
        'last_timestamp': last_ts
    }
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint, f)

    if last_ts:
        last_time = datetime.fromtimestamp(last_ts, tz=timezone.utc)
        print(f"   Data up to: {last_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    print(f"\n" + "=" * 60)
    print(f"✅ Processing complete!")
    print(f"   Total rows processed: {rows_processed:,}")
    print(f"   Total trades written: {rows_written:,}")
    print("=" * 60)


def process_chunk_from_dicts(chunk, markets_lookup):
    """Process a chunk of dict rows into trades"""
    results = []

    for row in chunk:
        maker_asset_id = str(row['makerAssetId'])
        taker_asset_id = str(row['takerAssetId'])

        # Find non-USDC asset
        if maker_asset_id != "0":
            nonusdc_asset_id = maker_asset_id
        else:
            nonusdc_asset_id = taker_asset_id

        # Look up market
        if nonusdc_asset_id not in markets_lookup:
            continue  # Skip unknown markets

        market_id, side = markets_lookup[nonusdc_asset_id]

        # Determine assets
        maker_asset = "USDC" if maker_asset_id == "0" else side
        taker_asset = "USDC" if taker_asset_id == "0" else side

        # Calculate amounts
        maker_amount = row['makerAmountFilled'] / 10**6
        taker_amount = row['takerAmountFilled'] / 10**6

        # Determine directions
        if taker_asset == "USDC":
            taker_direction = "BUY"
            maker_direction = "SELL"
            usd_amount = taker_amount
            token_amount = maker_amount
            price = taker_amount / maker_amount if maker_amount > 0 else 0
        else:
            taker_direction = "SELL"
            maker_direction = "BUY"
            usd_amount = maker_amount
            token_amount = taker_amount
            price = maker_amount / taker_amount if taker_amount > 0 else 0

        # Non-USDC side
        nonusdc_side = maker_asset if maker_asset != "USDC" else taker_asset

        results.append([
            row['timestamp'],
            market_id,
            row['maker'],
            row['taker'],
            nonusdc_side,
            maker_direction,
            taker_direction,
            price,
            usd_amount,
            token_amount,
            row['transactionHash']
        ])

    return results


def write_results(filepath, results, include_header):
    """Append results to CSV file"""
    mode = 'a' if os.path.exists(filepath) and not include_header else 'w'

    with open(filepath, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if include_header:
            writer.writerow([
                'timestamp', 'market_id', 'maker', 'taker', 'nonusdc_side',
                'maker_direction', 'taker_direction', 'price', 'usd_amount',
                'token_amount', 'transactionHash'
            ])
        writer.writerows(results)


if __name__ == "__main__":
    process_live()
