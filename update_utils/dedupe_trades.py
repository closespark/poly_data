#!/usr/bin/env python3
"""
Deduplicate trades.csv by removing duplicate rows.
Duplicates are identified by (timestamp, maker, taker, transactionHash).
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import subprocess
from poly_utils.config import get_data_path


def dedupe_trades():
    processed_file = get_data_path('processed/trades.csv')
    backup_file = get_data_path('processed/trades.csv.bak')
    temp_file = get_data_path('processed/trades_deduped.csv')
    checkpoint_file = get_data_path('processed/.goldsky_checkpoint')

    if not os.path.exists(processed_file):
        print("No trades.csv found, nothing to deduplicate")
        return

    print("=" * 60)
    print("🔧 Deduplicating trades.csv")
    print("=" * 60)

    # Count original rows
    result = subprocess.run(['wc', '-l', processed_file], capture_output=True, text=True)
    original_count = int(result.stdout.strip().split()[0])
    print(f"Original row count: {original_count:,}")

    # Use awk to dedupe - key is timestamp,maker,taker,transactionHash (cols 1,3,4,11)
    # Keep header, then dedupe rest
    print("Running deduplication...")

    # Extract header
    result = subprocess.run(['head', '-n', '1', processed_file], capture_output=True, text=True)
    header = result.stdout.strip()

    # Dedupe using sort -u on the unique key columns
    # This is memory-efficient for large files
    dedupe_cmd = f"""
    head -n 1 "{processed_file}" > "{temp_file}" && \
    tail -n +2 "{processed_file}" | sort -t',' -k1,1 -k3,3 -k4,4 -k11,11 -u >> "{temp_file}"
    """

    result = subprocess.run(dedupe_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error during deduplication: {result.stderr}")
        return

    # Count deduped rows
    result = subprocess.run(['wc', '-l', temp_file], capture_output=True, text=True)
    deduped_count = int(result.stdout.strip().split()[0])
    removed = original_count - deduped_count

    print(f"Deduped row count: {deduped_count:,}")
    print(f"Removed duplicates: {removed:,}")

    if removed > 0:
        # Backup original
        print(f"\nBacking up original to {backup_file}")
        os.rename(processed_file, backup_file)

        # Move deduped to original location
        os.rename(temp_file, processed_file)

        # Update checkpoint to match deduped count (minus header)
        new_checkpoint = deduped_count - 1
        with open(checkpoint_file, 'w') as f:
            f.write(str(new_checkpoint))
        print(f"Updated checkpoint to: {new_checkpoint:,}")

        print("\n✅ Deduplication complete!")
        print(f"   Backup saved to: {backup_file}")
    else:
        print("\n✅ No duplicates found!")
        os.remove(temp_file)


if __name__ == "__main__":
    dedupe_trades()
