#!/usr/bin/env python3
"""
Startup script for Render deployment.
Downloads and extracts the snapshot on first run, then runs the update pipeline.
"""

import os
import subprocess
import sys

from poly_utils.config import DATA_DIR, get_data_path


SNAPSHOT_URL = "https://poly-data-snapshot.onrender.com"


def download_snapshot():
    """Download and extract snapshot if data directory is empty."""
    markets_file = get_data_path("markets.csv")

    # Check if we already have data
    if os.path.exists(markets_file):
        print(f"✓ Data already exists at {DATA_DIR}, skipping snapshot download")
        return

    print(f"📥 No existing data found. Downloading snapshot...")
    print(f"   Fetching from: {SNAPSHOT_URL}")

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # Download and extract in one command
    # The snapshot service redirects to S3, curl -sL follows redirects
    cmd = f'curl -sL {SNAPSHOT_URL} | tar -xJ -C {DATA_DIR}'

    print(f"   Extracting to: {DATA_DIR}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"❌ Failed to download snapshot: {result.stderr}")
        sys.exit(1)

    print(f"✓ Snapshot extracted successfully")

    # List what we got
    for item in os.listdir(DATA_DIR):
        item_path = os.path.join(DATA_DIR, item)
        if os.path.isdir(item_path):
            print(f"   📁 {item}/")
        else:
            size = os.path.getsize(item_path)
            print(f"   📄 {item} ({size:,} bytes)")


def main():
    print("=" * 60)
    print("🚀 Poly Data Pipeline - Startup")
    print(f"   Data directory: {DATA_DIR}")
    print("=" * 60)

    # Step 1: Download snapshot if needed
    download_snapshot()

    # Step 2: Run the update pipeline
    print("\n" + "=" * 60)
    print("🔄 Running update pipeline...")
    print("=" * 60 + "\n")

    from update_utils.update_markets import update_markets
    from update_utils.update_goldsky import update_goldsky
    from update_utils.process_live import process_live

    print("Updating markets...")
    update_markets()

    print("\nUpdating goldsky...")
    update_goldsky()

    print("\nProcessing live...")
    process_live()

    print("\n" + "=" * 60)
    print("✅ Pipeline complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
