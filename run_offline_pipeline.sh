#!/bin/bash
# Offline Pipeline - Run after data collection completes
#
# Usage:
#   ./run_offline_pipeline.sh
#
# Or with custom data dir:
#   DATA_DIR=/var/data ./run_offline_pipeline.sh

set -e  # Exit on error

DATA_DIR="${DATA_DIR:-./data}"
echo "=============================================="
echo "OFFLINE PIPELINE"
echo "DATA_DIR: $DATA_DIR"
echo "=============================================="

# Check if trades.csv exists
if [ ! -f "$DATA_DIR/trades.csv" ]; then
    echo "ERROR: $DATA_DIR/trades.csv not found"
    echo "Wait for data collection to complete first"
    exit 1
fi

TRADES_COUNT=$(wc -l < "$DATA_DIR/trades.csv")
echo "Found trades.csv with $TRADES_COUNT rows"

# Step 1: Build sweeps
echo ""
echo "=============================================="
echo "STEP 1: Building sweeps from trades..."
echo "=============================================="
time python analysis/build_sweeps_v2.py

if [ ! -f "$DATA_DIR/sweeps.csv" ]; then
    echo "ERROR: sweeps.csv not created"
    exit 1
fi
SWEEPS_COUNT=$(wc -l < "$DATA_DIR/sweeps.csv")
echo "Created sweeps.csv with $SWEEPS_COUNT rows"

# Step 2: Build wallet toxicity
echo ""
echo "=============================================="
echo "STEP 2: Building wallet toxicity scores..."
echo "=============================================="
time python analysis/build_wallet_toxicity_v3.py

if [ ! -f "$DATA_DIR/wallet_toxicity_takers.csv" ]; then
    echo "ERROR: wallet_toxicity_takers.csv not created"
    exit 1
fi
TOX_COUNT=$(wc -l < "$DATA_DIR/wallet_toxicity_takers.csv")
echo "Created wallet_toxicity_takers.csv with $TOX_COUNT wallet contexts"

# Step 3: Wallet enrichment (optional, requires ALCHEMY_API_KEY)
echo ""
echo "=============================================="
echo "STEP 3: Wallet enrichment..."
echo "=============================================="
if [ -z "$ALCHEMY_API_KEY" ]; then
    echo "ALCHEMY_API_KEY not set - skipping enrichment"
    echo "(Set ALCHEMY_API_KEY to enable wallet enrichment)"
else
    echo "Running wallet enrichment with Alchemy..."
    time python analysis/wallet_enrichment.py

    if [ -f "$DATA_DIR/wallet_enrichment.csv" ]; then
        ENRICH_COUNT=$(wc -l < "$DATA_DIR/wallet_enrichment.csv")
        echo "Created wallet_enrichment.csv with $ENRICH_COUNT wallets"
    fi
fi

# Summary
echo ""
echo "=============================================="
echo "PIPELINE COMPLETE"
echo "=============================================="
echo ""
echo "Outputs:"
ls -lh "$DATA_DIR"/*.csv 2>/dev/null | grep -E "(sweeps|toxicity|enrichment)" || true
echo ""
echo "Ready to run signal engine:"
echo "  python run_mm.py"
