# Polymarket Data

A comprehensive data pipeline for fetching, processing, and analyzing Polymarket trading data — plus a **real-time signal engine** for market making with wallet toxicity scoring.

## Quick Download

**First-time users**: Download the [latest data snapshot](https://polydata-archive.s3.us-east-1.amazonaws.com/archive.tar.xz) and extract it in the main repository directory before your first run. This will save you over 2 days of initial data collection time.

## Overview

This system has two modes:

### Offline Pipeline (Data Collection & Analysis)
1. **Market Data Collection** - Fetches all Polymarket markets with metadata
2. **Order Event Scraping** - Collects order-filled events from Goldsky subgraph
3. **Trade Processing** - Transforms raw order events into structured trade data
4. **Toxicity Analysis** - Builds wallet toxicity scores from markout analysis

### Real-Time Signal Engine (Market Making)
- **3-axis toxicity model**: outcome-based, identity-based, intent-based
- **Book integrity monitoring**: WS state with REST reconciliation
- **Full observability**: structured logging for threshold tuning

## Installation

This project uses [UV](https://docs.astral.sh/uv/) for fast, reliable package management.

### Install UV

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or with pip
pip install uv
```

### Install Dependencies

```bash
# Install all dependencies
uv sync

# Install with development dependencies (Jupyter, etc.)
uv sync --extra dev
```

## Quick Start

```bash
# Run with UV (recommended)
uv run python update_all.py

# Or activate the virtual environment first
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
python update_all.py
```

This will sequentially run all three pipeline stages:
- Update markets from Polymarket API
- Update order-filled events from Goldsky
- Process new orders into trades

## Project Structure

```
poly_data/
├── update_all.py              # Main orchestrator script
├── run_mm.py                  # Real-time market maker entry point
├── run_offline_pipeline.sh    # Offline analysis pipeline
├── TUNING.md                  # Signal engine tuning reference
│
├── update_utils/              # Data collection modules
│   ├── update_markets.py      # Fetch markets from Polymarket API
│   ├── update_goldsky.py      # Scrape order events from Goldsky
│   └── process_live.py        # Process orders into trades
│
├── analysis/                  # Offline analysis & toxicity
│   ├── build_sweeps.py        # Group fills into atomic sweeps
│   ├── build_wallet_toxicity.py  # Calculate markout-based toxicity
│   ├── wallet_enrichment.py   # Alchemy wallet enrichment
│   └── decision_analysis.py   # Post-hoc signal analysis toolkit
│
├── realtime/                  # Real-time signal engine
│   ├── signal_engine.py       # Core 3-axis toxicity engine
│   ├── book_store.py          # Order book management (WS + REST seed)
│   ├── book_reconciler.py     # REST vs WS drift detection
│   ├── alchemy_observer.py    # Live wallet enrichment
│   ├── websocket_client.py    # Polymarket WS client
│   ├── quote_lifetime.py      # Quote timing logic
│   └── inventory.py           # Position tracking
│
├── enrichment/                # Wallet enrichment pipeline
│   ├── alchemy_client.py      # Alchemy API wrapper
│   ├── enrich_toxicity.py     # Batch enrichment
│   └── pending_observer.py    # Mempool observation
│
├── poly_utils/                # Utility functions
│   └── utils.py               # Market loading and missing token handling
│
├── markets.csv                # Main markets dataset
├── goldsky/                   # Order-filled events (auto-generated)
│   └── orderFilled.csv
└── processed/                 # Processed trade data (auto-generated)
    └── trades.csv
```

## Real-Time Signal Engine

The signal engine provides defensive signals for market making based on a 3-axis toxicity model.

### The Three Axes

| Axis | Source | What it measures |
|------|--------|------------------|
| **Outcome-based** | `wallet_toxicity.csv` | Historical markout performance (did this wallet's trades move against you?) |
| **Identity-based** | Alchemy enrichment | Wallet age, DEX activity, contract interactions (bounded 0.8-1.5x multiplier) |
| **Intent-based** | Live observation | Burst patterns, book state, mempool activity |

### Quick Start

```bash
# 1. Build toxicity scores (after data collection)
DATA_DIR=/var/data bash run_offline_pipeline.sh

# 2. Run the signal engine
ALCHEMY_API_KEY=your_key python run_mm.py
```

### Signal Types

| Signal | Meaning | When triggered |
|--------|---------|----------------|
| `PULL` | Cancel quotes immediately | High toxicity taker detected |
| `WIDEN` | Increase spread | Book integrity issues, moderate risk |
| `HOLD` | Maintain current quotes | Normal conditions |

### Key Design Principles

1. **Alchemy EXTENDS, never INITIATES** — Identity enrichment modifies confidence, doesn't trigger actions alone
2. **Sweep as atomic unit** — Fills grouped by `transactionHash` for accurate markout
3. **Side-conditional toxicity** — BUY and SELL scored separately per wallet
4. **Context decay** — Cross-regime lookups (different market type/expiry) are discounted

### Observability

Two log streams for surgical tuning:
- `logs/decisions.jsonl` — Every signal decision with full context
- `logs/health.jsonl` — System health metrics (30s intervals)

See `TUNING.md` for threshold adjustment workflow.

---

## Data Files

### markets.csv
Market metadata including:
- Market question, outcomes, and tokens
- Creation/close times and slugs
- Trading volume and condition IDs
- Negative risk indicators

**Fields**: `createdAt`, `id`, `question`, `answer1`, `answer2`, `neg_risk`, `market_slug`, `token1`, `token2`, `condition_id`, `volume`, `ticker`, `closedTime`

### goldsky/orderFilled.csv
Raw order-filled events with:
- Maker/taker addresses and asset IDs
- Fill amounts and transaction hashes
- Unix timestamps

**Fields**: `timestamp`, `maker`, `makerAssetId`, `makerAmountFilled`, `taker`, `takerAssetId`, `takerAmountFilled`, `transactionHash`

### processed/trades.csv
Structured trade data including:
- Market ID mapping and trade direction
- Price, USD amount, and token amount
- Maker/taker roles and transaction details

**Fields**: `timestamp`, `market_id`, `maker`, `taker`, `nonusdc_side`, `maker_direction`, `taker_direction`, `price`, `usd_amount`, `token_amount`, `transactionHash`

## Pipeline Stages

### 1. Update Markets (`update_markets.py`)

Fetches all markets from Polymarket API in chronological order.

**Features**:
- Automatic resume from last offset (idempotent)
- Rate limiting and error handling
- Batch fetching (500 markets per request)

**Usage**:
```bash
uv run python -c "from update_utils.update_markets import update_markets; update_markets()"
```

### 2. Update Goldsky (`update_goldsky.py`)

Scrapes order-filled events from Goldsky subgraph API.

**Features**:
- Resumes from last timestamp automatically
- Handles GraphQL queries with pagination
- Deduplicates events

**Usage**:
```bash
uv run python -c "from update_utils.update_goldsky import update_goldsky; update_goldsky()"
```

### 3. Process Live Trades (`process_live.py`)

Processes raw order events into structured trades.

**Features**:
- Maps asset IDs to markets using token lookup
- Calculates prices and trade directions
- Identifies BUY/SELL sides
- Handles missing markets by discovering them from trades
- Incremental processing from last checkpoint

**Usage**:
```bash
uv run python -c "from update_utils.process_live import process_live; process_live()"
```

**Processing Logic**:
- Identifies non-USDC asset in each trade
- Maps to market and outcome token (token1/token2)
- Determines maker/taker directions (BUY/SELL)
- Calculates price as USDC amount per outcome token
- Converts amounts from raw units (divides by 10^6)

### 4. Offline Analysis Pipeline

After collecting trade data, run the analysis pipeline to build toxicity scores:

```bash
# Run full pipeline
DATA_DIR=/var/data bash run_offline_pipeline.sh
```

This runs three stages:
1. **build_sweeps.py** — Groups fills by `transactionHash` into atomic sweeps
2. **build_wallet_toxicity.py** — Calculates markout at 1s/5s/30s horizons, produces wallet toxicity scores
3. **wallet_enrichment.py** — Enriches with Alchemy data (wallet age, activity patterns)

**Output**: `wallet_toxicity.csv` — compact file with per-wallet, per-side toxicity scores

After toxicity is built, you can delete the large raw data files to reclaim disk space.

## Dependencies

Dependencies are managed via `pyproject.toml` and installed automatically with `uv sync`.

**Key Libraries**:
- `polars` - Fast DataFrame operations
- `pandas` - Data manipulation
- `gql` - GraphQL client for Goldsky
- `requests` - HTTP requests to Polymarket API
- `flatten-json` - JSON flattening for nested responses

**Development Dependencies** (optional, installed with `--extra dev`):
- `jupyter` - Interactive notebooks
- `notebook` - Jupyter notebook interface
- `ipykernel` - Python kernel for Jupyter

## Features

### Resumable Operations
All stages automatically resume from where they left off:
- **Markets**: Counts existing CSV rows to set offset
- **Goldsky**: Reads last timestamp from orderFilled.csv
- **Processing**: Finds last processed transaction hash

### Error Handling
- Automatic retries on network failures
- Rate limit detection and backoff
- Server error (500) handling
- Graceful fallbacks for missing data

### Missing Market Discovery
The processing stage automatically discovers markets that weren't in the initial markets.csv (e.g., markets created after last update) and fetches them via the Polymarket API, saving to `missing_markets.csv`.

## Data Schema Details

### Trade Direction Logic
- **Taker Direction**: BUY if paying USDC, SELL if receiving USDC
- **Maker Direction**: Opposite of taker direction
- **Price**: Always expressed as USDC per outcome token

### Asset Mapping
- `makerAssetId`/`takerAssetId` of "0" represents USDC
- Non-zero IDs are outcome token IDs (token1/token2 from markets)
- Each trade involves USDC and one outcome token

## Notes

- All amounts are normalized to standard decimal format (divided by 10^6)
- Timestamps are converted from Unix epoch to datetime
- Platform wallets (`0xc5d563a36ae78145c45a50134d48a1215220f80a`, `0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`) are tracked in `poly_utils/utils.py`
- Negative risk markets are flagged in the market data

## Troubleshooting

**Issue**: Markets not found during processing
**Solution**: Run `update_markets()` first, or let `process_live()` auto-discover them

**Issue**: Duplicate trades
**Solution**: Deduplication is automatic - re-run processing from scratch if needed

**Issue**: Rate limiting
**Solution**: The pipeline handles this automatically with exponential backoff

## Analysis

### Loading Data

```python
import pandas as pd
import polars as pl
from poly_utils import get_markets, PLATFORM_WALLETS

# Load markets
markets_df = get_markets()

# Load trades
df = pl.scan_csv("processed/trades.csv").collect(streaming=True)
df = df.with_columns(
    pl.col("timestamp").str.to_datetime().alias("timestamp")
)
```

### Filtering Trades by User

**Important**: When filtering for a specific user's trades, filter by the `maker` column. Even though it appears you're only getting trades where the user is the maker, this is how Polymarket generates events at the contract level. The `maker` column shows trades from that user's perspective including price.

```python
USERS = {
    'domah': '0x9d84ce0306f8551e02efef1680475fc0f1dc1344',
    '50pence': '0x3cf3e8d5427aed066a7a5926980600f6c3cf87b3',
    'fhantom': '0x6356fb47642a028bc09df92023c35a21a0b41885',
    'car': '0x7c3db723f1d4d8cb9c550095203b686cb11e5c6b',
    'theo4': '0x56687bf447db6ffa42ffe2204a05edaa20f55839'
}

# Get all trades for a specific user
trader_df = df.filter((pl.col("maker") == USERS['domah']))
```

## Deployment (Render)

### Environment Variables
```
DATA_DIR=/var/data
ALCHEMY_API_KEY=your_key_here
```

### Persistent Disk
Mount at `/var/data` with sufficient storage (150GB+ for full history, or filter to specific markets).

### Workflow
1. Deploy with data collection running (`update_all.py`)
2. Wait for data fetch to complete
3. Run offline pipeline from shell: `DATA_DIR=/var/data bash run_offline_pipeline.sh`
4. Switch to signal engine: `python run_mm.py`

### Disk Management
After toxicity scores are built, delete raw data to reclaim space:
```bash
rm /var/data/goldsky/orderFilled.csv
rm /var/data/processed/trades.csv
# Keep: wallet_toxicity.csv, markets.csv, sweeps.csv
```

---

## License

Go wild with it
