"""
Wallet Enrichment - Offline Identity & Behavior Priors

Creates wallet_enrichment.csv with identity signals:
- Who is this wallet? (not "are they toxic")
- Bot vs human
- Capital tier
- Funding source

This is SEPARATE from toxicity scoring.
At runtime: effective_toxicity = toxicity * behavior_multiplier

Output: wallet_enrichment.csv

Schema:
- wallet
- first_seen_ts
- tx_count
- active_days
- eth_balance
- funding_source (coinbase/binance/bridge/unknown)
- wallet_type (human/bot/contract)
- bot_likelihood (0-1)
- capital_tier (small/medium/large/whale)
- enrichment_confidence (0-1)
- behavior_multiplier (computed modifier for toxicity)

Usage:
    export ALCHEMY_API_KEY=your_key
    python analysis/wallet_enrichment.py
"""

import csv
import os
import sys
import time
import requests
import json
from typing import Dict, List, Optional, Set
from datetime import datetime
from collections import defaultdict

DATA_DIR = os.environ.get('DATA_DIR', './data')

# Known funding sources
CEX_WALLETS = {
    '0x71660c4005ba85c37ccec55d0c4493e66fe775d3': 'coinbase',
    '0x503828976d22510aad0201ac7ec88293211d23da': 'coinbase',
    '0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740': 'coinbase',
    '0x28c6c06298d514db089934071355e5743bf21d60': 'binance',
    '0x21a31ee1afc51d94c2efccaa2092ad1028285549': 'binance',
    '0x2910543af39aba0cd09dbb2d50200b3e800a63d2': 'kraken',
    '0xf89d7b9c864f589bbf53a82105107622b35eaa40': 'bybit',
}

BRIDGE_CONTRACTS = {
    '0x88ad09518695c6c3712ac10a214be5109a655671': 'polygon_bridge',
    '0xa0c68c638235ee32657e8f720a23cec1bfc77c77': 'polygon_pos_bridge',
}


class AlchemyEnricher:
    """Alchemy client for wallet enrichment"""

    def __init__(self, api_key: str, network: str = 'polygon-mainnet'):
        self.api_key = api_key
        self.base_url = f'https://{network}.g.alchemy.com/v2/{api_key}'
        self.last_call = 0
        self.min_interval = 0.05  # 20 calls/sec max

    def _rate_limit(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.time()

    def _rpc(self, method: str, params: list) -> Optional[dict]:
        self._rate_limit()
        try:
            r = requests.post(self.base_url, json={
                'jsonrpc': '2.0', 'id': 1,
                'method': method, 'params': params
            }, timeout=10)
            result = r.json()
            return result.get('result')
        except Exception as e:
            print(f"[Alchemy] RPC error: {e}")
            return None

    def get_balance(self, address: str) -> float:
        """Get ETH balance"""
        result = self._rpc('eth_getBalance', [address, 'latest'])
        if result:
            return int(result, 16) / 1e18
        return 0

    def get_code(self, address: str) -> str:
        """Check if address is contract"""
        result = self._rpc('eth_getCode', [address, 'latest'])
        return result or '0x'

    def get_tx_count(self, address: str) -> int:
        """Get transaction count"""
        result = self._rpc('eth_getTransactionCount', [address, 'latest'])
        if result:
            return int(result, 16)
        return 0

    def get_transfers(self, address: str, direction: str = 'from',
                      max_count: int = 100) -> List[dict]:
        """Get asset transfers"""
        self._rate_limit()
        try:
            params = {
                'category': ['external'],
                'maxCount': hex(max_count),
                'order': 'asc',
                'withMetadata': True,
            }
            if direction == 'from':
                params['fromAddress'] = address
            else:
                params['toAddress'] = address

            r = requests.post(self.base_url, json={
                'jsonrpc': '2.0', 'id': 1,
                'method': 'alchemy_getAssetTransfers',
                'params': [params]
            }, timeout=15)

            result = r.json()
            return result.get('result', {}).get('transfers', [])
        except:
            return []


def classify_wallet_type(is_contract: bool, tx_count: int,
                        active_days: int) -> str:
    """Classify as human/bot/contract"""
    if is_contract:
        return 'contract'

    # High activity = likely bot
    if tx_count > 5000 and active_days > 100:
        return 'bot'

    # Very high tx rate
    if active_days > 0 and tx_count / active_days > 50:
        return 'bot'

    return 'human'


def calculate_bot_likelihood(wallet_type: str, tx_count: int,
                            active_days: int, eth_balance: float) -> float:
    """Calculate bot likelihood 0-1"""
    score = 0.0

    if wallet_type == 'contract':
        score += 0.4

    # High tx rate
    if active_days > 0:
        tx_rate = tx_count / active_days
        if tx_rate > 100:
            score += 0.3
        elif tx_rate > 50:
            score += 0.2
        elif tx_rate > 20:
            score += 0.1

    # Large balance (bots often have capital)
    if eth_balance > 10:
        score += 0.15
    elif eth_balance > 2:
        score += 0.1

    # High total tx count
    if tx_count > 10000:
        score += 0.15
    elif tx_count > 5000:
        score += 0.1

    return min(score, 1.0)


def get_capital_tier(eth_balance: float, usdc_estimate: float = 0) -> str:
    """Classify capital tier"""
    total = eth_balance * 2000 + usdc_estimate  # Rough USD estimate

    if total > 500000 or eth_balance > 200:
        return 'whale'
    elif total > 50000 or eth_balance > 25:
        return 'large'
    elif total > 5000 or eth_balance > 2:
        return 'medium'
    else:
        return 'small'


def calculate_behavior_multiplier(wallet_type: str, funding_source: str,
                                  capital_tier: str, bot_likelihood: float) -> float:
    """
    Calculate behavior multiplier for toxicity score.

    > 1.0 = increase perceived toxicity
    < 1.0 = decrease perceived toxicity
    = 1.0 = no change
    """
    multiplier = 1.0

    # Bots are more dangerous
    if wallet_type == 'bot':
        multiplier *= 1.25
    elif wallet_type == 'contract':
        multiplier *= 1.3  # Smart contracts even more so

    # CEX funded = slightly more traceable, but also serious
    if funding_source in ('coinbase', 'binance', 'kraken'):
        multiplier *= 1.1  # Serious traders
    elif funding_source == 'bridge':
        multiplier *= 1.15  # Cross-chain activity

    # Whale = can move markets
    if capital_tier == 'whale':
        multiplier *= 1.15
    elif capital_tier == 'large':
        multiplier *= 1.05

    # Bot likelihood adjustment
    multiplier *= (1.0 + bot_likelihood * 0.2)

    return round(multiplier, 3)


def enrich_wallets(wallets: Set[str], api_key: str,
                   cache_path: str = None) -> Dict[str, dict]:
    """
    Enrich a set of wallets with Alchemy data.

    Returns: {wallet: enrichment_dict}
    """
    # Load cache
    cache = {}
    if cache_path and os.path.exists(cache_path):
        print(f"Loading cache from {cache_path}...")
        with open(cache_path, 'r') as f:
            cache = json.load(f)
        print(f"  {len(cache)} wallets cached")

    # Filter to wallets needing enrichment
    to_enrich = [w for w in wallets if w.lower() not in cache]
    print(f"\n{len(to_enrich)} wallets need enrichment")

    if not to_enrich:
        return cache

    if not api_key:
        print("No API key - returning cache only")
        return cache

    client = AlchemyEnricher(api_key)
    results = dict(cache)

    for i, wallet in enumerate(to_enrich):
        wallet = wallet.lower()

        try:
            # Get basic data
            eth_balance = client.get_balance(wallet)
            code = client.get_code(wallet)
            tx_count = client.get_tx_count(wallet)

            is_contract = code != '0x' and len(code) > 2

            # Get transfers for funding source and activity
            incoming = client.get_transfers(wallet, 'to', 50)
            outgoing = client.get_transfers(wallet, 'from', 50)

            # Calculate first seen and active days
            all_transfers = incoming + outgoing
            first_seen_ts = None
            active_dates = set()

            for tx in all_transfers:
                meta = tx.get('metadata', {})
                ts_str = meta.get('blockTimestamp', '')
                if ts_str:
                    try:
                        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        ts = int(dt.timestamp())
                        if first_seen_ts is None or ts < first_seen_ts:
                            first_seen_ts = ts
                        active_dates.add(dt.date().isoformat())
                    except:
                        pass

            active_days = len(active_dates)

            # Identify funding source
            funding_source = 'unknown'
            for tx in incoming[:20]:
                from_addr = tx.get('from', '').lower()
                if from_addr in CEX_WALLETS:
                    funding_source = CEX_WALLETS[from_addr]
                    break
                if from_addr in BRIDGE_CONTRACTS:
                    funding_source = 'bridge'
                    break

            # Classify
            wallet_type = classify_wallet_type(is_contract, tx_count, active_days)
            bot_likelihood = calculate_bot_likelihood(
                wallet_type, tx_count, active_days, eth_balance
            )
            capital_tier = get_capital_tier(eth_balance)
            behavior_mult = calculate_behavior_multiplier(
                wallet_type, funding_source, capital_tier, bot_likelihood
            )

            # Confidence
            confidence = 0.5
            if tx_count > 0:
                confidence += 0.1
            if first_seen_ts:
                confidence += 0.1
            if funding_source != 'unknown':
                confidence += 0.15
            if active_days > 7:
                confidence += 0.1
            confidence = min(confidence, 1.0)

            results[wallet] = {
                'wallet': wallet,
                'first_seen_ts': first_seen_ts,
                'tx_count': tx_count,
                'active_days': active_days,
                'eth_balance': round(eth_balance, 4),
                'funding_source': funding_source,
                'wallet_type': wallet_type,
                'bot_likelihood': round(bot_likelihood, 3),
                'capital_tier': capital_tier,
                'enrichment_confidence': round(confidence, 3),
                'behavior_multiplier': behavior_mult,
            }

        except Exception as e:
            print(f"  Error enriching {wallet[:12]}...: {e}")
            results[wallet] = {
                'wallet': wallet,
                'enrichment_confidence': 0,
                'behavior_multiplier': 1.0,
            }

        if (i + 1) % 50 == 0:
            print(f"  Enriched {i + 1}/{len(to_enrich)}")

            # Save cache periodically
            if cache_path:
                with open(cache_path, 'w') as f:
                    json.dump(results, f)

    # Final cache save
    if cache_path:
        with open(cache_path, 'w') as f:
            json.dump(results, f)
        print(f"Cache saved to {cache_path}")

    return results


def load_wallets_from_toxicity(path: str) -> Set[str]:
    """Load unique wallets from toxicity CSV"""
    wallets = set()
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = row.get('wallet', '').lower()
            if w:
                wallets.add(w)
    return wallets


def write_enrichment_csv(enrichments: Dict[str, dict], output_path: str):
    """Write enrichment results to CSV"""
    fieldnames = [
        'wallet', 'first_seen_ts', 'tx_count', 'active_days',
        'eth_balance', 'funding_source', 'wallet_type',
        'bot_likelihood', 'capital_tier', 'enrichment_confidence',
        'behavior_multiplier'
    ]

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for wallet, data in enrichments.items():
            row = {k: data.get(k, '') for k in fieldnames}
            writer.writerow(row)

    print(f"Written {len(enrichments)} enrichments to {output_path}")


def main():
    print("=" * 60)
    print("WALLET ENRICHMENT")
    print("=" * 60)

    api_key = os.environ.get('ALCHEMY_API_KEY', '')
    if not api_key:
        print("\nWarning: ALCHEMY_API_KEY not set")
        print("Will use cached data only")

    # Paths
    toxicity_path = os.path.join(DATA_DIR, 'wallet_toxicity_takers.csv')
    cache_path = os.path.join(DATA_DIR, 'wallet_enrichment_cache.json')
    output_path = os.path.join(DATA_DIR, 'wallet_enrichment.csv')

    if not os.path.exists(toxicity_path):
        print(f"\nError: {toxicity_path} not found")
        print("Run build_wallet_toxicity_v3.py first")
        return

    # Load wallets
    print(f"\nLoading wallets from {toxicity_path}...")
    wallets = load_wallets_from_toxicity(toxicity_path)
    print(f"Found {len(wallets):,} unique wallets")

    # Enrich
    enrichments = enrich_wallets(wallets, api_key, cache_path)

    # Write output
    write_enrichment_csv(enrichments, output_path)

    # Summary
    print("\n" + "=" * 60)
    print("ENRICHMENT SUMMARY")
    print("=" * 60)

    enriched = [e for e in enrichments.values() if e.get('enrichment_confidence', 0) > 0]

    if enriched:
        types = defaultdict(int)
        tiers = defaultdict(int)
        sources = defaultdict(int)

        for e in enriched:
            types[e.get('wallet_type', 'unknown')] += 1
            tiers[e.get('capital_tier', 'unknown')] += 1
            sources[e.get('funding_source', 'unknown')] += 1

        print(f"\nWallet types:")
        for t, c in sorted(types.items(), key=lambda x: -x[1]):
            print(f"  {t}: {c:,} ({c/len(enriched)*100:.1f}%)")

        print(f"\nCapital tiers:")
        for t, c in sorted(tiers.items(), key=lambda x: -x[1]):
            print(f"  {t}: {c:,} ({c/len(enriched)*100:.1f}%)")

        print(f"\nFunding sources:")
        for s, c in sorted(sources.items(), key=lambda x: -x[1]):
            print(f"  {s}: {c:,} ({c/len(enriched)*100:.1f}%)")

        # Top bots
        bots = sorted(enriched, key=lambda x: x.get('bot_likelihood', 0), reverse=True)[:10]
        print(f"\nTop 10 likely bots:")
        for b in bots:
            print(f"  {b['wallet'][:12]}... "
                  f"bot={b.get('bot_likelihood', 0):.2f} "
                  f"tx={b.get('tx_count', 0):,} "
                  f"mult={b.get('behavior_multiplier', 1):.2f}")

    print(f"\nOutput: {output_path}")


if __name__ == '__main__':
    main()
