"""
Alchemy Client for Wallet Intelligence

Provides chain-level wallet enrichment:
- Wallet age / activity level
- Capital proxy (balance)
- Funding source classification
- Behavioral tags (bot, CEX mirror, bridge arb, etc.)

NOT for hot path - use for offline enrichment and async observation only.

Usage:
    from enrichment.alchemy_client import AlchemyClient

    client = AlchemyClient(api_key='your_key')

    # Single wallet
    profile = client.get_wallet_profile('0x...')

    # Batch enrichment
    profiles = client.enrich_wallets(['0x...', '0x...'])
"""

import os
import time
import requests
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
import json


# Known addresses for classification
KNOWN_CEX_HOT_WALLETS = {
    # Coinbase
    '0x71660c4005ba85c37ccec55d0c4493e66fe775d3': 'coinbase',
    '0x503828976d22510aad0201ac7ec88293211d23da': 'coinbase',
    '0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740': 'coinbase',
    # Binance
    '0x28c6c06298d514db089934071355e5743bf21d60': 'binance',
    '0x21a31ee1afc51d94c2efccaa2092ad1028285549': 'binance',
    # Kraken
    '0x2910543af39aba0cd09dbb2d50200b3e800a63d2': 'kraken',
    # Add more as needed
}

KNOWN_BRIDGE_CONTRACTS = {
    '0x88ad09518695c6c3712ac10a214be5109a655671': 'polygon_bridge',
    # Add more as needed
}

POLYMARKET_CONTRACTS = {
    '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e': 'ctf_exchange',
    '0x4d97dcd97ec945f40cf65f87097ace5ea0476045': 'neg_risk_ctf_exchange',
    # Add more as needed
}


@dataclass
class WalletProfile:
    """Enriched wallet profile from chain data"""
    address: str

    # Activity metrics
    tx_count: int = 0
    first_tx_timestamp: Optional[int] = None
    wallet_age_days: float = 0

    # Capital
    eth_balance: float = 0
    usdc_balance: float = 0

    # Classification
    tags: Set[str] = field(default_factory=set)
    funding_sources: Set[str] = field(default_factory=set)

    # Polymarket specific
    polymarket_tx_count: int = 0
    is_active_trader: bool = False

    # Risk flags
    is_fresh_wallet: bool = False  # < 7 days old
    is_high_capital: bool = False  # > $10k
    is_likely_bot: bool = False
    is_cex_funded: bool = False

    # Metadata
    enriched_at: float = 0
    confidence: float = 0


class AlchemyClient:
    """
    Alchemy API client for wallet intelligence.

    Rate limiting: Alchemy free tier = 330 CU/s
    Most calls are 10-25 CU, so ~15-30 calls/second is safe.
    """

    def __init__(self, api_key: str = None, network: str = 'polygon-mainnet'):
        self.api_key = api_key or os.environ.get('ALCHEMY_API_KEY', '')
        self.network = network
        self.base_url = f'https://{network}.g.alchemy.com/v2/{self.api_key}'

        # Rate limiting
        self.calls_per_second = 15
        self.last_call_time = 0

        # Cache
        self.profile_cache: Dict[str, WalletProfile] = {}
        self.cache_ttl = 3600  # 1 hour

    def _rate_limit(self):
        """Simple rate limiting"""
        now = time.time()
        min_interval = 1.0 / self.calls_per_second
        elapsed = now - self.last_call_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_call_time = time.time()

    def _rpc_call(self, method: str, params: list) -> Optional[dict]:
        """Make JSON-RPC call to Alchemy"""
        self._rate_limit()

        try:
            response = requests.post(
                self.base_url,
                json={
                    'jsonrpc': '2.0',
                    'id': 1,
                    'method': method,
                    'params': params
                },
                timeout=10
            )
            response.raise_for_status()
            result = response.json()

            if 'error' in result:
                print(f"[Alchemy] RPC error: {result['error']}")
                return None

            return result.get('result')

        except Exception as e:
            print(f"[Alchemy] Request failed: {e}")
            return None

    def get_transaction_count(self, address: str) -> int:
        """Get total transaction count for address"""
        result = self._rpc_call('eth_getTransactionCount', [address, 'latest'])
        if result:
            return int(result, 16)
        return 0

    def get_balance(self, address: str) -> float:
        """Get ETH balance in ETH (not wei)"""
        result = self._rpc_call('eth_getBalance', [address, 'latest'])
        if result:
            wei = int(result, 16)
            return wei / 1e18
        return 0

    def get_token_balance(self, address: str, token_contract: str) -> float:
        """
        Get ERC20 token balance.
        Uses eth_call with balanceOf(address).
        """
        # balanceOf function selector + padded address
        data = '0x70a08231' + address[2:].zfill(64)

        result = self._rpc_call('eth_call', [
            {'to': token_contract, 'data': data},
            'latest'
        ])

        if result and result != '0x':
            return int(result, 16) / 1e6  # Assuming 6 decimals (USDC)
        return 0

    def get_asset_transfers(self, address: str,
                           category: str = 'external',
                           max_count: int = 100) -> List[dict]:
        """
        Get asset transfers to/from address.
        Uses Alchemy's enhanced API.
        """
        self._rate_limit()

        try:
            response = requests.post(
                self.base_url,
                json={
                    'jsonrpc': '2.0',
                    'id': 1,
                    'method': 'alchemy_getAssetTransfers',
                    'params': [{
                        'fromAddress': address,
                        'category': [category],
                        'maxCount': hex(max_count),
                        'order': 'asc'  # Oldest first
                    }]
                },
                timeout=15
            )
            response.raise_for_status()
            result = response.json()

            if 'error' in result:
                return []

            return result.get('result', {}).get('transfers', [])

        except Exception as e:
            print(f"[Alchemy] Asset transfers failed: {e}")
            return []

    def get_wallet_profile(self, address: str,
                          use_cache: bool = True) -> WalletProfile:
        """
        Build comprehensive wallet profile.

        This is the main enrichment function.
        """
        address = address.lower()

        # Check cache
        if use_cache and address in self.profile_cache:
            cached = self.profile_cache[address]
            if time.time() - cached.enriched_at < self.cache_ttl:
                return cached

        profile = WalletProfile(address=address)

        # Basic metrics
        profile.tx_count = self.get_transaction_count(address)
        profile.eth_balance = self.get_balance(address)

        # USDC balance (Polygon USDC)
        usdc_polygon = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174'
        profile.usdc_balance = self.get_token_balance(address, usdc_polygon)

        # Get first transactions for age calculation
        transfers = self.get_asset_transfers(address, max_count=10)
        if transfers:
            first_tx = transfers[0]
            if 'metadata' in first_tx and 'blockTimestamp' in first_tx['metadata']:
                ts_str = first_tx['metadata']['blockTimestamp']
                try:
                    first_dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                    profile.first_tx_timestamp = int(first_dt.timestamp())
                    profile.wallet_age_days = (time.time() - profile.first_tx_timestamp) / 86400
                except:
                    pass

        # Classify funding sources
        self._classify_funding_sources(profile, transfers)

        # Set flags
        profile.is_fresh_wallet = profile.wallet_age_days < 7
        profile.is_high_capital = (profile.usdc_balance > 10000 or
                                   profile.eth_balance > 5)
        profile.is_cex_funded = len(profile.funding_sources & set(KNOWN_CEX_HOT_WALLETS.values())) > 0

        # Bot detection heuristics
        profile.is_likely_bot = self._detect_bot(profile)

        # Activity classification
        profile.is_active_trader = profile.tx_count > 100

        # Confidence (based on data quality)
        profile.confidence = self._calculate_confidence(profile)
        profile.enriched_at = time.time()

        # Cache
        self.profile_cache[address] = profile

        return profile

    def _classify_funding_sources(self, profile: WalletProfile,
                                  transfers: List[dict]):
        """Identify where funds came from"""
        for tx in transfers:
            from_addr = tx.get('from', '').lower()

            # Check known CEX wallets
            if from_addr in KNOWN_CEX_HOT_WALLETS:
                profile.funding_sources.add(KNOWN_CEX_HOT_WALLETS[from_addr])
                profile.tags.add('cex_funded')

            # Check bridges
            if from_addr in KNOWN_BRIDGE_CONTRACTS:
                profile.funding_sources.add(KNOWN_BRIDGE_CONTRACTS[from_addr])
                profile.tags.add('bridge_user')

            # Check Polymarket contracts
            if from_addr in POLYMARKET_CONTRACTS:
                profile.polymarket_tx_count += 1
                profile.tags.add('polymarket_user')

    def _detect_bot(self, profile: WalletProfile) -> bool:
        """
        Heuristic bot detection.

        Bots typically have:
        - High tx count relative to age
        - Consistent timing patterns (we'd need more data)
        - Contract interactions only
        """
        if profile.wallet_age_days == 0:
            return False

        tx_per_day = profile.tx_count / max(profile.wallet_age_days, 1)

        # More than 50 tx/day is suspicious
        if tx_per_day > 50:
            profile.tags.add('high_frequency')
            return True

        # Fresh wallet with many tx
        if profile.is_fresh_wallet and profile.tx_count > 100:
            profile.tags.add('suspicious_activity')
            return True

        return False

    def _calculate_confidence(self, profile: WalletProfile) -> float:
        """
        Calculate confidence in classification.
        Based on data completeness.
        """
        score = 0.5  # Base

        if profile.tx_count > 0:
            score += 0.1
        if profile.first_tx_timestamp:
            score += 0.1
        if profile.funding_sources:
            score += 0.15
        if profile.wallet_age_days > 30:
            score += 0.1
        if profile.usdc_balance > 0 or profile.eth_balance > 0:
            score += 0.05

        return min(score, 1.0)

    def enrich_wallets(self, addresses: List[str],
                       progress_callback=None) -> Dict[str, WalletProfile]:
        """
        Batch enrich multiple wallets.

        Args:
            addresses: List of wallet addresses
            progress_callback: Optional callback(current, total)

        Returns:
            {address: WalletProfile}
        """
        results = {}
        total = len(addresses)

        for i, addr in enumerate(addresses):
            try:
                profile = self.get_wallet_profile(addr)
                results[addr.lower()] = profile
            except Exception as e:
                print(f"[Alchemy] Failed to enrich {addr[:12]}...: {e}")

            if progress_callback:
                progress_callback(i + 1, total)

            # Progress logging
            if (i + 1) % 100 == 0:
                print(f"[Alchemy] Enriched {i + 1}/{total} wallets")

        return results

    def export_profiles(self, profiles: Dict[str, WalletProfile],
                        filepath: str):
        """Export profiles to JSON"""
        data = {}
        for addr, profile in profiles.items():
            data[addr] = {
                'address': profile.address,
                'tx_count': profile.tx_count,
                'wallet_age_days': profile.wallet_age_days,
                'eth_balance': profile.eth_balance,
                'usdc_balance': profile.usdc_balance,
                'tags': list(profile.tags),
                'funding_sources': list(profile.funding_sources),
                'is_fresh_wallet': profile.is_fresh_wallet,
                'is_high_capital': profile.is_high_capital,
                'is_likely_bot': profile.is_likely_bot,
                'is_cex_funded': profile.is_cex_funded,
                'confidence': profile.confidence,
                'enriched_at': profile.enriched_at,
            }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"[Alchemy] Exported {len(data)} profiles to {filepath}")

    def load_profiles(self, filepath: str) -> Dict[str, WalletProfile]:
        """Load profiles from JSON"""
        with open(filepath, 'r') as f:
            data = json.load(f)

        profiles = {}
        for addr, d in data.items():
            profile = WalletProfile(
                address=d['address'],
                tx_count=d['tx_count'],
                wallet_age_days=d['wallet_age_days'],
                eth_balance=d['eth_balance'],
                usdc_balance=d['usdc_balance'],
                tags=set(d['tags']),
                funding_sources=set(d['funding_sources']),
                is_fresh_wallet=d['is_fresh_wallet'],
                is_high_capital=d['is_high_capital'],
                is_likely_bot=d['is_likely_bot'],
                is_cex_funded=d['is_cex_funded'],
                confidence=d['confidence'],
                enriched_at=d['enriched_at'],
            )
            profiles[addr] = profile

        print(f"[Alchemy] Loaded {len(profiles)} profiles from {filepath}")
        return profiles
