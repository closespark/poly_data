"""
Wallet Enrichment via Alchemy

Provides chain-level intelligence for wallets:
- Offline enrichment (wallet age, funding sources, bot detection)
- Async pending transaction observation

NOT for hot path - enrichment and observation only.

Usage:
    # Offline enrichment
    from enrichment import AlchemyClient
    client = AlchemyClient(api_key='...')
    profile = client.get_wallet_profile('0x...')

    # Live observation
    from enrichment import PendingObserver
    observer = PendingObserver(toxic_wallets={'0x...'})
    observer.start()
"""

from enrichment.alchemy_client import AlchemyClient, WalletProfile
from enrichment.pending_observer import PendingObserver, PendingTxEvent

__all__ = [
    'AlchemyClient',
    'WalletProfile',
    'PendingObserver',
    'PendingTxEvent',
]
