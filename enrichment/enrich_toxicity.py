"""
Enrich Wallet Toxicity with Chain Data

Takes wallet_toxicity_takers.csv and adds Alchemy-derived modifiers:
- Wallet age penalty (fresh wallets = higher risk)
- Capital awareness
- CEX funding flags
- Bot detection

Output: wallet_toxicity_enriched.csv

New columns:
- wallet_age_days
- is_fresh_wallet
- is_high_capital
- is_cex_funded
- is_likely_bot
- behavior_multiplier
- enriched_toxicity_score (= toxicity_score × behavior_multiplier)

Usage:
    export ALCHEMY_API_KEY=your_key
    python enrichment/enrich_toxicity.py
"""

import csv
import os
import sys
from typing import Dict, Set

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from enrichment.alchemy_client import AlchemyClient, WalletProfile


DATA_DIR = os.environ.get('DATA_DIR', './data')


def load_unique_wallets(toxicity_path: str) -> Set[str]:
    """Extract unique wallet addresses from toxicity CSV"""
    wallets = set()

    with open(toxicity_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallet = row.get('wallet', '').lower()
            if wallet:
                wallets.add(wallet)

    return wallets


def calculate_behavior_multiplier(profile: WalletProfile) -> float:
    """
    Calculate multiplier for toxicity score based on chain behavior.

    Multiplier > 1.0 = increase perceived toxicity
    Multiplier < 1.0 = decrease perceived toxicity
    Multiplier = 1.0 = no change

    Risk factors (increase multiplier):
    - Fresh wallet (< 7 days)
    - Bot-like behavior
    - High capital (can move markets)

    Trust factors (decrease multiplier):
    - CEX funded (traceable)
    - Old wallet with history
    - Low activity (probably retail)
    """
    multiplier = 1.0

    # Fresh wallet = higher risk (unknown actor)
    if profile.is_fresh_wallet:
        multiplier *= 1.3

    # Bot = higher risk (systematic, fast)
    if profile.is_likely_bot:
        multiplier *= 1.4

    # High capital = higher risk (can move markets)
    if profile.is_high_capital:
        multiplier *= 1.2

    # CEX funded = slightly lower risk (traceable)
    if profile.is_cex_funded:
        multiplier *= 0.9

    # Old wallet with history = lower risk
    if profile.wallet_age_days > 180:
        multiplier *= 0.85

    # Very low activity = probably retail noise
    if profile.tx_count < 20 and not profile.is_fresh_wallet:
        multiplier *= 0.8

    # Confidence adjustment
    # Low confidence = pull toward 1.0
    multiplier = 1.0 + (multiplier - 1.0) * profile.confidence

    return round(multiplier, 3)


def enrich_toxicity_file(input_path: str, output_path: str,
                         profiles: Dict[str, WalletProfile]):
    """
    Read toxicity CSV, add enrichment columns, write new CSV.
    """
    print(f"Enriching {input_path}...")

    with open(input_path, 'r') as infile, \
         open(output_path, 'w', newline='') as outfile:

        reader = csv.DictReader(infile)

        # Add new columns
        fieldnames = reader.fieldnames + [
            'wallet_age_days',
            'is_fresh_wallet',
            'is_high_capital',
            'is_cex_funded',
            'is_likely_bot',
            'behavior_multiplier',
            'enriched_toxicity_score',
        ]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        enriched_count = 0
        total_count = 0

        for row in reader:
            total_count += 1
            wallet = row.get('wallet', '').lower()

            # Get profile if available
            profile = profiles.get(wallet)

            if profile:
                enriched_count += 1
                multiplier = calculate_behavior_multiplier(profile)
                original_toxicity = float(row.get('toxicity_score', 0) or 0)

                row['wallet_age_days'] = round(profile.wallet_age_days, 1)
                row['is_fresh_wallet'] = profile.is_fresh_wallet
                row['is_high_capital'] = profile.is_high_capital
                row['is_cex_funded'] = profile.is_cex_funded
                row['is_likely_bot'] = profile.is_likely_bot
                row['behavior_multiplier'] = multiplier
                row['enriched_toxicity_score'] = round(original_toxicity * multiplier, 4)
            else:
                # No enrichment data - use defaults
                row['wallet_age_days'] = ''
                row['is_fresh_wallet'] = ''
                row['is_high_capital'] = ''
                row['is_cex_funded'] = ''
                row['is_likely_bot'] = ''
                row['behavior_multiplier'] = 1.0
                row['enriched_toxicity_score'] = row.get('toxicity_score', 0)

            writer.writerow(row)

    print(f"Enriched {enriched_count}/{total_count} rows")
    print(f"Output written to {output_path}")


def main():
    print("=" * 60)
    print("WALLET TOXICITY ENRICHMENT")
    print("=" * 60)

    # Check for API key
    api_key = os.environ.get('ALCHEMY_API_KEY', '')
    if not api_key:
        print("\nWarning: ALCHEMY_API_KEY not set")
        print("Set it with: export ALCHEMY_API_KEY=your_key")
        print("Continuing with cached profiles only...")

    # Paths
    toxicity_path = os.path.join(DATA_DIR, 'wallet_toxicity_takers.csv')
    profiles_cache_path = os.path.join(DATA_DIR, 'wallet_profiles.json')
    output_path = os.path.join(DATA_DIR, 'wallet_toxicity_enriched.csv')

    if not os.path.exists(toxicity_path):
        print(f"Error: {toxicity_path} not found")
        print("Run build_wallet_toxicity_v3.py first")
        return

    # Load unique wallets
    print(f"\nLoading wallets from {toxicity_path}...")
    wallets = load_unique_wallets(toxicity_path)
    print(f"Found {len(wallets):,} unique wallets")

    # Initialize client
    client = AlchemyClient(api_key=api_key)

    # Try to load cached profiles
    profiles = {}
    if os.path.exists(profiles_cache_path):
        print(f"\nLoading cached profiles from {profiles_cache_path}...")
        profiles = client.load_profiles(profiles_cache_path)

    # Find wallets needing enrichment
    wallets_to_enrich = wallets - set(profiles.keys())

    if wallets_to_enrich and api_key:
        print(f"\n{len(wallets_to_enrich):,} wallets need enrichment")

        # Limit to avoid long runs (can be adjusted)
        max_to_enrich = 1000
        if len(wallets_to_enrich) > max_to_enrich:
            print(f"Limiting to {max_to_enrich} wallets (run again to continue)")
            wallets_to_enrich = set(list(wallets_to_enrich)[:max_to_enrich])

        print(f"Enriching {len(wallets_to_enrich)} wallets...")
        new_profiles = client.enrich_wallets(list(wallets_to_enrich))
        profiles.update(new_profiles)

        # Save updated cache
        print(f"\nSaving profiles to {profiles_cache_path}...")
        client.export_profiles(profiles, profiles_cache_path)

    elif wallets_to_enrich:
        print(f"\n{len(wallets_to_enrich):,} wallets need enrichment but no API key set")

    # Enrich the toxicity file
    print(f"\nEnriching toxicity data...")
    enrich_toxicity_file(toxicity_path, output_path, profiles)

    # Summary stats
    print("\n" + "=" * 60)
    print("ENRICHMENT SUMMARY")
    print("=" * 60)

    enriched_profiles = [p for p in profiles.values() if p.address in wallets]

    if enriched_profiles:
        fresh_count = sum(1 for p in enriched_profiles if p.is_fresh_wallet)
        bot_count = sum(1 for p in enriched_profiles if p.is_likely_bot)
        high_cap_count = sum(1 for p in enriched_profiles if p.is_high_capital)
        cex_count = sum(1 for p in enriched_profiles if p.is_cex_funded)

        print(f"\nOf {len(enriched_profiles):,} enriched wallets:")
        print(f"  Fresh wallets (<7d): {fresh_count:,} ({fresh_count/len(enriched_profiles)*100:.1f}%)")
        print(f"  Likely bots: {bot_count:,} ({bot_count/len(enriched_profiles)*100:.1f}%)")
        print(f"  High capital: {high_cap_count:,} ({high_cap_count/len(enriched_profiles)*100:.1f}%)")
        print(f"  CEX funded: {cex_count:,} ({cex_count/len(enriched_profiles)*100:.1f}%)")

    print(f"\nOutput: {output_path}")


if __name__ == '__main__':
    main()
