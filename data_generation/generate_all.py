#!/usr/bin/env python3
"""
Generate all synthetic data for the retail lakehouse.

Usage:
    python generate_all.py --stores 500 --products 50000 --transactions 10000000 --seed 42 --output-dir ../data
"""

import argparse
from datetime import datetime
from pathlib import Path

from config import DataGenConfig
from generators import ProductGenerator, StoreGenerator, TransactionGenerator


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic retail data with configurable parameters"
    )
    # Required parameters
    parser.add_argument(
        "--stores",
        type=int,
        required=True,
        help="Number of stores to generate",
    )
    parser.add_argument(
        "--products",
        type=int,
        required=True,
        help="Number of products to generate",
    )
    parser.add_argument(
        "--transactions",
        type=int,
        required=True,
        help="Number of transactions to generate",
    )
    parser.add_argument(
        "--seed",
        type=int,
        required=True,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for generated data",
    )

    # Optional control parameters
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for transactions (YYYY-MM-DD). Default: 2020-01-01",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for transactions (YYYY-MM-DD). Default: today",
    )
    parser.add_argument(
        "--yoy-growth",
        type=float,
        default=None,
        help="Year-over-year growth rate (e.g., 0.15 for 15%%). Default: 0.15",
    )
    parser.add_argument(
        "--no-seasonality",
        action="store_true",
        help="Disable seasonal patterns (uniform distribution)",
    )
    parser.add_argument(
        "--no-holidays",
        action="store_true",
        help="Disable holiday effects (no reduced traffic on holidays)",
    )
    parser.add_argument(
        "--avg-items",
        type=int,
        default=None,
        help="Average items per transaction. Default: 3",
    )
    parser.add_argument(
        "--peak-multiplier",
        type=float,
        default=None,
        help="Peak hour traffic multiplier. Default: 1.0 (no change)",
    )

    args = parser.parse_args()

    config = DataGenConfig(
        seed=args.seed,
        num_stores=args.stores,
        num_products=args.products,
        num_transactions=args.transactions,
        data_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        yoy_growth=args.yoy_growth,
        enable_seasonality=not args.no_seasonality,
        enable_holidays=not args.no_holidays,
        avg_items_per_transaction=args.avg_items,
        peak_hour_multiplier=args.peak_multiplier,
    )

    config.ensure_directories()

    print("Generating stores...")
    store_gen = StoreGenerator(config)
    stores_df = store_gen.generate(num_stores=config.num_stores)
    store_gen.save(stores_df)
    print(f"  ✓ Generated {len(stores_df)} stores")

    print("Generating products...")
    product_gen = ProductGenerator(config)
    products_df = product_gen.generate(num_products=config.num_products)
    product_gen.save(products_df)
    print(f"  ✓ Generated {len(products_df)} products")

    transaction_gen = TransactionGenerator(config)

    transactions_df = transaction_gen.generate(
        num_transactions=config.num_transactions,
        store_ids=stores_df["store_id"].to_list(),
        product_ids=products_df["product_id"].to_list(),
    )
    transaction_gen.save(transactions_df)
    print(f"\nAll data generated successfully in: {config.landing_dir}")


if __name__ == "__main__":
    main()
