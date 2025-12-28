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
        description="Generate synthetic retail data"
    )
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

    args = parser.parse_args()

    config = DataGenConfig(
        seed=args.seed,
        num_stores=args.stores,
        num_products=args.products,
        num_transactions=args.transactions,
        data_dir=args.output_dir,
    )

    config.ensure_directories()

    store_gen = StoreGenerator(config)
    stores_df = store_gen.generate(num_stores=config.num_stores)
    store_gen.save(stores_df)

    product_gen = ProductGenerator(config)
    products_df = product_gen.generate(num_products=config.num_products)
    product_gen.save(products_df)

    transaction_gen = TransactionGenerator(config)

    from datetime import datetime
    start_date = datetime(2020, 1, 1, 0, 0, 0)
    end_date = datetime.now()

    transactions_df = transaction_gen.generate(
        num_transactions=config.num_transactions,
        store_ids=stores_df["store_id"].to_list(),
        product_ids=products_df["product_id"].to_list(),
        start_date=start_date,
        end_date=end_date,
    )
    transaction_gen.save(transactions_df)


if __name__ == "__main__":
    main()
