#!/usr/bin/env python3
"""
Generate retail business intelligence report from Gold layer data.

Usage:
    python retail_report.py

Outputs:
    - Console report with business metrics
    - CSV files in reports/ directory
"""

import json
from pathlib import Path

import polars as pl

with open(Path(__file__).parent.parent / "config.json") as f:
    config = json.load(f)

AGGREGATED_DIR = Path(config["aggregated_dir"])
CLEANED_DIR = Path(config["cleaned_dir"])
OUTPUT_DIR = Path(config["reports_dir"])

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def print_summary_report():
    sales_file = AGGREGATED_DIR / "sales_by_region.parquet"
    if not sales_file.exists():
        print("No data found. Run the pipeline first.")
        return

    sales_by_region = pl.read_parquet(AGGREGATED_DIR / "sales_by_region.parquet")
    top_categories = pl.read_parquet(AGGREGATED_DIR / "top_categories.parquet")
    daily_sales = pl.read_parquet(AGGREGATED_DIR / "sales_daily.parquet")
    transactions = pl.read_parquet(CLEANED_DIR / "transactions_enriched.parquet")

    print("=" * 80)
    print("RETAIL BUSINESS INTELLIGENCE REPORT")
    print("=" * 80)
    print()

    total_sales = sales_by_region["total_sales"].sum()
    total_transactions = sales_by_region["num_transactions"].sum()
    avg_transaction = total_sales / total_transactions
    num_regions = len(sales_by_region)
    num_stores = sales_by_region["num_stores"].sum()

    print("OVERALL METRICS")
    print(f"Total Sales:         €{total_sales:,.2f}")
    print(f"Total Transactions:  {total_transactions:,}")
    print(f"Avg Transaction:     €{avg_transaction:.2f}")
    print(f"Regions:             {num_regions}")
    print(f"Stores:              {num_stores}")
    print()

    print("TOP 5 REGIONS")
    top_regions = sales_by_region.sort("total_sales", descending=True).head(5)
    for row in top_regions.iter_rows(named=True):
        pct = (row["total_sales"] / total_sales) * 100
        print(f"{row['region']:20s} €{row['total_sales']:>12,.2f} ({pct:>5.1f}%)")
    print()

    print("TOP 5 CATEGORIES")
    for row in top_categories.head(5).iter_rows(named=True):
        print(f"{row['category']:25s} €{row['total_sales']:>12,.2f}")
    print()

    best_day = daily_sales.sort("total_sales", descending=True).head(1)
    worst_day = daily_sales.sort("total_sales").head(1)
    avg_daily = daily_sales["total_sales"].mean()

    print("SALES TRENDS")
    for row in best_day.iter_rows(named=True):
        print(f"Best Day:   {row['date']}  €{row['total_sales']:,.2f}")
    for row in worst_day.iter_rows(named=True):
        print(f"Worst Day:  {row['date']}  €{row['total_sales']:,.2f}")
    print(f"Avg Daily:               €{avg_daily:,.2f}")
    print()

    hourly_sales = (
        transactions.group_by("hour")
        .agg(pl.sum("total_amount").alias("sales"))
        .sort("sales", descending=True)
    )

    print("PEAK HOURS")
    for row in hourly_sales.head(5).iter_rows(named=True):
        hour = row["hour"]
        print(f"{hour:02d}:00-{hour+1:02d}:00  €{row['sales']:,.2f}")
    print()

    avg_discount = transactions["discount_percent"].mean()
    total_discount = transactions["discount_amount"].sum()
    pct_with_discount = (
        transactions.filter(pl.col("discount_percent") > 0).height / transactions.height
    ) * 100

    print("DISCOUNTS")
    print(f"Avg Discount:      {avg_discount*100:.1f}%")
    print(f"Total Discounted:  €{total_discount:,.2f}")
    print(f"% with Discount:   {pct_with_discount:.1f}%")
    print()

    print("=" * 80)


def export_csv():
    if not (AGGREGATED_DIR / "sales_by_region.parquet").exists():
        return

    files = [
        ("sales_by_region.parquet", "sales_by_region.csv"),
        ("top_categories.parquet", "top_categories.csv"),
        ("sales_daily.parquet", "sales_daily.csv"),
    ]

    for parquet_file, csv_file in files:
        df = pl.read_parquet(AGGREGATED_DIR / parquet_file)
        df.write_csv(OUTPUT_DIR / csv_file)


if __name__ == "__main__":
    print_summary_report()
    export_csv()
