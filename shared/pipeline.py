"""
Medallion architecture pipeline (Bronze → Silver → Gold).

This module contains the core data transformation logic that is reused
by both Airflow and Prefect orchestrators.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MedallionPipeline:

    def __init__(self, data_dir: Optional[Path] = None, config_path: Optional[Path] = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config.json"

        with open(config_path) as f:
            config = json.load(f)

        self.data_dir = Path(config["data_dir"]) if data_dir is None else Path(data_dir)
        self.landing_dir = Path(config["landing_dir"])
        self.raw_dir = Path(config["raw_dir"])
        self.cleaned_dir = Path(config["cleaned_dir"])
        self.aggregated_dir = Path(config["aggregated_dir"])
        self.compression = config["compression"]
        self.compression_level = config["compression_level"]

        for dir_path in [self.raw_dir, self.cleaned_dir, self.aggregated_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

    def bronze_ingest_stores(self) -> int:
        logger.info("Bronze: Ingesting stores")
        df = pl.read_parquet(self.landing_dir / "stores.parquet")

        output_path = self.raw_dir / "stores.parquet"
        df.write_parquet(output_path, compression="zstd")

        logger.info(f"Bronze: Ingested {len(df)} stores to {output_path}")
        return len(df)

    def bronze_ingest_products(self) -> int:
        logger.info("Bronze: Ingesting products")
        df = pl.read_parquet(self.landing_dir / "products.parquet")

        output_path = self.raw_dir / "products.parquet"
        df.write_parquet(output_path, compression="zstd")

        logger.info(f"Bronze: Ingested {len(df)} products to {output_path}")
        return len(df)

    def bronze_ingest_transactions(self) -> int:
        logger.info("Bronze: Ingesting transactions")

        output_path = self.raw_dir / "transactions.parquet"

        pl.scan_parquet(self.landing_dir / "transactions.parquet").sink_parquet(
            output_path, compression="zstd"
        )

        count = pl.scan_parquet(output_path).select(pl.len()).collect().item()
        logger.info(f"Bronze: Ingested {count:,} transactions to {output_path}")
        return count

    def silver_clean_stores(self) -> int:
        logger.info("Silver: Cleaning stores")
        df = pl.read_parquet(self.raw_dir / "stores.parquet")

        # Remove duplicates
        df = df.unique(subset=["store_id"])

        # Validate geographic coordinates
        df = df.with_columns(
            [
                pl.when(
                    (pl.col("latitude").is_between(47.0, 55.0))
                    & (pl.col("longitude").is_between(5.0, 15.0))
                )
                .then(True)
                .otherwise(False)
                .alias("valid_coordinates")
            ]
        )

        # Add quality score
        df = df.with_columns(
            [
                pl.when(pl.col("valid_coordinates"))
                .then(1.0)
                .otherwise(0.5)
                .alias("data_quality_score")
            ]
        )

        output_path = self.cleaned_dir / "stores_cleaned.parquet"
        df.write_parquet(output_path, compression="zstd")

        logger.info(f"Silver: Cleaned {len(df)} stores to {output_path}")
        return len(df)

    def silver_clean_products(self) -> int:
        logger.info("Silver: Cleaning products")
        df = pl.read_parquet(self.raw_dir / "products.parquet")

        # Remove duplicates
        df = df.unique(subset=["product_id"])

        # Filter active products only
        df = df.filter(pl.col("is_active") == True)

        # Validate EAN length
        df = df.with_columns([(pl.col("ean").str.len_bytes() == 13).alias("valid_ean")])

        # Ensure prices are positive
        df = df.filter(pl.col("unit_price") > 0)

        # Add calculated fields
        df = df.with_columns(
            [
                (pl.col("unit_price") * (1 + pl.col("vat_rate")))
                .round(2)
                .alias("price_incl_vat")
            ]
        )

        output_path = self.cleaned_dir / "products_cleaned.parquet"
        df.write_parquet(output_path, compression="zstd")

        logger.info(f"Silver: Cleaned {len(df)} products to {output_path}")
        return len(df)

    def silver_enrich_transactions(self) -> int:
        logger.info("Silver: Enriching transactions")

        output_path = self.cleaned_dir / "transactions_enriched.parquet"

        transactions = pl.scan_parquet(self.raw_dir / "transactions.parquet")
        stores = pl.scan_parquet(self.cleaned_dir / "stores_cleaned.parquet")
        products = pl.scan_parquet(self.cleaned_dir / "products_cleaned.parquet")

        df = (
            transactions.join(
                stores.select(["store_id", "region", "city"]), on="store_id", how="inner"
            )
            .join(
                products.select(
                    ["product_id", "category", "unit_price", "price_incl_vat"]
                ),
                on="product_id",
                how="inner",
            )
            .with_columns(
                [
                    (pl.col("quantity") * pl.col("price_incl_vat")).alias("subtotal"),
                ]
            )
            .with_columns(
                [
                    (pl.col("subtotal") * pl.col("discount_percent")).alias(
                        "discount_amount"
                    ),
                ]
            )
            .with_columns(
                [
                    (pl.col("subtotal") - pl.col("discount_amount"))
                    .round(2)
                    .alias("total_amount")
                ]
            )
            .with_columns(
                [
                    pl.col("transaction_datetime").dt.year().alias("year"),
                    pl.col("transaction_datetime").dt.month().alias("month"),
                    pl.col("transaction_datetime").dt.weekday().alias("weekday"),
                    pl.col("transaction_datetime").dt.hour().alias("hour"),
                ]
            )
        )

        df.sink_parquet(output_path, compression="zstd")

        count = pl.scan_parquet(output_path).select(pl.len()).collect().item()
        logger.info(f"Silver: Enriched {count:,} transactions to {output_path}")
        return count

    def gold_yearly_sales(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating yearly sales")

        output_path = self.aggregated_dir / "sales_yearly.parquet"

        yearly_sales = (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .group_by("year")
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                    pl.mean("total_amount").alias("avg_transaction_value"),
                    pl.n_unique("store_id").alias("num_stores"),
                    pl.n_unique("category").alias("num_categories"),
                ]
            )
            .sort("year")
            .collect()
        )

        yearly_sales.write_parquet(output_path, compression=self.compression)

        logger.info(f"Gold: Yearly sales saved to {output_path}")

        return {
            "num_years": len(yearly_sales),
            "total_sales": float(yearly_sales["total_sales"].sum()),
        }

    def gold_monthly_sales(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating monthly sales")

        output_path = self.aggregated_dir / "sales_monthly.parquet"

        monthly_sales = (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns(
                [pl.col("transaction_datetime").dt.strftime("%Y-%m").alias("year_month")]
            )
            .group_by(["year", "month", "year_month"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                    pl.mean("total_amount").alias("avg_transaction_value"),
                ]
            )
            .sort(["year", "month"])
            .collect()
        )

        monthly_sales.write_parquet(output_path, compression=self.compression)

        logger.info(f"Gold: Monthly sales saved to {output_path}")

        return {
            "num_months": len(monthly_sales),
            "total_sales": float(monthly_sales["total_sales"].sum()),
        }

    def gold_weekly_sales(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating weekly sales")

        output_path = self.aggregated_dir / "sales_weekly.parquet"

        weekly_sales = (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns(
                [
                    pl.col("transaction_datetime").dt.year().alias("iso_year"),
                    pl.col("transaction_datetime").dt.week().alias("iso_week"),
                ]
            )
            .group_by(["iso_year", "iso_week"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                    pl.mean("total_amount").alias("avg_transaction_value"),
                ]
            )
            .sort(["iso_year", "iso_week"])
            .collect()
        )

        weekly_sales.write_parquet(output_path, compression=self.compression)

        logger.info(f"Gold: Weekly sales saved to {output_path}")

        return {
            "num_weeks": len(weekly_sales),
            "total_sales": float(weekly_sales["total_sales"].sum()),
        }

    def gold_daily_sales(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating daily sales")

        output_path = self.aggregated_dir / "sales_daily.parquet"

        daily_sales = (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns([pl.col("transaction_datetime").dt.date().alias("date")])
            .group_by(["year", "month", "date"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                    pl.mean("total_amount").alias("avg_transaction_value"),
                    pl.n_unique("category").alias("num_categories"),
                ]
            )
            .sort("date")
            .collect()
        )

        daily_sales.write_parquet(output_path, compression=self.compression)

        logger.info(f"Gold: Daily sales saved to {output_path}")

        return {
            "num_days": len(daily_sales),
            "total_sales": float(daily_sales["total_sales"].sum()),
            "avg_daily_sales": float(daily_sales["total_sales"].mean()),
        }

    def gold_sales_by_region(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating sales by region")

        output_path = self.aggregated_dir / "sales_by_region.parquet"

        sales_by_region = (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .group_by("region")
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                    pl.mean("total_amount").alias("avg_transaction_value"),
                    pl.n_unique("store_id").alias("num_stores"),
                ]
            )
            .sort("total_sales", descending=True)
            .collect()
        )

        sales_by_region.write_parquet(output_path, compression=self.compression)

        logger.info(f"Gold: Sales by region saved to {output_path}")

        return {
            "num_rows": len(sales_by_region),
            "total_sales": float(sales_by_region["total_sales"].sum()),
        }

    def gold_top_categories(self, top_n: int = 10) -> Dict[str, Any]:
        logger.info(f"Gold: Finding top {top_n} categories")

        output_path = self.aggregated_dir / "top_categories.parquet"

        category_performance = (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .group_by("category")
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                    pl.sum("quantity").alias("total_units_sold"),
                    pl.mean("total_amount").alias("avg_transaction_value"),
                ]
            )
            .sort("total_sales", descending=True)
            .head(top_n)
            .collect()
        )

        category_performance.write_parquet(output_path, compression=self.compression)

        logger.info(f"Gold: Top categories saved to {output_path}")

        return {
            "num_categories": len(category_performance),
            "total_sales": float(category_performance["total_sales"].sum()),
        }

    def gold_hourly_sales(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating hourly sales by weekday")

        output_path = self.aggregated_dir / "hourly_sales.parquet"

        hourly_sales = (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .group_by(["weekday", "hour"])
            .agg(pl.sum("total_amount").alias("sales"))
            .sort(["weekday", "hour"])
            .collect()
        )

        hourly_sales.write_parquet(output_path, compression="zstd")

        logger.info(f"Gold: Hourly sales saved to {output_path}")

        return {
            "num_hours": len(hourly_sales),
            "total_sales": float(hourly_sales["sales"].sum()),
        }

    def gold_discount_analysis(self) -> Dict[str, Any]:
        logger.info("Gold: Analyzing discount buckets")

        output_path = self.aggregated_dir / "discount_analysis.parquet"

        discount_buckets = (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns(
                [
                    pl.when(pl.col("discount_percent") == 0)
                    .then(pl.lit("No Discount"))
                    .when(pl.col("discount_percent") <= 0.10)
                    .then(pl.lit("1-10%"))
                    .when(pl.col("discount_percent") <= 0.20)
                    .then(pl.lit("11-20%"))
                    .when(pl.col("discount_percent") <= 0.30)
                    .then(pl.lit("21-30%"))
                    .otherwise(pl.lit("30%+"))
                    .alias("discount_bucket")
                ]
            )
            .group_by("discount_bucket")
            .agg(
                [
                    pl.count("transaction_id").alias("num_transactions"),
                    pl.sum("total_amount").alias("total_sales"),
                    pl.mean("total_amount").alias("avg_transaction_value"),
                ]
            )
            .collect()
        )

        discount_buckets.write_parquet(output_path, compression="zstd")

        logger.info(f"Gold: Discount analysis saved to {output_path}")

        return {
            "num_buckets": len(discount_buckets),
            "total_sales": float(discount_buckets["total_sales"].sum()),
        }

    def gold_yearly_by_region(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating yearly sales by region")

        output_path = self.aggregated_dir / "sales_yearly_by_region.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .group_by(["year", "region"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                    pl.mean("total_amount").alias("avg_transaction_value"),
                ]
            )
            .sort(["year", "region"])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Yearly sales by region saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_yearly_by_category(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating yearly sales by category")

        output_path = self.aggregated_dir / "sales_yearly_by_category.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .group_by(["year", "category"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                    pl.sum("quantity").alias("total_units_sold"),
                ]
            )
            .sort(["year", "total_sales"], descending=[False, True])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Yearly sales by category saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_yearly_top_products(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating yearly top products")

        output_path = self.aggregated_dir / "sales_yearly_top_products.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .group_by(["year", "product_id"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.sum("quantity").alias("total_units_sold"),
                    pl.count("transaction_id").alias("num_transactions"),
                ]
            )
            .sort(["year", "total_sales"], descending=[False, True])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Yearly top products saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_monthly_by_region(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating monthly sales by region")

        output_path = self.aggregated_dir / "sales_monthly_by_region.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns(
                [pl.col("transaction_datetime").dt.strftime("%Y-%m").alias("year_month")]
            )
            .group_by(["year", "month", "year_month", "region"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                ]
            )
            .sort(["year", "month", "region"])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Monthly sales by region saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_monthly_by_category(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating monthly sales by category")

        output_path = self.aggregated_dir / "sales_monthly_by_category.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns(
                [pl.col("transaction_datetime").dt.strftime("%Y-%m").alias("year_month")]
            )
            .group_by(["year", "month", "year_month", "category"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.sum("quantity").alias("total_units_sold"),
                ]
            )
            .sort(["year", "month", "total_sales"], descending=[False, False, True])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Monthly sales by category saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_monthly_top_products(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating monthly top products")

        output_path = self.aggregated_dir / "sales_monthly_top_products.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns(
                [pl.col("transaction_datetime").dt.strftime("%Y-%m").alias("year_month")]
            )
            .group_by(["year", "month", "year_month", "product_id"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.sum("quantity").alias("total_units_sold"),
                ]
            )
            .sort(["year", "month", "total_sales"], descending=[False, False, True])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Monthly top products saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_weekly_by_region(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating weekly sales by region")

        output_path = self.aggregated_dir / "sales_weekly_by_region.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns(
                [
                    pl.col("transaction_datetime").dt.year().alias("iso_year"),
                    pl.col("transaction_datetime").dt.week().alias("iso_week"),
                ]
            )
            .group_by(["iso_year", "iso_week", "region"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                ]
            )
            .sort(["iso_year", "iso_week", "region"])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Weekly sales by region saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_weekly_by_category(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating weekly sales by category")

        output_path = self.aggregated_dir / "sales_weekly_by_category.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns(
                [
                    pl.col("transaction_datetime").dt.year().alias("iso_year"),
                    pl.col("transaction_datetime").dt.week().alias("iso_week"),
                ]
            )
            .group_by(["iso_year", "iso_week", "category"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.sum("quantity").alias("total_units_sold"),
                ]
            )
            .sort(["iso_year", "iso_week", "total_sales"], descending=[False, False, True])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Weekly sales by category saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_weekly_top_products(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating weekly top products")

        output_path = self.aggregated_dir / "sales_weekly_top_products.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns(
                [
                    pl.col("transaction_datetime").dt.year().alias("iso_year"),
                    pl.col("transaction_datetime").dt.week().alias("iso_week"),
                ]
            )
            .group_by(["iso_year", "iso_week", "product_id"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.sum("quantity").alias("total_units_sold"),
                ]
            )
            .sort(["iso_year", "iso_week", "total_sales"], descending=[False, False, True])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Weekly top products saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_daily_by_region(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating daily sales by region")

        output_path = self.aggregated_dir / "sales_daily_by_region.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns([pl.col("transaction_datetime").dt.date().alias("date")])
            .group_by(["year", "month", "date", "region"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.count("transaction_id").alias("num_transactions"),
                ]
            )
            .sort(["date", "region"])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Daily sales by region saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_daily_by_category(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating daily sales by category")

        output_path = self.aggregated_dir / "sales_daily_by_category.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns([pl.col("transaction_datetime").dt.date().alias("date")])
            .group_by(["year", "month", "date", "category"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.sum("quantity").alias("total_units_sold"),
                ]
            )
            .sort(["date", "total_sales"], descending=[False, True])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Daily sales by category saved to {output_path}")

        return {"num_rows": num_rows}

    def gold_daily_top_products(self) -> Dict[str, Any]:
        logger.info("Gold: Calculating daily top products")

        output_path = self.aggregated_dir / "sales_daily_top_products.parquet"

        (
            pl.scan_parquet(self.cleaned_dir / "transactions_enriched.parquet")
            .with_columns([pl.col("transaction_datetime").dt.date().alias("date")])
            .group_by(["year", "month", "date", "product_id"])
            .agg(
                [
                    pl.sum("total_amount").alias("total_sales"),
                    pl.sum("quantity").alias("total_units_sold"),
                ]
            )
            .sort(["date", "total_sales"], descending=[False, True])
            .sink_parquet(output_path, compression=self.compression)
        )

        num_rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()

        logger.info(f"Gold: Daily top products saved to {output_path}")

        return {"num_rows": num_rows}

    # FULL PIPELINE EXECUTION

    def run_full_pipeline(self) -> Dict[str, Any]:
        results = {
            "bronze": {},
            "silver": {},
            "gold": {},
        }

        # Bronze layer
        logger.info("=" * 80)
        logger.info("BRONZE LAYER - Raw Data Ingestion")
        logger.info("=" * 80)
        results["bronze"]["stores"] = self.bronze_ingest_stores()
        results["bronze"]["products"] = self.bronze_ingest_products()
        results["bronze"]["transactions"] = self.bronze_ingest_transactions()

        # Silver layer
        logger.info("=" * 80)
        logger.info("SILVER LAYER - Data Cleaning & Enrichment")
        logger.info("=" * 80)
        results["silver"]["stores"] = self.silver_clean_stores()
        results["silver"]["products"] = self.silver_clean_products()
        results["silver"]["transactions"] = self.silver_enrich_transactions()

        # Gold layer
        logger.info("=" * 80)
        logger.info("GOLD LAYER - Business Aggregations")
        logger.info("=" * 80)
        results["gold"]["yearly_sales"] = self.gold_yearly_sales()
        results["gold"]["monthly_sales"] = self.gold_monthly_sales()
        results["gold"]["weekly_sales"] = self.gold_weekly_sales()
        results["gold"]["daily_sales"] = self.gold_daily_sales()
        results["gold"]["sales_by_region"] = self.gold_sales_by_region()
        results["gold"]["top_categories"] = self.gold_top_categories()
        results["gold"]["hourly_sales"] = self.gold_hourly_sales()
        results["gold"]["discount_analysis"] = self.gold_discount_analysis()

        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 80)

        return results
