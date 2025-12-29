"""
Prefect flow for the Retail Lakehouse Medallion Pipeline.

This flow orchestrates the Bronze → Silver → Gold transformation.
"""

import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))

from pipeline import MedallionPipeline


@task(name="bronze_ingest_stores", task_run_name="Bronze: Ingest Stores")
def bronze_ingest_stores() -> int:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    rows = pipeline.bronze_ingest_stores()
    logger.info(f"Ingested {rows} stores")
    return rows


@task(name="bronze_ingest_products", task_run_name="Bronze: Ingest Products")
def bronze_ingest_products() -> int:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    rows = pipeline.bronze_ingest_products()
    logger.info(f"Ingested {rows} products")
    return rows


@task(name="bronze_ingest_transactions", task_run_name="Bronze: Ingest Transactions")
def bronze_ingest_transactions() -> int:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    rows = pipeline.bronze_ingest_transactions()
    logger.info(f"Ingested {rows:,} transactions")
    return rows


@task(name="silver_clean_stores", task_run_name="Silver: Clean Stores")
def silver_clean_stores(upstream_rows: int) -> int:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    rows = pipeline.silver_clean_stores()
    logger.info(f"Cleaned {rows} stores")
    return rows


@task(name="silver_clean_products", task_run_name="Silver: Clean Products")
def silver_clean_products(upstream_rows: int) -> int:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    rows = pipeline.silver_clean_products()
    logger.info(f"Cleaned {rows} products")
    return rows


@task(name="silver_enrich_transactions", task_run_name="Silver: Enrich Transactions")
def silver_enrich_transactions(
    stores_rows: int, products_rows: int, transactions_rows: int
) -> int:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    rows = pipeline.silver_enrich_transactions()
    logger.info(f"Enriched {rows:,} transactions")
    return rows


@task(name="gold_yearly_sales", task_run_name="Gold: Yearly Sales")
def gold_yearly_sales(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_yearly_sales()
    logger.info(f"Calculated sales for {result['num_years']} years")
    return result


@task(name="gold_monthly_sales", task_run_name="Gold: Monthly Sales")
def gold_monthly_sales(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_monthly_sales()
    logger.info(f"Calculated sales for {result['num_months']} months")
    return result


@task(name="gold_weekly_sales", task_run_name="Gold: Weekly Sales")
def gold_weekly_sales(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_weekly_sales()
    logger.info(f"Calculated sales for {result['num_weeks']} weeks")
    return result


@task(name="gold_daily_sales", task_run_name="Gold: Daily Sales")
def gold_daily_sales(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_daily_sales()
    logger.info(f"Calculated sales for {result['num_days']} days")
    return result


@task(name="gold_sales_by_region", task_run_name="Gold: Sales by Region")
def gold_sales_by_region(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_sales_by_region()
    logger.info(f"Calculated sales for {result['num_rows']} regions")
    return result


@task(name="gold_top_categories", task_run_name="Gold: Top Categories")
def gold_top_categories(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_top_categories()
    logger.info(f"Calculated top {result['num_categories']} categories")
    return result


@task(name="gold_hourly_sales", task_run_name="Gold: Hourly Sales")
def gold_hourly_sales(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_hourly_sales()
    logger.info(f"Calculated hourly sales for {result['num_hours']} hours")
    return result


@task(name="gold_discount_analysis", task_run_name="Gold: Discount Analysis")
def gold_discount_analysis(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_discount_analysis()
    logger.info(f"Analyzed {result['num_buckets']} discount buckets")
    return result


@task(name="gold_yearly_by_region", task_run_name="Gold: Yearly Sales by Region")
def gold_yearly_by_region(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_yearly_by_region()
    logger.info(f"Calculated yearly sales for {result['num_rows']} region-year combinations")
    return result


@task(name="gold_yearly_by_category", task_run_name="Gold: Yearly Sales by Category")
def gold_yearly_by_category(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_yearly_by_category()
    logger.info(f"Calculated yearly sales for {result['num_rows']} category-year combinations")
    return result


@task(name="gold_yearly_top_products", task_run_name="Gold: Yearly Top Products")
def gold_yearly_top_products(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_yearly_top_products()
    logger.info(f"Calculated top {result['num_rows']} products by year")
    return result


@task(name="gold_monthly_by_region", task_run_name="Gold: Monthly Sales by Region")
def gold_monthly_by_region(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_monthly_by_region()
    logger.info(f"Calculated monthly sales for {result['num_rows']} region-month combinations")
    return result


@task(name="gold_monthly_by_category", task_run_name="Gold: Monthly Sales by Category")
def gold_monthly_by_category(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_monthly_by_category()
    logger.info(f"Calculated monthly sales for {result['num_rows']} category-month combinations")
    return result


@task(name="gold_monthly_top_products", task_run_name="Gold: Monthly Top Products")
def gold_monthly_top_products(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_monthly_top_products()
    logger.info(f"Calculated top {result['num_rows']} products by month")
    return result


@task(name="gold_weekly_by_region", task_run_name="Gold: Weekly Sales by Region")
def gold_weekly_by_region(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_weekly_by_region()
    logger.info(f"Calculated weekly sales for {result['num_rows']} region-week combinations")
    return result


@task(name="gold_weekly_by_category", task_run_name="Gold: Weekly Sales by Category")
def gold_weekly_by_category(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_weekly_by_category()
    logger.info(f"Calculated weekly sales for {result['num_rows']} category-week combinations")
    return result


@task(name="gold_weekly_top_products", task_run_name="Gold: Weekly Top Products")
def gold_weekly_top_products(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_weekly_top_products()
    logger.info(f"Calculated top {result['num_rows']} products by week")
    return result


@task(name="gold_daily_by_region", task_run_name="Gold: Daily Sales by Region")
def gold_daily_by_region(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_daily_by_region()
    logger.info(f"Calculated daily sales for {result['num_rows']} region-day combinations")
    return result


@task(name="gold_daily_by_category", task_run_name="Gold: Daily Sales by Category")
def gold_daily_by_category(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_daily_by_category()
    logger.info(f"Calculated daily sales for {result['num_rows']} category-day combinations")
    return result


@task(name="gold_daily_top_products", task_run_name="Gold: Daily Top Products")
def gold_daily_top_products(upstream_rows: int) -> dict:
    logger = get_run_logger()
    pipeline = MedallionPipeline()
    result = pipeline.gold_daily_top_products()
    logger.info(f"Calculated top {result['num_rows']} products by day")
    return result


@flow(name="retail_medallion_pipeline", log_prints=True)
def retail_medallion_pipeline():
    logger = get_run_logger()

    logger.info("=" * 80)
    logger.info("BRONZE LAYER - Raw Data Ingestion")
    logger.info("=" * 80)

    stores_bronze = bronze_ingest_stores()
    products_bronze = bronze_ingest_products()
    transactions_bronze = bronze_ingest_transactions()

    logger.info("=" * 80)
    logger.info("SILVER LAYER - Data Cleaning & Enrichment")
    logger.info("=" * 80)

    stores_silver = silver_clean_stores(stores_bronze)
    products_silver = silver_clean_products(products_bronze)

    transactions_silver = silver_enrich_transactions(
        stores_silver, products_silver, transactions_bronze
    )

    logger.info("=" * 80)
    logger.info("GOLD LAYER - Business Aggregations")
    logger.info("=" * 80)

    logger.info("Batch 1: Time-based aggregations")
    yearly_result = gold_yearly_sales(transactions_silver)
    monthly_result = gold_monthly_sales(transactions_silver)
    weekly_result = gold_weekly_sales(transactions_silver)
    daily_result = gold_daily_sales(transactions_silver)

    logger.info("Batch 2: Regional and category aggregations")
    sales_by_region = gold_sales_by_region(transactions_silver)
    top_categories = gold_top_categories(transactions_silver)
    hourly_sales_result = gold_hourly_sales(transactions_silver)
    discount_result = gold_discount_analysis(transactions_silver)

    logger.info("Batch 3: Yearly drill-downs")
    yearly_by_region_result = gold_yearly_by_region(transactions_silver)
    yearly_by_category_result = gold_yearly_by_category(transactions_silver)
    yearly_top_products_result = gold_yearly_top_products(transactions_silver)

    logger.info("Batch 4: Monthly drill-downs")
    monthly_by_region_result = gold_monthly_by_region(transactions_silver)
    monthly_by_category_result = gold_monthly_by_category(transactions_silver)
    monthly_top_products_result = gold_monthly_top_products(transactions_silver)

    logger.info("Batch 5: Weekly drill-downs")
    weekly_by_region_result = gold_weekly_by_region(transactions_silver)
    weekly_by_category_result = gold_weekly_by_category(transactions_silver)
    weekly_top_products_result = gold_weekly_top_products(transactions_silver)

    logger.info("Batch 6: Daily drill-downs")
    daily_by_region_result = gold_daily_by_region(transactions_silver)
    daily_by_category_result = gold_daily_by_category(transactions_silver)
    daily_top_products_result = gold_daily_top_products(transactions_silver)

    logger.info("=" * 80)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 80)

    return {
        "bronze": {
            "stores": stores_bronze,
            "products": products_bronze,
            "transactions": transactions_bronze,
        },
        "silver": {
            "stores": stores_silver,
            "products": products_silver,
            "transactions": transactions_silver,
        },
        "gold": {
            "yearly_sales": yearly_result,
            "monthly_sales": monthly_result,
            "weekly_sales": weekly_result,
            "daily_sales": daily_result,
            "sales_by_region": sales_by_region,
            "top_categories": top_categories,
            "hourly_sales": hourly_sales_result,
            "discount_analysis": discount_result,
            "yearly_by_region": yearly_by_region_result,
            "yearly_by_category": yearly_by_category_result,
            "yearly_top_products": yearly_top_products_result,
            "monthly_by_region": monthly_by_region_result,
            "monthly_by_category": monthly_by_category_result,
            "monthly_top_products": monthly_top_products_result,
            "weekly_by_region": weekly_by_region_result,
            "weekly_by_category": weekly_by_category_result,
            "weekly_top_products": weekly_top_products_result,
            "daily_by_region": daily_by_region_result,
            "daily_by_category": daily_by_category_result,
            "daily_top_products": daily_top_products_result,
        },
    }


if __name__ == "__main__":
    retail_medallion_pipeline()
