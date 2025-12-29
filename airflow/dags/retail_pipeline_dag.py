"""
Airflow DAG for the Retail Lakehouse Medallion Pipeline.

This DAG orchestrates the Bronze → Silver → Gold transformation.
"""

import sys
from datetime import datetime
from pathlib import Path

from airflow.operators.python import PythonOperator

from airflow import DAG

PROJECT_ROOT = Path(__file__).parent.parent.parent
SHARED_DIR = PROJECT_ROOT / "shared"

sys.path.insert(0, str(SHARED_DIR))

from pipeline import MedallionPipeline


def bronze_ingest_stores(**context):
    pipeline = MedallionPipeline()
    pipeline.bronze_ingest_stores()


def bronze_ingest_products(**context):
    pipeline = MedallionPipeline()
    pipeline.bronze_ingest_products()


def bronze_ingest_transactions(**context):
    pipeline = MedallionPipeline()
    pipeline.bronze_ingest_transactions()


def silver_clean_stores(**context):
    pipeline = MedallionPipeline()
    pipeline.silver_clean_stores()


def silver_clean_products(**context):
    pipeline = MedallionPipeline()
    pipeline.silver_clean_products()


def silver_enrich_transactions(**context):
    pipeline = MedallionPipeline()
    pipeline.silver_enrich_transactions()


def gold_yearly_sales(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_yearly_sales()


def gold_monthly_sales(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_monthly_sales()


def gold_weekly_sales(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_weekly_sales()


def gold_daily_sales(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_daily_sales()


def gold_sales_by_region(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_sales_by_region()


def gold_top_categories(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_top_categories()


def gold_hourly_sales(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_hourly_sales()


def gold_discount_analysis(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_discount_analysis()


def gold_yearly_by_region(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_yearly_by_region()


def gold_yearly_by_category(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_yearly_by_category()


def gold_yearly_top_products(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_yearly_top_products()


def gold_monthly_by_region(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_monthly_by_region()


def gold_monthly_by_category(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_monthly_by_category()


def gold_monthly_top_products(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_monthly_top_products()


def gold_weekly_by_region(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_weekly_by_region()


def gold_weekly_by_category(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_weekly_by_category()


def gold_weekly_top_products(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_weekly_top_products()


def gold_daily_by_region(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_daily_by_region()


def gold_daily_by_category(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_daily_by_category()


def gold_daily_top_products(**context):
    pipeline = MedallionPipeline()
    pipeline.gold_daily_top_products()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "retail_medallion_pipeline",
    default_args=default_args,
    description="Retail Lakehouse Pipeline (Bronze → Silver → Gold)",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "medallion", "lakehouse"],
) as dag:

    task_bronze_stores = PythonOperator(
        task_id="bronze_ingest_stores",
        python_callable=bronze_ingest_stores,
    )

    task_bronze_products = PythonOperator(
        task_id="bronze_ingest_products",
        python_callable=bronze_ingest_products,
    )

    task_bronze_transactions = PythonOperator(
        task_id="bronze_ingest_transactions",
        python_callable=bronze_ingest_transactions,
    )

    task_silver_stores = PythonOperator(
        task_id="silver_clean_stores",
        python_callable=silver_clean_stores,
    )

    task_silver_products = PythonOperator(
        task_id="silver_clean_products",
        python_callable=silver_clean_products,
    )

    task_silver_transactions = PythonOperator(
        task_id="silver_enrich_transactions",
        python_callable=silver_enrich_transactions,
    )

    task_gold_yearly = PythonOperator(
        task_id="gold_yearly_sales",
        python_callable=gold_yearly_sales,
    )

    task_gold_monthly = PythonOperator(
        task_id="gold_monthly_sales",
        python_callable=gold_monthly_sales,
    )

    task_gold_weekly = PythonOperator(
        task_id="gold_weekly_sales",
        python_callable=gold_weekly_sales,
    )

    task_gold_daily = PythonOperator(
        task_id="gold_daily_sales",
        python_callable=gold_daily_sales,
    )

    task_gold_sales_region = PythonOperator(
        task_id="gold_sales_by_region",
        python_callable=gold_sales_by_region,
    )

    task_gold_top_categories = PythonOperator(
        task_id="gold_top_categories",
        python_callable=gold_top_categories,
    )

    task_gold_hourly = PythonOperator(
        task_id="gold_hourly_sales",
        python_callable=gold_hourly_sales,
    )

    task_gold_discount = PythonOperator(
        task_id="gold_discount_analysis",
        python_callable=gold_discount_analysis,
    )

    task_gold_yearly_by_region = PythonOperator(
        task_id="gold_yearly_by_region",
        python_callable=gold_yearly_by_region,
    )

    task_gold_yearly_by_category = PythonOperator(
        task_id="gold_yearly_by_category",
        python_callable=gold_yearly_by_category,
    )

    task_gold_yearly_top_products = PythonOperator(
        task_id="gold_yearly_top_products",
        python_callable=gold_yearly_top_products,
    )

    task_gold_monthly_by_region = PythonOperator(
        task_id="gold_monthly_by_region",
        python_callable=gold_monthly_by_region,
    )

    task_gold_monthly_by_category = PythonOperator(
        task_id="gold_monthly_by_category",
        python_callable=gold_monthly_by_category,
    )

    task_gold_monthly_top_products = PythonOperator(
        task_id="gold_monthly_top_products",
        python_callable=gold_monthly_top_products,
    )

    task_gold_weekly_by_region = PythonOperator(
        task_id="gold_weekly_by_region",
        python_callable=gold_weekly_by_region,
    )

    task_gold_weekly_by_category = PythonOperator(
        task_id="gold_weekly_by_category",
        python_callable=gold_weekly_by_category,
    )

    task_gold_weekly_top_products = PythonOperator(
        task_id="gold_weekly_top_products",
        python_callable=gold_weekly_top_products,
    )

    task_gold_daily_by_region = PythonOperator(
        task_id="gold_daily_by_region",
        python_callable=gold_daily_by_region,
    )

    task_gold_daily_by_category = PythonOperator(
        task_id="gold_daily_by_category",
        python_callable=gold_daily_by_category,
    )

    task_gold_daily_top_products = PythonOperator(
        task_id="gold_daily_top_products",
        python_callable=gold_daily_top_products,
    )

    [task_bronze_stores, task_bronze_products, task_bronze_transactions]

    task_bronze_stores >> task_silver_stores
    task_bronze_products >> task_silver_products
    task_bronze_transactions >> task_silver_transactions
    [task_silver_stores, task_silver_products] >> task_silver_transactions

    task_silver_transactions >> [
        task_gold_yearly,
        task_gold_monthly,
        task_gold_weekly,
        task_gold_daily,
        task_gold_sales_region,
        task_gold_top_categories,
        task_gold_hourly,
        task_gold_discount,
        task_gold_yearly_by_region,
        task_gold_yearly_by_category,
        task_gold_yearly_top_products,
        task_gold_monthly_by_region,
        task_gold_monthly_by_category,
        task_gold_monthly_top_products,
        task_gold_weekly_by_region,
        task_gold_weekly_by_category,
        task_gold_weekly_top_products,
        task_gold_daily_by_region,
        task_gold_daily_by_category,
        task_gold_daily_top_products,
    ]
