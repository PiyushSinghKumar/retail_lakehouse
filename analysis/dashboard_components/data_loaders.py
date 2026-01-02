"""Data loading functions for the dashboard."""

from pathlib import Path
import polars as pl


def load_yearly_sales(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_yearly.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_yearly_by_region(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_yearly_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_yearly_by_category(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_yearly_by_category.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_monthly_sales(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_monthly.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_monthly_by_region(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_monthly_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_monthly_by_category(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_monthly_by_category.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_weekly_sales(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_weekly.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_weekly_by_region(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_weekly_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_weekly_by_category(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_weekly_by_category.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_daily_sales(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_daily.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_daily_by_region(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_daily_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_daily_by_category(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_daily_by_category.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_sales_by_region(aggregated_dir: Path):
    file_path = aggregated_dir / "sales_by_region.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()


def load_top_categories(aggregated_dir: Path):
    file_path = aggregated_dir / "top_categories.parquet"
    return pl.read_parquet(file_path) if file_path.exists() else pl.DataFrame()
