# Retail Lakehouse

A retail analytics platform with medallion architecture (Bronze → Silver → Gold), ETL pipelines orchestrated by Airflow/Prefect, and an interactive dashboard.

## Quick Start

```bash
# 1. Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure paths in config.json

# 3. Generate data (basic)
cd data_generation
python generate_all.py --stores 500 --products 50000 --transactions 100000000 --seed 42 --output-dir ../data/landing

# Or with custom parameters
python generate_all.py \
  --stores 500 --products 50000 --transactions 10000000 --seed 42 \
  --output-dir ../data/landing \
  --start-date 2023-01-01 --end-date 2024-12-31 \
  --yoy-growth 0.20 --no-seasonality

# 4. Run pipeline (choose one)
cd ../airflow && ./run_airflow.sh     # http://localhost:8080
cd ../prefect && ./run_flow.sh        # http://localhost:4200

# 5. View dashboard
cd ../analysis
python retail_dashboard.py           # http://localhost:8050
```

## Architecture

**Medallion Pipeline:**
```
Landing (CSV) → Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated) → Dashboard
```

**Data Layers:**
- **Bronze**: Raw Parquet with Zstandard compression
- **Silver**: Cleaned, validated, enriched data
- **Gold**: Pre-computed business aggregations (yearly, monthly, weekly, daily)

## Features

### Data Generation
- Configurable synthetic retail data (stores, products, transactions)
- Realistic German market simulation with seasonality & holidays
- Scalable to 100M+ transactions
- **Control Parameters**: YoY growth, date range, seasonality, holidays, traffic patterns
- Supports streaming scenarios (configurable time windows)

### ETL Pipeline
- **Bronze**: CSV → Parquet ingestion
- **Silver**: Cleaning, validation, deduplication
- **Gold**: Aggregations by time period, region, category

### Dashboard
- **Overview**: All years, trends, regional/category breakdowns
- **Yearly Drill-Down**: Monthly breakdown when year selected
- **Monthly Drill-Down**: Daily sales for selected month
- **Weekly Drill-Down**: Daily sales for selected week
- Dynamic filters, auto-selection of latest values

### Orchestration
- **Airflow 3.x**: Standalone mode (Docker/Podman)
- **Prefect 3.x**: Modern dataflow orchestration

## Project Structure

```
retail_lakehouse/
├── data_generation/       # Synthetic data generation
├── shared/                # Shared ETL pipeline logic
├── airflow/               # Airflow DAG & config
├── prefect/               # Prefect flow & config
├── analysis/              # Dashboard & reports
│   ├── dashboard_components/
│   └── retail_dashboard.py
├── config.json            # Path configuration
└── README.md
```

## Configuration

Edit `config.json` for your environment:

```json
{
  "project_root": "/path/to/retail_lakehouse",
  "data_dir": "/path/to/data",
  "compression": "zstd",
  "compression_level": 9
}
```

### Data Generation Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--start-date` | 2020-01-01 | Transaction start date (YYYY-MM-DD) |
| `--end-date` | Today | Transaction end date (YYYY-MM-DD) |
| `--yoy-growth` | 0.15 | Year-over-year growth rate (0.15 = 15%) |
| `--no-seasonality` | False | Disable seasonal patterns |
| `--no-holidays` | False | Disable holiday effects |
| `--avg-items` | 3 | Average items per transaction |
| `--peak-multiplier` | 1.0 | Peak hour traffic multiplier |

**Examples:**
```bash
# Recession scenario: negative growth, shorter period
python generate_all.py --stores 500 --products 50000 --transactions 10000000 \
  --seed 42 --output-dir ../data/landing \
  --start-date 2023-01-01 --end-date 2023-12-31 --yoy-growth -0.05

# Boom scenario: high growth, strong seasonality
python generate_all.py --stores 500 --products 50000 --transactions 10000000 \
  --seed 42 --output-dir ../data/landing \
  --yoy-growth 0.30 --peak-multiplier 1.5

# Uniform distribution: no seasonality or holidays (for testing)
python generate_all.py --stores 500 --products 50000 --transactions 10000000 \
  --seed 42 --output-dir ../data/landing \
  --no-seasonality --no-holidays
```

## Technology Stack

- **Data**: Polars, Parquet, Zstandard
- **Orchestration**: Airflow 3.x, Prefect 3.x
- **Visualization**: Dash, Plotly, Bootstrap Components
- **Generation**: Faker, NumPy
- **Containers**: Docker/Podman

## Performance (100M Transactions)

| Stage | Time | Size |
|-------|------|------|
| Data Generation | ~15-20 min | 3GB CSV |
| Bronze Ingestion | ~5 min | 1.2GB Parquet |
| Silver Cleaning | ~8 min | 1.3GB Parquet |
| Gold Aggregation | ~10 min | 50MB Parquet |
| **Total Pipeline** | **~25 min** | - |

**Dashboard**: <2s load, <500ms chart rendering

## Notes

- Requires 16GB+ RAM for 100M transactions
- Airflow standalone mode is for development only
- Dashboard auto-selects latest year/month/week for better UX
- All data files are gitignored
