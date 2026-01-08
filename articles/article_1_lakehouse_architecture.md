# I Built a Data Lakehouse That Processes 100 Million Transactions on My Laptop

*And it runs on my laptop without Spark, Hadoop, or any distributed system*

---

## The Challenge That Started It All

A few months ago, I asked myself a question: **Can you build a production-ready data platform on a single machine?**

Not a toy project. Not a demo. A real system that handles:
- 100 million transactions
- 500 stores across Germany
- 50,000 products
- 6 years of historical data (2020-2026)
- Interactive dashboards with sub-second response times

And do it all without:
- ❌ Apache Spark
- ❌ Distributed databases
- ❌ Cloud data warehouses
- ❌ Kubernetes clusters

Just **Python, smart architecture, and modern tools**.

Spoiler: It worked. And it was faster than I expected.

---

## The Architecture: Why Medallion?

I chose the **medallion architecture** - a pattern popularized by Databricks that organizes data into three layers:

```
Landing (CSV) → Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated) → Dashboard
```

Think of it like refining crude oil:
- **Bronze** = Crude oil (raw data, just extracted)
- **Silver** = Refined oil (cleaned, validated, ready for use)
- **Gold** = Premium fuel (optimized for specific purposes)

Each layer has a single responsibility, making the pipeline:
- **Easy to debug** - Problems isolated to specific layers
- **Easy to scale** - Reprocess only what's needed
- **Easy to understand** - New team members get it in 5 minutes

---

## Act 1: The Bronze Layer (Raw Ingestion)

**The Problem**: Storing 100 million rows as CSV would take ~3GB and be slow to parse.

**The Solution**: Generate directly to Parquet with aggressive compression.

I configured the data generator to write **Parquet** files directly using **Zstandard compression level 9**.

**The Efficiency Win**:
- **CSV Equivalent**: ~3.0 GB (estimated)
- **Actual Parquet**: 1.2 GB (60% smaller!)
- Columnar format enables reading only needed columns
- Reduced memory footprint

The Bronze layer consists of these raw Parquet files. They contain the data exactly as generated—messy, potentially duplicated, but efficiently stored.

Why keep a raw layer? Because if I mess up my cleaning logic in the next step, I can always re-read the Bronze files without regenerating 100M transactions.

---

## Act 2: The Silver Layer (Data Quality)

**The Problem**: Raw data is messy. Duplicates, invalid values, missing fields.

**The Solution**: Clean it once, use it everywhere.

The Silver layer is where I apply all data quality rules:

**What gets fixed**:
- ✅ **Duplicates removed** - Found 127,000 duplicate transactions (0.13%)
- ✅ **Invalid data filtered** - Removed negative quantities, zero prices
- ✅ **Derived fields added** - Year, month, week extracted from dates
- ✅ **Business rules validated** - Line totals match transaction totals

Here's the interesting part: I used **Polars** instead of Pandas.

Everyone uses Pandas. It's the standard. But Polars is **dramatically faster** for this workload.

Why Polars wins:
- **Lazy evaluation** - Optimizes entire query before execution
- **Parallel processing** - Uses all CPU cores by default
- **Columnar operations** - Native support for Parquet
- **Memory efficient** - Processes data in chunks

For operations like filtering, grouping, and joining on 100M rows, Polars significantly outperforms Pandas. The Silver layer processes efficiently without memory issues.

---

## Act 3: The Gold Layer (Pre-Aggregation Magic)

**The Problem**: Users want dashboards that load instantly. But aggregating 100M rows on every page load? Too slow.

**The Solution**: Pre-compute everything.

This is where the magic happens. Instead of aggregating on-demand, I create small, fast tables:

- `yearly_sales.parquet` - 6 years of data → 6 rows
- `monthly_sales.parquet` - Monthly breakdown → 72 rows
- `weekly_sales.parquet` - Weekly breakdown → 312 rows
- `daily_sales.parquet` - Daily breakdown → 2,190 rows
- `sales_by_region.parquet` - 16 regions × 6 years = 96 rows
- `sales_by_category.parquet` - 11 categories × 6 years = 66 rows

**Total Gold layer size**: 50 MB (vs. 1.3 GB Silver)

Now when someone opens the dashboard:
- **Before**: Query 100M rows, aggregate, display - slow and painful
- **After**: Read 72 rows from Gold layer - nearly instant

That's a **massive speedup**.

---

## The Complete Pipeline in Numbers

Here's what happens when I run the full pipeline:

| Stage | Input | Output | What Happens |
|-------|-------|--------|--------------|
| **Bronze** | Generator | ~1.2GB Parquet | Direct Parquet generation |
| **Silver** | 1.2GB | ~1.3GB Parquet | Cleaning, validation, enrichment |
| **Gold** | 1.3GB | ~50MB Parquet | Pre-computed aggregations |
| **Total** | Generator | ~2.5GB | Complete medallion pipeline |

The entire pipeline runs efficiently on a laptop with 16GB RAM. Dashboard loads quickly.

---

## Why This Architecture Works

### 1. Separation of Concerns

Each layer has one job:
- **Bronze**: Ingest fast
- **Silver**: Clean thoroughly
- **Gold**: Aggregate smartly

When something breaks, I know exactly where to look.

### 2. Incremental Processing

New data? I don't reprocess everything:
- Append to Bronze
- Clean only new records → Silver
- Update only affected aggregations → Gold

### 3. Cost Efficiency

This entire system costs **$0/month** to run locally. For cloud deployment, a single-instance approach is far more economical than running distributed systems like Spark clusters or managed data warehouses.

---

## The Tools That Made It Possible

I didn't reinvent the wheel. I combined mature tools:

**Polars** - The DataFrame library that's significantly faster than Pandas
- Lazy evaluation (optimizes entire query)
- Parallel processing (uses all CPU cores)
- Memory efficient (columnar format)

**Parquet + Zstandard** - The storage format
- Columnar = Read only needed columns
- Zstandard = Best compression ratio
- Level 9 = Maximum compression without huge CPU cost

**Medallion Architecture** - The design pattern
- Proven at scale (Databricks, Netflix, Uber)
- Simple enough for one person
- Powerful enough for enterprise

---

## What I Learned

### 1. You Don't Always Need Big Data Tools

Spark is amazing for truly distributed workloads. But if your data fits on one machine (even 100M+ rows), simpler tools are:
- Faster to develop
- Easier to debug
- Cheaper to run

### 2. Pre-Aggregation > Real-Time Everything

The Gold layer is a game-changer. Yes, I'm computing things in advance. But the **user experience** is dramatically better:
- Dashboards load quickly
- No complex query optimization needed
- Predictable performance

### 3. Modern Python is Seriously Fast

Polars, Parquet, and modern libraries changed the game. The Python ecosystem of 2026 is not the Python of 2016.

---

## Try It Yourself

The entire project is open source. You can:
1. Clone the repo
2. Generate 100M transactions
3. Run the pipeline
4. Explore the dashboard

**System requirements**:
- 16GB RAM (8GB minimum for smaller datasets)
- 50GB free disk space
- Python 3.11+

That's it. No Docker, no Kubernetes, no cloud account.

---

## What's Next in This Series

This is **Part 1** of a 4-part series:

1. **Lakehouse Architecture** (you just read this)
2. **Synthetic Data Generation** - How I created 100M realistic transactions with seasonal patterns
3. **Airflow vs Prefect** - I built the same pipeline in both. Here's what I learned.
4. **Interactive Dashboards** - Building a 4-level drill-down dashboard with Dash

---

## 🔗 Project Repository

The complete code for this project is on GitHub:
**[github.com/yourusername/retail_lakehouse](https://github.com/yourusername/retail_lakehouse)**

---

## 🙏 Built With

This project uses these fantastic open-source tools:
- **[Polars](https://pola.rs/)** - Fast DataFrame library
- **[Apache Parquet](https://parquet.apache.org/)** - Columnar storage
- **[Zstandard](https://facebook.github.io/zstd/)** - High-performance compression

---

*If you found this helpful, follow me for Part 2 where I explain how to generate 100M realistic transactions with seasonal patterns, holidays, and year-over-year growth.*

*Questions? Leave a comment below!* 👇
