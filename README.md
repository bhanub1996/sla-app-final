# SLA App Databricks Conversion — E-commerce Dynamic Hyper-Marketing

This package is a Databricks-standard conversion of the dashboard/ETL/agent requirements.

## Architecture

- **Bronze**: raw CSV ingestion into Delta tables from Unity Catalog Volumes.
- **Silver**: typed, deduplicated, quality-checked tables.
- **Gold**: business-ready dashboard tables, push-now rankings, action recommendations, and drilldown datasets.
- **Ops**: audit logs and data-quality results.

## Core setup

```bash
# Upload generated CSV files to the raw UC volume after creating UC objects.
./scripts/upload_seed_data.sh workspace ecommerce_hypermarketing_dev ./data DEFAULT

# Deploy bundle.
./scripts/deploy_bundle.sh dev DEFAULT

# Run job.
./scripts/run_hourly_job.sh dev DEFAULT
```

## Databricks bundle job

The bundle job is `ecommerce_hypermarketing_hourly_etl` and runs:

1. `00_setup_uc_objects.py`
2. `01_ingest_bronze.py`
3. `02_build_silver.py`
4. `03_build_gold.py`
5. `04_optimize_tables.py`

## Metadata-driven ETL

Table schema, keys, partitions, ZORDER columns, and file mappings are in:

- `metadata/table_registry.json`
- `metadata/quality_rules.json`
- `metadata/pipeline_config.json`

## Source files expected

Place these CSVs in `/Volumes/<catalog>/<schema_prefix>_bronze/ecommerce_raw/`:

- product_master.csv
- marketing_performance_hourly.csv
- stock_movement_hourly.csv
- order_transactions_sample.csv
- orders_hourly_summary.csv
- category_performance_hourly.csv
- channel_performance_hourly.csv
- restock_schedule.csv

## Dashboard SQL

Use these SQL files in Databricks SQL / AI/BI dashboards:

- `sql/dashboard/main_dashboard_queries.sql`
- `sql/dashboard/second_level_drilldowns.sql`
