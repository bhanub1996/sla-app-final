-- Replace ${catalog} and ${schema_prefix} before execution.
CREATE CATALOG IF NOT EXISTS ${catalog};
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_prefix}_bronze;
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_prefix}_silver;
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_prefix}_gold;
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_prefix}_ops;

CREATE VOLUME IF NOT EXISTS ${catalog}.${schema_prefix}_bronze.ecommerce_raw;
CREATE VOLUME IF NOT EXISTS ${catalog}.${schema_prefix}_bronze.ecommerce_checkpoint;
CREATE VOLUME IF NOT EXISTS ${catalog}.${schema_prefix}_bronze.ecommerce_schema;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_prefix}_ops.etl_audit_log (
  run_id STRING,
  task_name STRING,
  table_name STRING,
  status STRING,
  row_count BIGINT,
  message STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_prefix}_ops.data_quality_results (
  run_id STRING,
  layer STRING,
  table_name STRING,
  rule_name STRING,
  rule_status STRING,
  failed_count BIGINT,
  checked_at TIMESTAMP
) USING DELTA;
