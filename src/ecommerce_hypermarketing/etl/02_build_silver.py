from uuid import uuid4
from pyspark.sql import functions as F
from ecommerce_hypermarketing.common.args import parse_args
from ecommerce_hypermarketing.common.config import names, fq, table_registry, quality_rules
from ecommerce_hypermarketing.common.delta_utils import merge_upsert
from ecommerce_hypermarketing.common.dq import run_table_checks
from ecommerce_hypermarketing.common.audit import audit


def normalize_columns(df):
    for c in df.columns:
        n = c.strip().lower().replace(' ', '_').replace('-', '_')
        if c != n:
            df = df.withColumnRenamed(c, n)
    return df


def cast_to_schema(df, columns):
    df = normalize_columns(df)
    for col_name, dtype in columns.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
        else:
            df = df.withColumn(col_name, F.lit(None).cast(dtype))
    selected = list(columns.keys()) + [c for c in ['_source_name', '_ingested_at', '_input_file_name'] if c in df.columns]
    return df.select(*selected)


def build_silver(spark, catalog, schema_prefix, run_id):
    ns = names(catalog, schema_prefix)
    registry = table_registry()
    rules = quality_rules().get('tables', {})
    for source_name, meta in registry.items():
        bronze_table = fq(ns['bronze'], f'raw_{source_name}')
        silver_table = fq(ns['silver'], meta['target'])
        try:
            df = spark.table(bronze_table)
            df = cast_to_schema(df, meta['columns'])
            df = df.dropDuplicates(meta['primary_keys']).withColumn('_silver_processed_at', F.current_timestamp())
            table_rules = rules.get(meta['target'], {})
            run_table_checks(spark, df, catalog, schema_prefix, run_id, 'silver', meta['target'], table_rules.get('unique_key', meta['primary_keys']), table_rules.get('not_null', []))
            row_count = merge_upsert(spark, df, silver_table, meta['primary_keys'])
            audit(spark, catalog, schema_prefix, run_id, 'build_silver', silver_table, 'SUCCESS', row_count, None)
        except Exception as exc:
            audit(spark, catalog, schema_prefix, run_id, 'build_silver', silver_table, 'FAILED', None, str(exc))
            raise


if __name__ == '__main__':
    args = parse_args()
    build_silver(spark, args.catalog, args.schema_prefix, args.run_id or str(uuid4()))  # noqa: F821
