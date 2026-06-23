from pyspark.sql import functions as F
from ecommerce_hypermarketing.common.config import names, fq, quality_rules


def write_dq_result(spark, catalog, schema_prefix, run_id, layer, table_name, rule_name, status, failed_count):
    ns = names(catalog, schema_prefix)
    rows = [(run_id, layer, table_name, rule_name, status, int(failed_count))]
    df = spark.createDataFrame(rows, ['run_id', 'layer', 'table_name', 'rule_name', 'rule_status', 'failed_count'])
    df = df.withColumn('checked_at', F.current_timestamp())
    df.write.format('delta').mode('append').saveAsTable(fq(ns['ops'], 'data_quality_results'))


def run_table_checks(spark, df, catalog, schema_prefix, run_id, layer, table_name, keys=None, not_null_cols=None):
    not_null_cols = not_null_cols or []
    keys = keys or []
    for col_name in not_null_cols:
        failed = df.filter(F.col(col_name).isNull()).count() if col_name in df.columns else df.count()
        write_dq_result(spark, catalog, schema_prefix, run_id, layer, table_name, f'not_null:{col_name}', 'PASS' if failed == 0 else 'FAIL', failed)
    if keys:
        dupes = (df.groupBy(*keys).count().filter(F.col('count') > 1).count())
        write_dq_result(spark, catalog, schema_prefix, run_id, layer, table_name, 'unique_key:' + ','.join(keys), 'PASS' if dupes == 0 else 'FAIL', dupes)
