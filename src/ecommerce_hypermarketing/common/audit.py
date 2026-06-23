from datetime import datetime
from pyspark.sql import Row
from ecommerce_hypermarketing.common.config import names, fq


def audit(spark, catalog, schema_prefix, run_id, task_name, table_name, status, row_count=None, message=None, started_at=None):
    ns = names(catalog, schema_prefix)
    started_at = started_at or datetime.utcnow()
    row = Row(run_id=run_id, task_name=task_name, table_name=table_name, status=status,
              row_count=row_count, message=message, started_at=started_at, ended_at=datetime.utcnow())
    spark.createDataFrame([row]).write.format('delta').mode('append').saveAsTable(fq(ns['ops'], 'etl_audit_log'))
