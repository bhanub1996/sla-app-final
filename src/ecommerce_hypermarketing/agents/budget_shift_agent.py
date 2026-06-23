from pyspark.sql import functions as F
from ecommerce_hypermarketing.common.config import names, fq


class BudgetShiftAgent:
    def __init__(self, spark, catalog, schema_prefix):
        self.spark = spark
        self.catalog = catalog
        self.schema_prefix = schema_prefix

    def build(self):
        ns = names(self.catalog, self.schema_prefix)
        df = self.spark.table(fq(ns['silver'], 'fact_channel_performance_hourly'))
        latest_ts = df.agg(F.max('timestamp').alias('ts')).collect()[0]['ts']
        latest = df.filter(F.col('timestamp') == F.lit(latest_ts))
        avg_roas = latest.agg(F.avg('roas').alias('avg_roas')).collect()[0]['avg_roas'] or 0
        result = (latest.withColumn('suggested_budget_shift_pct',
                    F.when(F.col('roas') >= F.lit(avg_roas * 1.15), F.lit(12.0))
                     .when(F.col('roas') >= F.lit(avg_roas), F.lit(5.0))
                     .otherwise(F.lit(-8.0)))
                  .withColumn('budget_action', F.when(F.col('suggested_budget_shift_pct') > 0, F.lit('Increase')).otherwise(F.lit('Reduce')))
                  .withColumn('created_at', F.current_timestamp()))
        result.write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable(fq(ns['gold'], 'channel_budget_shift_current'))
        return result
