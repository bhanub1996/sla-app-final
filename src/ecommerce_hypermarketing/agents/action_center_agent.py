from pyspark.sql import functions as F
from ecommerce_hypermarketing.common.config import names, fq


class ActionCenterAgent:
    def __init__(self, spark, catalog, schema_prefix):
        self.spark = spark
        self.catalog = catalog
        self.schema_prefix = schema_prefix

    def build(self, limit=5):
        ns = names(self.catalog, self.schema_prefix)
        src = fq(ns['gold'], 'product_pushnow_current')
        target = fq(ns['gold'], 'recommended_actions_current_hour')
        df = (self.spark.table(src)
              .filter(F.col('priority_rank') <= F.lit(limit))
              .withColumn('reason', F.concat(
                  F.lit('Demand spike '), F.col('demand_spike_pct').cast('string'), F.lit('%, CVR '),
                  F.col('cvr_pct').cast('string'), F.lit('%, ROAS '), F.col('roas').cast('string'),
                  F.lit('x, stock cover '), F.col('stock_cover_hrs').cast('string'), F.lit(' hrs')
              ))
              .withColumn('keep_promoting_until_hour', F.expr('timestamp + INTERVAL 3 HOURS'))
              .select('priority_rank', 'sku', 'product_name', 'category', 'action_type', 'primary_channel_sequence',
                      'reason', 'suggested_budget_shift_pct', 'keep_promoting_until_hour')
              .withColumn('created_at', F.current_timestamp()))
        df.write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable(target)
        return df
