from pyspark.sql import Window, functions as F
from ecommerce_hypermarketing.common.config import names, fq


class PushNowAgent:
    def __init__(self, spark, catalog, schema_prefix, stock_low_hrs=12.0, min_push_roas=5.0, min_push_cvr=4.5, margin_threshold=45.0):
        self.spark = spark
        self.catalog = catalog
        self.schema_prefix = schema_prefix
        self.stock_low_hrs = stock_low_hrs
        self.min_push_roas = min_push_roas
        self.min_push_cvr = min_push_cvr
        self.margin_threshold = margin_threshold

    def build(self):
        ns = names(self.catalog, self.schema_prefix)
        m = self.spark.table(fq(ns['silver'], 'fact_marketing_performance_hourly'))
        s = self.spark.table(fq(ns['silver'], 'fact_stock_movement_hourly')).select('hour_key', 'sku', 'closing_stock', 'stock_cover_hrs', 'stock_risk_flag')
        p = self.spark.table(fq(ns['silver'], 'dim_product')).select('sku', 'margin_pct', 'unit_price', 'brand')
        latest_ts = m.agg(F.max('timestamp').alias('ts')).collect()[0]['ts']
        df = (m.filter(F.col('timestamp') == F.lit(latest_ts))
                .join(s, ['hour_key', 'sku'], 'left')
                .join(p, ['sku'], 'left'))
        w = Window.orderBy(F.desc('push_score'), F.desc('roas'), F.desc('cvr_pct'))
        result = (df.withColumn('decision',
                    F.when(F.col('stock_cover_hrs') < F.lit(self.stock_low_hrs), F.lit('PROMOTE_CAREFULLY_LOW_STOCK'))
                     .when((F.col('roas') >= F.lit(self.min_push_roas)) & (F.col('cvr_pct') >= F.lit(self.min_push_cvr)), F.lit('PUSH_NOW_PAID_AND_OWNED'))
                     .when(F.col('margin_pct') >= F.lit(self.margin_threshold), F.lit('PUSH_HIGH_INTENT_PROFIT'))
                     .otherwise(F.lit('SECONDARY_PUSH')))
                  .withColumn('action_type',
                    F.when(F.col('decision') == 'PUSH_NOW_PAID_AND_OWNED', F.lit('Push Now'))
                     .when(F.col('decision') == 'PROMOTE_CAREFULLY_LOW_STOCK', F.lit('Guarded Push'))
                     .otherwise(F.lit('Secondary Push')))
                  .withColumn('primary_channel_sequence',
                    F.when(F.col('action_type') == 'Push Now', F.lit('Homepage Tile → App Push → Search Boost → Retargeting'))
                     .when(F.col('action_type') == 'Guarded Push', F.lit('Retargeting Only → Frequency Cap → Await Restock'))
                     .otherwise(F.lit('Category Placement → Search Boost → Retargeting')))
                  .withColumn('suggested_budget_shift_pct',
                    F.when(F.col('action_type') == 'Push Now', F.lit(12.0))
                     .when(F.col('action_type') == 'Secondary Push', F.lit(5.0))
                     .otherwise(F.lit(0.0)))
                  .withColumn('priority_rank', F.row_number().over(w))
                  .withColumn('created_at', F.current_timestamp()))
        target = fq(ns['gold'], 'product_pushnow_current')
        result.write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable(target)
        return result
