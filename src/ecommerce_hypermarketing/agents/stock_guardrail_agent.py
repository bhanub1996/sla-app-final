from pyspark.sql import functions as F
from ecommerce_hypermarketing.common.config import names, fq


class StockGuardrailAgent:
    def __init__(self, spark, catalog, schema_prefix, stock_low_hrs=12.0):
        self.spark = spark
        self.catalog = catalog
        self.schema_prefix = schema_prefix
        self.stock_low_hrs = stock_low_hrs

    def build_view(self):
        ns = names(self.catalog, self.schema_prefix)
        table = fq(ns['gold'], 'product_pushnow_current')
        df = (self.spark.table(table)
              .filter(F.col('stock_cover_hrs') < F.lit(self.stock_low_hrs))
              .select('sku', 'product_name', 'category', 'closing_stock', 'stock_cover_hrs', 'stock_risk_flag', 'action_type'))
        df.createOrReplaceTempView('low_stock_throttle_tmp')
        view_name = fq(ns['gold'], 'vw_low_stock_throttle_list')
        self.spark.sql(f'CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM low_stock_throttle_tmp')
        return view_name
