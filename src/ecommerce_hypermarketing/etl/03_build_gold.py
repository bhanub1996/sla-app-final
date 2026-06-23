from uuid import uuid4
from pyspark.sql import functions as F
from ecommerce_hypermarketing.common.args import parse_args
from ecommerce_hypermarketing.common.config import names, fq
from ecommerce_hypermarketing.common.audit import audit
from ecommerce_hypermarketing.agents.push_now_agent import PushNowAgent
from ecommerce_hypermarketing.agents.action_center_agent import ActionCenterAgent
from ecommerce_hypermarketing.agents.stock_guardrail_agent import StockGuardrailAgent
from ecommerce_hypermarketing.agents.budget_shift_agent import BudgetShiftAgent


def build_main_kpis(spark, catalog, schema_prefix):
    ns = names(catalog, schema_prefix)
    m = spark.table(fq(ns['silver'], 'fact_marketing_performance_hourly'))
    s = spark.table(fq(ns['silver'], 'fact_stock_movement_hourly'))
    latest_ts = m.agg(F.max('timestamp').alias('ts')).collect()[0]['ts']
    perf = m.filter(F.col('timestamp') == F.lit(latest_ts))
    stock = s.filter(F.col('timestamp') == F.lit(latest_ts))
    kpi = (perf.groupBy('timestamp', 'hour_key')
           .agg(F.sum('gross_revenue').alias('revenue_last_hour'),
                F.sum('orders').alias('orders_last_hour'),
                F.sum('units_sold').alias('units_last_hour'),
                F.sum('sessions').alias('sessions_last_hour'),
                (F.sum('orders') / F.sum('sessions') * F.lit(100)).alias('conversion_rate_pct'),
                (F.sum('gross_revenue') / F.sum('ad_spend')).alias('roas'),
                F.avg('demand_index').alias('demand_pulse_index')))
    stock_total = stock.groupBy('timestamp').agg(F.sum('closing_stock').alias('stock_on_hand_total'))
    result = (kpi.join(stock_total, 'timestamp', 'left')
              .select('timestamp', 'hour_key', 'revenue_last_hour', 'orders_last_hour', 'units_last_hour',
                      'sessions_last_hour', 'conversion_rate_pct', 'roas', 'stock_on_hand_total', 'demand_pulse_index')
              .withColumn('created_at', F.current_timestamp()))
    result.write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable(fq(ns['gold'], 'main_dashboard_kpis'))
    return result


def build_category_lift(spark, catalog, schema_prefix):
    ns = names(catalog, schema_prefix)
    src = spark.table(fq(ns['silver'], 'fact_category_performance_hourly'))
    latest_ts = src.agg(F.max('timestamp').alias('ts')).collect()[0]['ts']
    result = (src.filter(F.col('timestamp') == F.lit(latest_ts))
              .select('timestamp', 'category', F.col('avg_demand_spike_pct').alias('demand_lift_pct'), 'avg_push_score', 'cvr_pct', 'roas')
              .withColumn('created_at', F.current_timestamp()))
    result.write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable(fq(ns['gold'], 'category_demand_lift_current'))
    return result


def build_gold(spark, catalog, schema_prefix, run_id, stock_low_hrs, min_push_roas, min_push_cvr):
    build_main_kpis(spark, catalog, schema_prefix)
    build_category_lift(spark, catalog, schema_prefix)
    push_df = PushNowAgent(spark, catalog, schema_prefix, stock_low_hrs, min_push_roas, min_push_cvr).build()
    ActionCenterAgent(spark, catalog, schema_prefix).build()
    BudgetShiftAgent(spark, catalog, schema_prefix).build()
    StockGuardrailAgent(spark, catalog, schema_prefix, stock_low_hrs).build_view()
    audit(spark, catalog, schema_prefix, run_id, 'build_gold', 'gold_outputs', 'SUCCESS', push_df.count(), None)


if __name__ == '__main__':
    args = parse_args()
    build_gold(spark, args.catalog, args.schema_prefix, args.run_id or str(uuid4()), args.stock_low_hrs, args.min_push_roas, args.min_push_cvr)  # noqa: F821
