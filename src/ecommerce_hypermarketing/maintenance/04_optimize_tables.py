from ecommerce_hypermarketing.common.args import parse_args
from ecommerce_hypermarketing.common.config import names, fq, table_registry


def optimize_table(spark, table_name, zorder_cols=None):
    try:
        if zorder_cols:
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({', '.join(zorder_cols)})")
        else:
            spark.sql(f"OPTIMIZE {table_name}")
    except Exception as exc:
        print(f"WARN: OPTIMIZE failed for {table_name}: {exc}")


def main(spark, catalog, schema_prefix):
    ns = names(catalog, schema_prefix)
    registry = table_registry()
    for _, meta in registry.items():
        optimize_table(spark, fq(ns['silver'], meta['target']), meta.get('zorder_by'))
    for table in ['main_dashboard_kpis', 'product_pushnow_current', 'recommended_actions_current_hour', 'category_demand_lift_current', 'channel_budget_shift_current']:
        optimize_table(spark, fq(ns['gold'], table))


if __name__ == '__main__':
    args = parse_args()
    main(spark, args.catalog, args.schema_prefix)  # noqa: F821
