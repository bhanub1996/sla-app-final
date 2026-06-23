from pathlib import Path
from ecommerce_hypermarketing.common.args import parse_args
from ecommerce_hypermarketing.common.config import project_root


def execute_sql_file(spark, sql_text):
    for stmt in [s.strip() for s in sql_text.split(';') if s.strip()]:
        spark.sql(stmt)


def main(spark, catalog, schema_prefix):
    sql_path = project_root() / 'sql' / 'ddl' / '00_create_uc_objects.sql'
    sql_text = sql_path.read_text(encoding='utf-8').replace('${catalog}', catalog).replace('${schema_prefix}', schema_prefix)
    execute_sql_file(spark, sql_text)


if __name__ == '__main__':
    args = parse_args()
    main(spark, args.catalog, args.schema_prefix)  # noqa: F821
