from delta.tables import DeltaTable
from pyspark.sql import functions as F


def table_exists(spark, table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def merge_upsert(spark, df, target_table: str, keys):
    if not table_exists(spark, target_table):
        df.write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable(target_table)
        return df.count()
    condition = ' AND '.join([f't.`{k}` = s.`{k}`' for k in keys])
    delta = DeltaTable.forName(spark, target_table)
    (delta.alias('t')
          .merge(df.alias('s'), condition)
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute())
    return df.count()


def add_metadata(df, source_name: str):
    return (df.withColumn('_source_name', F.lit(source_name))
              .withColumn('_ingested_at', F.current_timestamp())
              .withColumn('_input_file_name', F.input_file_name()))
