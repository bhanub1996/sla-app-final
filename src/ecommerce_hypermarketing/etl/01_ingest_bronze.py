from uuid import uuid4
from ecommerce_hypermarketing.common.args import parse_args
from ecommerce_hypermarketing.common.config import names, fq, table_registry
from ecommerce_hypermarketing.common.delta_utils import add_metadata
from ecommerce_hypermarketing.common.audit import audit


def read_source_batch(spark, source_path):
    return (spark.read.format('csv')
            .option('header', True)
            .option('inferSchema', True)
            .option('multiLine', True)
            .option('escape', '"')
            .load(source_path))


def ingest_batch(spark, catalog, schema_prefix, run_id):
    ns = names(catalog, schema_prefix)
    registry = table_registry()
    for source_name, meta in registry.items():
        started = None
        source_file = meta['source_file']
        # Supports both /volume/file.csv and /volume/source_name/*.csv patterns.
        file_path = f"{ns['raw_volume']}/{source_file}"
        folder_path = f"{ns['raw_volume']}/{source_name}"
        target = fq(ns['bronze'], f"raw_{source_name}")
        try:
            try:
                df = read_source_batch(spark, file_path)
            except Exception:
                df = read_source_batch(spark, folder_path)
            df = add_metadata(df, source_name)
            df.write.format('delta').mode('append').option('mergeSchema', True).saveAsTable(target)
            audit(spark, catalog, schema_prefix, run_id, 'ingest_bronze_batch', target, 'SUCCESS', df.count(), None, started)
        except Exception as exc:
            audit(spark, catalog, schema_prefix, run_id, 'ingest_bronze_batch', target, 'FAILED', None, str(exc), started)
            raise


def ingest_autoloader(spark, catalog, schema_prefix, run_id):
    ns = names(catalog, schema_prefix)
    registry = table_registry()
    for source_name, meta in registry.items():
        source_dir = f"{ns['raw_volume']}/{source_name}"
        target = fq(ns['bronze'], f"raw_{source_name}")
        checkpoint = f"{ns['checkpoint_volume']}/bronze/{source_name}"
        schema_location = f"{ns['schema_volume']}/bronze/{source_name}"
        df = (spark.readStream.format('cloudFiles')
              .option('cloudFiles.format', 'csv')
              .option('cloudFiles.schemaLocation', schema_location)
              .option('header', True)
              .option('inferColumnTypes', True)
              .load(source_dir))
        df = add_metadata(df, source_name)
        query = (df.writeStream.format('delta')
                 .option('checkpointLocation', checkpoint)
                 .option('mergeSchema', True)
                 .trigger(availableNow=True)
                 .toTable(target))
        query.awaitTermination()
        audit(spark, catalog, schema_prefix, run_id, 'ingest_bronze_autoloader', target, 'SUCCESS', None, 'availableNow completed')


if __name__ == '__main__':
    args = parse_args()
    run_id = args.run_id or str(uuid4())
    if args.mode == 'autoloader':
        ingest_autoloader(spark, args.catalog, args.schema_prefix, run_id)  # noqa: F821
    else:
        ingest_batch(spark, args.catalog, args.schema_prefix, run_id)  # noqa: F821
