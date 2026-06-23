import json
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_json(relative_path: str):
    return json.loads((project_root() / relative_path).read_text(encoding='utf-8'))


def table_registry():
    return load_json('metadata/table_registry.json')


def quality_rules():
    return load_json('metadata/quality_rules.json')


def pipeline_config():
    return load_json('metadata/pipeline_config.json')


def names(catalog: str, schema_prefix: str):
    return {
        'catalog': catalog,
        'bronze': f'{catalog}.{schema_prefix}_bronze',
        'silver': f'{catalog}.{schema_prefix}_silver',
        'gold': f'{catalog}.{schema_prefix}_gold',
        'ops': f'{catalog}.{schema_prefix}_ops',
        'raw_volume': f'/Volumes/{catalog}/{schema_prefix}_bronze/ecommerce_raw',
        'checkpoint_volume': f'/Volumes/{catalog}/{schema_prefix}_bronze/ecommerce_checkpoint',
        'schema_volume': f'/Volumes/{catalog}/{schema_prefix}_bronze/ecommerce_schema',
    }


def fq(schema: str, table: str) -> str:
    parts = schema.split('.')
    return '.'.join([f'`{p}`' for p in parts] + [f'`{table}`'])
