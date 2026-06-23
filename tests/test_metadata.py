import json
from pathlib import Path


def test_table_registry_has_required_tables():
    registry = json.loads(Path('metadata/table_registry.json').read_text())
    for table in ['product_master', 'marketing_performance_hourly', 'stock_movement_hourly', 'order_transactions']:
        assert table in registry
        assert registry[table]['primary_keys']
        assert registry[table]['columns']
