#!/usr/bin/env bash
set -euo pipefail
TARGET="${1:-dev}"
PROFILE="${2:-DEFAULT}"
databricks bundle run ecommerce_hypermarketing_hourly_etl -t "$TARGET" --profile "$PROFILE"
