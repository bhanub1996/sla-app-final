#!/usr/bin/env bash
set -euo pipefail
CATALOG="${1:?catalog required}"
SCHEMA_PREFIX="${2:?schema_prefix required}"
DATA_DIR="${3:?local data dir required}"
PROFILE="${4:-DEFAULT}"
RAW_PATH="/Volumes/${CATALOG}/${SCHEMA_PREFIX}_bronze/ecommerce_raw"
databricks fs mkdirs "$RAW_PATH" --profile "$PROFILE"
databricks fs cp "$DATA_DIR" "$RAW_PATH" --recursive --overwrite --profile "$PROFILE"
echo "Uploaded $DATA_DIR to $RAW_PATH"
