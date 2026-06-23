#!/usr/bin/env bash
set -euo pipefail
TARGET="${1:-dev}"
PROFILE="${2:-DEFAULT}"
databricks bundle validate -t "$TARGET" --profile "$PROFILE"
databricks bundle deploy -t "$TARGET" --profile "$PROFILE"
