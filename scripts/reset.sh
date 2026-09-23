#!/usr/bin/env bash

set -euo pipefail


PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"


echo "============================================================"
echo "RESET TIKI CRAWLER STATE"
echo "============================================================"

echo "Project: $PROJECT_DIR"

echo


rm -f \
    "$PROJECT_DIR/state/crawler.db"

rm -f \
    "$PROJECT_DIR/state/crawler.db-wal"

rm -f \
    "$PROJECT_DIR/state/crawler.db-shm"

rm -f \
    "$PROJECT_DIR/output"/products_*.json

rm -f \
    "$PROJECT_DIR/errors"/permanent_errors.json

rm -f \
    "$PROJECT_DIR/errors"/temporary_errors.json


echo "Crawler state removed."
echo
echo "Logs were NOT deleted."
echo
echo "Reset complete."
