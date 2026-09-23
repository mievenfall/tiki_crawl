#!/usr/bin/env bash

set -u


PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"


echo "============================================================"
echo "TIKI PIPELINE STATUS"
echo "============================================================"

echo


echo "DATABASE"
echo "------------------------------------------------------------"

if [[ -f "$PROJECT_DIR/state/crawler.db" ]]; then

    ls -lh \
        "$PROJECT_DIR/state/crawler.db"

else

    echo "No crawler database."

fi


echo
echo "OUTPUT"
echo "------------------------------------------------------------"


PRODUCT_FILES=$(find \
    "$PROJECT_DIR/output" \
    -maxdepth 1 \
    -name 'products_*.json' \
    -type f \
    2>/dev/null \
    | wc -l)


echo "Product JSON files: $PRODUCT_FILES"


if [[ "$PRODUCT_FILES" -gt 0 ]]; then

    du -sh \
        "$PROJECT_DIR/output"

fi


echo
echo "ERROR FILES"
echo "------------------------------------------------------------"


if [[ -f "$PROJECT_DIR/errors/permanent_errors.json" ]]; then

    ls -lh \
        "$PROJECT_DIR/errors/permanent_errors.json"

else

    echo "No permanent error file."

fi


if [[ -f "$PROJECT_DIR/errors/temporary_errors.json" ]]; then

    ls -lh \
        "$PROJECT_DIR/errors/temporary_errors.json"

else

    echo "No temporary error file."

fi


echo
echo "LATEST LOG"
echo "------------------------------------------------------------"


LATEST_LOG=$(find \
    "$PROJECT_DIR/logs" \
    -maxdepth 1 \
    -name '*.log' \
    -type f \
    -printf '%T@ %p\n' \
    2>/dev/null \
    | sort -nr \
    | head -1 \
    | cut -d' ' -f2-)


if [[ -n "$LATEST_LOG" ]]; then

    echo "$LATEST_LOG"

    echo
    echo "Last 20 lines:"
    echo

    tail -n 20 \
        "$LATEST_LOG"

else

    echo "No logs found."

fi


echo
echo "============================================================"
