#!/usr/bin/env bash

set -uo pipefail


# ============================================================
# CONFIG
# ============================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PYTHON_SCRIPT="$PROJECT_DIR/tiki_production.py"

LOG_DIR="$PROJECT_DIR/logs"

mkdir -p "$LOG_DIR"


# ============================================================
# ARGUMENT
# ============================================================

LIMIT="${1:-5000}"


# ============================================================
# TIMESTAMP
# ============================================================

TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"


if [[ "$LIMIT" == "full" || "$LIMIT" == "none" ]]; then

    RUN_NAME="full"

else

    RUN_NAME="${LIMIT}_ids"

fi


LOG_FILE="$LOG_DIR/crawler_${RUN_NAME}_${TIMESTAMP}.log"


# ============================================================
# HEADER
# ============================================================

{
    echo "============================================================"
    echo "TIKI DATA INGESTION PIPELINE"
    echo "============================================================"

    echo "Started at: $(date '+%Y-%m-%d %H:%M:%S')"

    echo "Host: $(hostname)"

    echo "User: $(whoami)"

    echo "Project: $PROJECT_DIR"

    echo "Python: $(python3 --version 2>&1)"

    echo "Test limit: $LIMIT"

    echo "Log file: $LOG_FILE"

    echo "============================================================"
    echo

} | tee "$LOG_FILE"


# ============================================================
# RUN
# ============================================================

START_TIME=$(date +%s)


cd "$PROJECT_DIR"


TEST_LIMIT="$LIMIT" \
python3 -u "$PYTHON_SCRIPT" \
    2>&1 \
    | tee -a "$LOG_FILE"


# Save Python exit status instead of tee exit status
PIPE_STATUS=("${PIPESTATUS[@]}")

EXIT_CODE="${PIPE_STATUS[0]}"


# ============================================================
# FINISH
# ============================================================

END_TIME=$(date +%s)

ELAPSED=$((END_TIME - START_TIME))

HOURS=$((ELAPSED / 3600))

MINUTES=$(((ELAPSED % 3600) / 60))

SECONDS=$((ELAPSED % 60))


{
    echo
    echo "============================================================"
    echo "PIPELINE FINISHED"
    echo "============================================================"

    echo "Finished at: $(date '+%Y-%m-%d %H:%M:%S')"

    printf "Wall-clock runtime: %02d:%02d:%02d\n" \
        "$HOURS" \
        "$MINUTES" \
        "$SECONDS"

    echo "Exit code: $EXIT_CODE"

    echo "Log file: $LOG_FILE"

    echo "============================================================"

} | tee -a "$LOG_FILE"


exit "$EXIT_CODE"
