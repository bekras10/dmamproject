#!/usr/bin/env bash
# run_pipeline.sh — Master orchestration for the soccer analytics pipeline.
#
# Runs the full ETL sequence:  init_db → scrape → clean → test
# Any extra CLI args (e.g. --inspect, --no-cache) are forwarded to scrape.py.
#
# Usage:
#   ./run_pipeline.sh               # Full pipeline
#   ./run_pipeline.sh --inspect     # Quick test (1 league x 1 season)
#   ./run_pipeline.sh --no-cache    # Force re-download

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p data/raw logs

# Activate virtual environment if present
if [ -f "venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source venv/bin/activate
elif [ -f ".venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi

echo "=========================================="
echo " PIPELINE START  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

echo ""
echo "[1/5] Initializing database..."
python3 init_db.py

echo ""
echo "[2/5] Scraping FBRef data..."
python3 scrape.py "$@"

echo ""
echo "[3/5] Cleaning and loading data..."
python3 clean.py

echo ""
echo "[4/5] Validating data integrity..."
python3 test_db.py || echo "WARNING: Some integrity checks failed — see logs/test.log"

echo ""
echo "[5/5] Running analysis..."
python3 analysis.py

echo ""
echo "=========================================="
echo " PIPELINE COMPLETE  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
