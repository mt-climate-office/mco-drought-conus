#!/usr/bin/env bash
# Quick-test script: runs a minimal subset of the pipeline for fast validation.
# Works both inside Docker and locally.
set -euo pipefail

# ---- Help --------------------------------------------------------------------
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'USAGE'
Usage: ./docker/run_test.sh

Environment variables (all optional):
  METRIC        Which metric to run: precip, spei, eddi, vpd, tmax, all
                (default: precip)
  TIMESCALES    Comma-separated timescales (default: 30)
  TILE_IDS      Comma-separated tile IDs (default: 1,2,3)
  CORES         Parallel workers (default: 2)
  CLIM_PERIODS  Climatology spec (default: rolling:30)
  SKIP_CACHE    Set to 1 to skip GridMET download (default: 0)

Examples:
  # Minimal local test (precip, 30-day, 3 tiles):
  ./docker/run_test.sh

  # Test SPEI with more timescales:
  METRIC=spei TIMESCALES=30,60,90 ./docker/run_test.sh

  # Test all metrics, 1 tile:
  METRIC=all TILE_IDS=50 ./docker/run_test.sh

  # Inside Docker:
  docker compose run --rm mco-drought bash docker/run_test.sh
USAGE
  exit 0
fi

# ---- Defaults ----------------------------------------------------------------
TEST_START=$SECONDS

export PROJECT_DIR="${PROJECT_DIR:-$HOME/mco-drought-conus}"
export DATA_DIR="${DATA_DIR:-$HOME/mco-drought-conus-data}"

export METRIC="${METRIC:-precip}"
export TIMESCALES="${TIMESCALES:-30}"
if [ -z "${TILE_IDS+x}" ]; then
  export TILE_IDS="30,31,32"
else
  export TILE_IDS
fi
export CORES="${CORES:-2}"
export CLIM_PERIODS="${CLIM_PERIODS:-rolling:30}"
export KEEP_TILES="${KEEP_TILES:-0}"
export CONUS_MASK="${CONUS_MASK:-0}"

SKIP_CACHE="${SKIP_CACHE:-0}"

# Disable S3 sync for local testing
unset AWS_BUCKET 2>/dev/null || true

export TMPDIR="${TMPDIR:-$DATA_DIR/tmp}"
export R_TEMP_DIR="${R_TEMP_DIR:-$DATA_DIR/tmp/R}"
export TERRA_TEMP_DIR="${TERRA_TEMP_DIR:-$DATA_DIR/tmp/terra}"

echo "=== Quick Test Configuration ==="
echo "  METRIC:       $METRIC"
echo "  TIMESCALES:   $TIMESCALES"
echo "  TILE_IDS:     $TILE_IDS"
echo "  CORES:        $CORES"
echo "  CLIM_PERIODS: $CLIM_PERIODS"
echo "  SKIP_CACHE:   $SKIP_CACHE"
echo "  PROJECT_DIR:  $PROJECT_DIR"
echo "  DATA_DIR:     $DATA_DIR"
echo ""

# ---- Setup dirs --------------------------------------------------------------
mkdir -p "$DATA_DIR/raw" "$DATA_DIR/interim" "$DATA_DIR/derived" \
         "$TMPDIR" "$R_TEMP_DIR" "$TERRA_TEMP_DIR"

for var in pr pet vpd tmmx; do
  mkdir -p "$DATA_DIR/raw/$var"
done

# ---- Cache phase -------------------------------------------------------------
if [[ "$SKIP_CACHE" == "1" ]]; then
  echo "=== $(date) — Skipping GridMET cache (SKIP_CACHE=1) ==="
else
  CACHE_START=$SECONDS
  echo "=== $(date) — Syncing GridMET cache ==="
  Rscript "$PROJECT_DIR/R/1_gridmet-cache.R"
  echo "  Cache elapsed: $(( SECONDS - CACHE_START ))s"
fi

# ---- Metric script lookup -----------------------------------------------------
script_for_metric() {
  case "$1" in
    precip) echo "$PROJECT_DIR/R/2_metrics-precip.R" ;;
    spei)   echo "$PROJECT_DIR/R/3_metrics-spei.R" ;;
    eddi)   echo "$PROJECT_DIR/R/4_metrics-eddi.R" ;;
    vpd)    echo "$PROJECT_DIR/R/5_metrics-vpd.R" ;;
    tmax)   echo "$PROJECT_DIR/R/6_metrics-tmax.R" ;;
    *)      echo "" ;;
  esac
}

if [ "$METRIC" = "all" ]; then
  METRICS_TO_RUN="precip spei eddi vpd tmax"
else
  METRICS_TO_RUN="$METRIC"
fi

# ---- Run metrics -------------------------------------------------------------
for m in $METRICS_TO_RUN; do
  script="$(script_for_metric "$m")"
  if [ -z "$script" ]; then
    echo "ERROR: Unknown metric '$m'. Valid: precip, spei, eddi, vpd, tmax, all"
    exit 1
  fi
  METRIC_START=$SECONDS
  echo "=== $(date) — Running $m metrics ==="
  Rscript "$script"
  echo "  $m elapsed: $(( SECONDS - METRIC_START ))s"
done

# ---- Summary -----------------------------------------------------------------
echo ""
echo "=== Output Summary ==="
CONUS_DIR="$DATA_DIR/derived/conus_drought"
if [[ -d "$CONUS_DIR" ]]; then
  FILE_COUNT=$(find "$CONUS_DIR" -name "*.tif" 2>/dev/null | wc -l | tr -d ' ')
  echo "  COG files in conus_drought/: $FILE_COUNT"
  if [[ "$FILE_COUNT" -gt 0 ]]; then
    find "$CONUS_DIR" -name "*.tif" -exec basename {} \; | sort | head -20
    if [[ "$FILE_COUNT" -gt 20 ]]; then
      echo "  ... and $(( FILE_COUNT - 20 )) more"
    fi
  fi
else
  echo "  No output directory found at $CONUS_DIR"
fi

TOTAL_ELAPSED=$(( SECONDS - TEST_START ))
echo ""
echo "=== Quick test complete: ${TOTAL_ELAPSED}s ($(( TOTAL_ELAPSED / 60 ))m $(( TOTAL_ELAPSED % 60 ))s) ==="
