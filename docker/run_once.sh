#!/usr/bin/env bash
set -euo pipefail
PIPELINE_START=$SECONDS

export PROJECT_DIR="${PROJECT_DIR:-$HOME/mco-drought-conus}"
export DATA_DIR="${DATA_DIR:-$HOME/mco-drought-conus-data}"

export CORES="${CORES:-12}"
export KEEP_TILES="${KEEP_TILES:-0}"
export CONUS_MASK="${CONUS_MASK:-1}"
export TILE_DX="${TILE_DX:-2}"
export TILE_DY="${TILE_DY:-2}"

export START_YEAR="${START_YEAR:-1979}"

# Make temp dirs writable and keep terra/gdal scratch off /tmp if you want
export TMPDIR="${TMPDIR:-$DATA_DIR/tmp}"
export R_TEMP_DIR="${R_TEMP_DIR:-$DATA_DIR/tmp/R}"
export TERRA_TEMP_DIR="${TERRA_TEMP_DIR:-$DATA_DIR/tmp/terra}"

echo "=== $(date) — Preparing writable data dirs ==="
mkdir -p \
  "$DATA_DIR" \
  "$DATA_DIR/raw" \
  "$DATA_DIR/interim" \
  "$DATA_DIR/derived" \
  "$TMPDIR" \
  "$R_TEMP_DIR" \
  "$TERRA_TEMP_DIR"

# Fix permissions on existing dirs before trying to create new subdirs.
chown -R rstudio:rstudio "$DATA_DIR" 2>/dev/null || true
chmod -R a+rwx "$DATA_DIR" 2>/dev/null || true

for var in pr pet vpd tmmx; do
  mkdir -p "$DATA_DIR/interim/gridmet/$var/raw"
done

# ============================================================
# S3 CACHE RESTORE
# Pulls the GridMET raw + interim cache from S3 so subsequent
# Fargate runs skip re-downloading the full historical record.
# Only runs when AWS_BUCKET is set (local runs are unaffected).
# ============================================================
if [ -n "${AWS_BUCKET:-}" ]; then
  echo "=== $(date) — Restoring GridMET cache from s3://${AWS_BUCKET}/cache/ ==="
  aws s3 sync "s3://${AWS_BUCKET}/cache/raw/"     "$DATA_DIR/raw/"     --no-progress || true
  aws s3 sync "s3://${AWS_BUCKET}/cache/interim/" "$DATA_DIR/interim/" --no-progress || true
  echo "=== $(date) — Cache restore complete ==="
fi

echo "=== $(date) — Syncing GridMET cache (START_YEAR=${START_YEAR}) ==="
Rscript "$PROJECT_DIR/R/1_gridmet-cache.R"

echo "=== $(date) — Running precipitation metrics ==="
Rscript "$PROJECT_DIR/R/2_metrics-precip.R"

echo "=== $(date) — Running SPEI metrics ==="
Rscript "$PROJECT_DIR/R/3_metrics-spei.R"

echo "=== $(date) — Running EDDI metrics ==="
Rscript "$PROJECT_DIR/R/4_metrics-eddi.R"

echo "=== $(date) — Running VPD metrics ==="
Rscript "$PROJECT_DIR/R/5_metrics-vpd.R"

echo "=== $(date) — Running tmax metrics ==="
Rscript "$PROJECT_DIR/R/6_metrics-tmax.R"

echo "=== $(date) — All drought metrics complete (precip, SPEI, EDDI, VPD, tmax) ==="

# ============================================================
# TILE CLEANUP
# Deletes all derived metric folders EXCEPT conus_drought.
# Only runs when KEEP_TILES=0.
# ============================================================
if [ "${KEEP_TILES}" = "0" ]; then
  echo "=== $(date) — KEEP_TILES=0: removing intermediate tile folders ==="
  DERIVED_DIR="$DATA_DIR/derived"

  for dir in "$DERIVED_DIR"/*/; do
    folder="$(basename "$dir")"
    if [ "$folder" != "conus_drought" ]; then
      echo "  Removing: $dir"
      rm -rf "$dir"
    else
      echo "  Keeping:  $dir"
    fi
  done

  echo "=== $(date) — Tile cleanup complete ==="
else
  echo "=== $(date) — KEEP_TILES=1: intermediate tile folders retained ==="
fi

echo "=== $(date) — Converting COGs to web-optimized COGs ==="
bash "$PROJECT_DIR/docker/make_web_cogs.sh"

# ============================================================
# S3 CACHE SAVE
# Pushes updated raw + interim files back to S3 so the next
# Fargate run can restore them rather than re-downloading.
# ============================================================
if [ -n "${AWS_BUCKET:-}" ]; then
  echo "=== $(date) — Saving GridMET cache to s3://${AWS_BUCKET}/cache/ ==="
  aws s3 sync "$DATA_DIR/raw/"     "s3://${AWS_BUCKET}/cache/raw/"     --no-progress
  aws s3 sync "$DATA_DIR/interim/" "s3://${AWS_BUCKET}/cache/interim/" --no-progress
  echo "=== $(date) — Cache save complete ==="
fi

# ============================================================
# S3 SYNC — derived outputs
# Syncs derived/conus_drought/ to s3://$AWS_BUCKET/derived/conus_drought/
# Only runs when AWS_BUCKET is set (i.e. in the Fargate/CI environment).
# Local docker-compose runs skip this step automatically.
# ============================================================
if [ -n "${AWS_BUCKET:-}" ]; then
  echo "=== $(date) — Syncing outputs to s3://${AWS_BUCKET}/derived/conus_drought/ ==="
  aws s3 sync \
    "$DATA_DIR/derived/conus_drought/" \
    "s3://${AWS_BUCKET}/derived/conus_drought/" \
    --delete \
    --no-progress
  echo "=== $(date) — S3 sync complete ==="
else
  echo "=== $(date) — AWS_BUCKET not set; skipping S3 sync (local run) ==="
fi

PIPELINE_ELAPSED=$(( SECONDS - PIPELINE_START ))
echo "=== $(date) — Total pipeline wall time: $(( PIPELINE_ELAPSED / 60 ))m $(( PIPELINE_ELAPSED % 60 ))s ==="