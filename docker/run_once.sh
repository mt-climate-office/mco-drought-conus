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
chown -R mco-drought:mco-drought "$DATA_DIR" 2>/dev/null || true
chmod -R a+rwx "$DATA_DIR" 2>/dev/null || true

for var in pr pet vpd tmmx; do
  mkdir -p "$DATA_DIR/raw/$var"
done

# ============================================================
# S3 RAW RESTORE
# Pulls the GridMET raw files from S3 so subsequent
# Fargate runs skip re-downloading the full historical record.
# Only runs when AWS_BUCKET is set (local runs are unaffected).
#
# NOTE: aws s3 sync does not preserve file timestamps — restored
# files get mtime = now, which would break curl -z (If-Modified-Since)
# and the make-style dependency checks in the metrics scripts.
# s3_restore_timestamps() re-applies the S3 LastModified time to each
# local file after sync so the mtime-based logic works correctly.
# ============================================================
s3_restore_timestamps() {
  local bucket="$1" s3_prefix="$2" local_dir="$3"
  aws s3 ls --recursive "s3://${bucket}/${s3_prefix}" | \
    while read -r date time _size key; do
      local_file="${local_dir}${key#${s3_prefix}}"
      if [ -f "$local_file" ]; then touch -d "${date} ${time} UTC" "$local_file"; fi
    done
}

if [ -n "${AWS_BUCKET:-}" ]; then
  echo "=== $(date) — Syncing raw GridMET files from S3 ==="
  aws s3 sync "s3://${AWS_BUCKET}/raw/" "$DATA_DIR/raw/" --no-progress || true
  echo "=== $(date) — Restoring raw file timestamps ==="
  s3_restore_timestamps "${AWS_BUCKET}" "raw/" "$DATA_DIR/raw/" || true

  # Restore from latest/ so freshness checks see previously-completed outputs.
  echo "=== $(date) — Syncing derived outputs from S3 (latest/) ==="
  mkdir -p "$DATA_DIR/derived/conus_drought"
  aws s3 sync "s3://${AWS_BUCKET}/derived/conus_drought/latest/" "$DATA_DIR/derived/conus_drought/" --no-progress || true
  echo "=== $(date) — Restoring derived file timestamps ==="
  s3_restore_timestamps "${AWS_BUCKET}" "derived/conus_drought/latest/" "$DATA_DIR/derived/conus_drought/" || true

  echo "=== $(date) — Cache restore complete ==="
fi

# Copy a local directory to a temp staging dir with _YYYY-MM-DD stripped from
# .tif filenames, then sync the staging dir to S3 with --delete.
# Non-.tif files (e.g. _time.txt) are copied unchanged.
# Optional third argument: data date string written to latest-date.txt.
s3_sync_dateless() {
  local local_dir="$1" s3_dest="$2" date_str="${3:-}"
  local stage
  stage="$(mktemp -d)"
  for f in "$local_dir"/*.tif; do
    [ -f "$f" ] || continue
    base="$(basename "$f")"
    dateless="$(echo "$base" | sed 's/_[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\.tif$/.tif/')"
    cp "$f" "$stage/$dateless"
  done
  for f in "$local_dir"/*; do
    [ -f "$f" ] || continue
    [[ "$f" == *.tif ]] && continue
    cp "$f" "$stage/"
  done
  if [ -n "$date_str" ]; then
    echo "$date_str" > "$stage/latest-date.txt"
  fi
  aws s3 sync "$stage/" "$s3_dest" --delete --no-progress || true
  rm -rf "$stage"
}

# Intermediate sync after each metric script — keeps latest/ current so a
# failed run can resume without reprocessing completed datasets.
s3_sync_derived() {
  if [ -n "${AWS_BUCKET:-}" ]; then
    echo "=== $(date) — Syncing derived/conus_drought to S3 (latest/) ==="
    local _date
    _date=$(find "$DATA_DIR/derived/conus_drought/" -name "*.tif" 2>/dev/null \
      | head -1 | xargs basename 2>/dev/null \
      | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || true)
    s3_sync_dateless \
      "$DATA_DIR/derived/conus_drought/" \
      "s3://${AWS_BUCKET}/derived/conus_drought/latest/" \
      "${_date:-}"
  fi
}

echo "=== $(date) — Syncing GridMET cache (START_YEAR=${START_YEAR}) ==="
Rscript "$PROJECT_DIR/R/1_gridmet-cache.R"

echo "=== $(date) — Running precipitation metrics ==="
Rscript "$PROJECT_DIR/R/2_metrics-precip.R"
s3_sync_derived

echo "=== $(date) — Running SPEI metrics ==="
Rscript "$PROJECT_DIR/R/3_metrics-spei.R"
s3_sync_derived

echo "=== $(date) — Running EDDI metrics ==="
Rscript "$PROJECT_DIR/R/4_metrics-eddi.R"
s3_sync_derived

echo "=== $(date) — Running VPD metrics ==="
Rscript "$PROJECT_DIR/R/5_metrics-vpd.R"
s3_sync_derived

echo "=== $(date) — Running tmax metrics ==="
Rscript "$PROJECT_DIR/R/6_metrics-tmax.R"
s3_sync_derived

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
# S3 RAW SAVE
# Pushes updated raw GridMET files back to S3 so the next
# Fargate run can restore them rather than re-downloading.
# ============================================================
if [ -n "${AWS_BUCKET:-}" ]; then
  echo "=== $(date) — Saving raw GridMET files to s3://${AWS_BUCKET}/raw/ ==="
  aws s3 sync "$DATA_DIR/raw/" "s3://${AWS_BUCKET}/raw/" --no-progress
  echo "=== $(date) — Raw GridMET save complete ==="
fi

# ============================================================
# S3 SYNC — derived outputs
# Publishes to:
#   derived/conus_drought/{date}/    — date-stamped archive
#   derived/conus_drought/latest/    — operational copy (with --delete)
# and mirrored for conus_drought_web/.
#
# Data date is read from the _time.txt files the R scripts write
# alongside each mosaic; falls back to today's date.
# ============================================================
if [ -n "${AWS_BUCKET:-}" ]; then
  # Determine the data date from the date embedded in output filenames,
  # e.g. spi_15d_rolling-30_2026-03-10.tif → 2026-03-10.
  DATA_DATE=$(find "$DATA_DIR/derived/conus_drought/" -name "*.tif" 2>/dev/null \
    | head -1 | xargs basename 2>/dev/null \
    | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || true)
  DATA_DATE="${DATA_DATE:-$(date +%Y-%m-%d)}"
  echo "=== $(date) — Data date: ${DATA_DATE} ==="

  # Remove any .tif files from prior runs (different date) so that latest/
  # stays clean — only the current date's files get synced there.
  find "$DATA_DIR/derived/conus_drought/" -name "*.tif" \
    ! -name "*_${DATA_DATE}.tif" -delete 2>/dev/null || true
  find "$DATA_DIR/derived/conus_drought_web/" -name "*.tif" \
    ! -name "*_${DATA_DATE}.tif" -delete 2>/dev/null || true

  echo "=== $(date) — Archiving outputs to s3://${AWS_BUCKET}/derived/ ==="

  # conus_drought — date archive
  aws s3 sync \
    "$DATA_DIR/derived/conus_drought/" \
    "s3://${AWS_BUCKET}/derived/conus_drought/${DATA_DATE}/" \
    --no-progress

  # conus_drought — latest (dateless filenames; --delete removes stale files)
  s3_sync_dateless \
    "$DATA_DIR/derived/conus_drought/" \
    "s3://${AWS_BUCKET}/derived/conus_drought/latest/" \
    "$DATA_DATE"

  # conus_drought_web — date archive
  aws s3 sync \
    "$DATA_DIR/derived/conus_drought_web/" \
    "s3://${AWS_BUCKET}/derived/conus_drought_web/${DATA_DATE}/" \
    --no-progress

  # conus_drought_web — latest (dateless filenames)
  s3_sync_dateless \
    "$DATA_DIR/derived/conus_drought_web/" \
    "s3://${AWS_BUCKET}/derived/conus_drought_web/latest/" \
    "$DATA_DATE"

  echo "=== $(date) — S3 sync complete (date=${DATA_DATE}) ==="
else
  echo "=== $(date) — AWS_BUCKET not set; skipping S3 sync (local run) ==="
fi

PIPELINE_ELAPSED=$(( SECONDS - PIPELINE_START ))
echo "=== $(date) — Total pipeline wall time: $(( PIPELINE_ELAPSED / 60 ))m $(( PIPELINE_ELAPSED % 60 ))s ==="
