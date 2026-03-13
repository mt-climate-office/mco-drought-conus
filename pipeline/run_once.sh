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

# Seed per-dataset dates from any manifest restored from S3.
# These serve as fallbacks when a metric is skipped (raw data unchanged).
_MANIFEST="$DATA_DIR/derived/conus_drought/manifest.csv"
PRECIP_DATE="$(grep '^precip,' "$_MANIFEST" 2>/dev/null | cut -d, -f2 | head -1 || echo "")"
SPEI_DATE="$(grep '^spei,'   "$_MANIFEST" 2>/dev/null | cut -d, -f2 | head -1 || echo "")"
EDDI_DATE="$(grep '^eddi,'   "$_MANIFEST" 2>/dev/null | cut -d, -f2 | head -1 || echo "")"
VPD_DATE="$(grep '^vpd,'     "$_MANIFEST" 2>/dev/null | cut -d, -f2 | head -1 || echo "")"
TMAX_DATE="$(grep '^tmax,'   "$_MANIFEST" 2>/dev/null | cut -d, -f2 | head -1 || echo "")"

# Copy a local directory to a temp staging dir with _YYYY-MM-DD stripped from
# .tif filenames, then sync the staging dir to S3 with --delete.
# Non-.tif files (e.g. manifest.csv) are copied unchanged.
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
# Uses the max known data date across all metric groups.
s3_sync_derived() {
  if [ -n "${AWS_BUCKET:-}" ]; then
    echo "=== $(date) — Syncing derived/conus_drought to S3 (latest/) ==="
    local _date=""
    for _d in "${PRECIP_DATE:-}" "${SPEI_DATE:-}" "${EDDI_DATE:-}" "${VPD_DATE:-}" "${TMAX_DATE:-}"; do
      [ -n "$_d" ] && [[ "$_d" > "$_date" ]] && _date="$_d"
    done
    # Fall back to scanning dated filenames if no metric dates tracked yet
    if [ -z "$_date" ]; then
      _date=$(find "$DATA_DIR/derived/conus_drought/" -maxdepth 1 \
        -name "*_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].tif" 2>/dev/null \
        | sort | tail -1 | xargs basename 2>/dev/null \
        | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || true)
    fi
    s3_sync_dateless \
      "$DATA_DIR/derived/conus_drought/" \
      "s3://${AWS_BUCKET}/derived/conus_drought/latest/" \
      "${_date:-}"
  fi
}

# Get the data date embedded in newly-written dated .tif files for a metric prefix.
# Dated files end with _YYYY-MM-DD.tif. Returns empty string if none found.
get_metric_date() {
  local prefix="$1"
  find "$DATA_DIR/derived/conus_drought/" -maxdepth 1 \
    -name "${prefix}_*_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].tif" \
    2>/dev/null | sort | tail -1 | xargs basename 2>/dev/null \
    | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || echo ""
}

# Remove stale (dateless or wrong-dated) .tif files for the given metric prefixes.
# Only runs when date is non-empty (i.e., the metric was recomputed this run).
# Dateless files are restored-from-S3 placeholders superseded by newly computed outputs.
# Wrong-dated files are outputs from a prior pipeline run for the same metric.
cleanup_metric_stale() {
  local dir="$1" date="$2"; shift 2
  [ -z "$date" ] && return
  for prefix in "$@"; do
    # Remove dateless versions (restored from latest/, no _YYYY-MM-DD suffix)
    find "$dir" -maxdepth 1 -name "${prefix}_*.tif" \
      ! -name "*_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].tif" \
      -delete 2>/dev/null || true
    # Remove dated versions from previous pipeline runs (different date)
    find "$dir" -maxdepth 1 \
      -name "${prefix}_*_[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].tif" \
      ! -name "*_${date}.tif" \
      -delete 2>/dev/null || true
  done
}

# Write manifest.txt to dir recording the data date for each dataset.
# The "updated" field is the latest date across all datasets.
# This file is the authoritative per-dataset manifest; latest-date.txt
# retains the "updated" value for backward compatibility.
write_manifest() {
  local dir="$1"
  local updated=""
  for _d in "${PRECIP_DATE:-}" "${SPEI_DATE:-}" "${EDDI_DATE:-}" "${VPD_DATE:-}" "${TMAX_DATE:-}"; do
    [ -n "$_d" ] && [[ "$_d" > "$updated" ]] && updated="$_d"
  done
  [ -z "$updated" ] && updated="$(date +%Y-%m-%d)"
  {
    printf 'dataset,date\n'
    printf 'updated,%s\n' "$updated"
    [ -n "${PRECIP_DATE:-}" ] && printf 'precip,%s\n' "$PRECIP_DATE"
    [ -n "${SPEI_DATE:-}"   ] && printf 'spei,%s\n'   "$SPEI_DATE"
    [ -n "${EDDI_DATE:-}"   ] && printf 'eddi,%s\n'   "$EDDI_DATE"
    [ -n "${VPD_DATE:-}"    ] && printf 'vpd,%s\n'    "$VPD_DATE"
    [ -n "${TMAX_DATE:-}"   ] && printf 'tmax,%s\n'   "$TMAX_DATE"
  } > "$dir/manifest.csv"
  echo "=== Manifest: updated=${updated} | precip=${PRECIP_DATE:-?} spei=${SPEI_DATE:-?} eddi=${EDDI_DATE:-?} vpd=${VPD_DATE:-?} tmax=${TMAX_DATE:-?} ==="
}

echo "=== $(date) — Syncing GridMET cache (START_YEAR=${START_YEAR}) ==="
Rscript "$PROJECT_DIR/R/1_gridmet-cache.R"

echo "=== $(date) — Running precipitation metrics ==="
Rscript "$PROJECT_DIR/R/2_metrics-precip.R"
_d=$(get_metric_date "spi"); [ -n "$_d" ] && PRECIP_DATE="$_d"
cleanup_metric_stale "$DATA_DIR/derived/conus_drought" "${PRECIP_DATE:-}" \
  "spi" "precip-pon" "precip-dev" "precip-pctile" "precip-mm"
s3_sync_derived

echo "=== $(date) — Running SPEI metrics ==="
Rscript "$PROJECT_DIR/R/3_metrics-spei.R"
_d=$(get_metric_date "spei"); [ -n "$_d" ] && SPEI_DATE="$_d"
cleanup_metric_stale "$DATA_DIR/derived/conus_drought" "${SPEI_DATE:-}" "spei"
s3_sync_derived

echo "=== $(date) — Running EDDI metrics ==="
Rscript "$PROJECT_DIR/R/4_metrics-eddi.R"
_d=$(get_metric_date "eddi"); [ -n "$_d" ] && EDDI_DATE="$_d"
cleanup_metric_stale "$DATA_DIR/derived/conus_drought" "${EDDI_DATE:-}" "eddi"
s3_sync_derived

echo "=== $(date) — Running VPD metrics ==="
Rscript "$PROJECT_DIR/R/5_metrics-vpd.R"
_d=$(get_metric_date "svpdi"); [ -n "$_d" ] && VPD_DATE="$_d"
cleanup_metric_stale "$DATA_DIR/derived/conus_drought" "${VPD_DATE:-}" \
  "svpdi" "vpd-pon" "vpd-dev" "vpd-pctile"
s3_sync_derived

echo "=== $(date) — Running tmax metrics ==="
Rscript "$PROJECT_DIR/R/6_metrics-tmax.R"
_d=$(get_metric_date "tmax-pctile"); [ -n "$_d" ] && TMAX_DATE="$_d"
cleanup_metric_stale "$DATA_DIR/derived/conus_drought" "${TMAX_DATE:-}" \
  "tmax-pctile" "tmax-dev"
s3_sync_derived

echo "=== $(date) — All drought metrics complete (precip, SPEI, EDDI, VPD, tmax) ==="

# Write per-dataset manifest into conus_drought/ so it is included in all
# subsequent syncs (dated archives, latest/, and web COG conversions).
write_manifest "$DATA_DIR/derived/conus_drought"

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
bash "$PROJECT_DIR/pipeline/make_web_cogs.sh"

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
# LOCAL LATEST DIRECTORIES
# Mirrors conus_drought/ and conus_drought_web/ into dateless
# "latest" siblings on local disk. Runs on every pipeline run
# (not gated on AWS_BUCKET) so local and S3 layouts stay in sync.
# ============================================================

# Determine the data date as the latest date across all datasets.
DATA_DATE="${PRECIP_DATE:-}"
for _d in "${SPEI_DATE:-}" "${EDDI_DATE:-}" "${VPD_DATE:-}" "${TMAX_DATE:-}"; do
  [ -n "$_d" ] && [[ "$_d" > "$DATA_DATE" ]] && DATA_DATE="$_d"
done
DATA_DATE="${DATA_DATE:-$(date +%Y-%m-%d)}"
echo "=== $(date) — Data date: ${DATA_DATE} ==="

# Copy a local directory to a local destination with _YYYY-MM-DD stripped from
# .tif filenames. Non-.tif files (e.g. manifest.csv) are copied unchanged.
# Writes latest-date.txt with the data date.
local_sync_dateless() {
  local src="$1" dst="$2" date_str="${3:-}"
  rm -rf "$dst"
  mkdir -p "$dst"
  for f in "$src"/*.tif; do
    [ -f "$f" ] || continue
    base="$(basename "$f")"
    dateless="$(echo "$base" | sed 's/_[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\.tif$/.tif/')"
    cp "$f" "$dst/$dateless"
  done
  for f in "$src"/*; do
    [ -f "$f" ] || continue
    [[ "$f" == *.tif ]] && continue
    cp "$f" "$dst/"
  done
  [ -n "$date_str" ] && echo "$date_str" > "$dst/latest-date.txt"
}

echo "=== $(date) — Updating local latest directories ==="
local_sync_dateless \
  "$DATA_DIR/derived/conus_drought" \
  "$DATA_DIR/derived/conus_drought_latest" \
  "$DATA_DATE"
local_sync_dateless \
  "$DATA_DIR/derived/conus_drought_web" \
  "$DATA_DIR/derived/conus_drought_web_latest" \
  "$DATA_DATE"

# ============================================================
# S3 SYNC — derived outputs
# Publishes to:
#   derived/conus_drought/{date}/    — date-stamped archive
#   derived/conus_drought/latest/    — operational copy (with --delete)
# and mirrored for conus_drought_web/.
#
# Archive date is the latest data date across all datasets.
# Per-dataset dates are recorded in manifest.csv within each directory.
# ============================================================
if [ -n "${AWS_BUCKET:-}" ]; then
  echo "=== $(date) — Archiving outputs to s3://${AWS_BUCKET}/derived/ ==="

  # conus_drought — date archive (includes manifest.csv with per-dataset dates)
  aws s3 sync \
    "$DATA_DIR/derived/conus_drought/" \
    "s3://${AWS_BUCKET}/derived/conus_drought/${DATA_DATE}/" \
    --no-progress

  # conus_drought — latest (dateless filenames; --delete removes stale files)
  s3_sync_dateless \
    "$DATA_DIR/derived/conus_drought/" \
    "s3://${AWS_BUCKET}/derived/conus_drought/latest/" \
    "$DATA_DATE"

  # conus_drought_web — date archive (includes manifest.csv)
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
