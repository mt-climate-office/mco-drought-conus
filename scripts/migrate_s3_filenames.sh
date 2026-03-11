#!/usr/bin/env bash
# migrate_s3_filenames.sh
#
# One-time migration to rename existing S3 objects to match the new conventions:
#   1. Reference period separator: rolling_30 → rolling-30, fixed_1991_2020 → fixed-1991-2020
#   2. latest/ folders: strip trailing _YYYY-MM-DD from .tif filenames
#
# Affected prefixes:
#   derived/conus_drought/           (root-level files, no date)
#   derived/conus_drought/latest/    (dated → dateless + slug fix)
#   derived/conus_drought/YYYY-MM-DD/ (dated archives, slug fix only)
#   derived/conus_drought_web/        (same structure)
#
# Usage:
#   AWS_BUCKET=mco-gridmet AWS_PROFILE=mco bash scripts/migrate_s3_filenames.sh
#
# Set DRY_RUN=1 to preview renames without making changes.

set -euo pipefail

BUCKET="${AWS_BUCKET:-mco-gridmet}"
PROFILE="${AWS_PROFILE:-mco}"
DRY_RUN="${DRY_RUN:-0}"

AWS="aws s3 --profile $PROFILE"
AWS_API="aws s3api --profile $PROFILE"

n_renamed=0
n_deleted=0
n_skipped=0

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Compute new filename for an S3 key.
# Args: $1 = full S3 key, $2 = 1 if this key is under a latest/ prefix
new_key() {
  local key="$1" is_latest="${2:-0}"
  local dir base new_base

  dir="$(dirname "$key")"
  base="$(basename "$key")"

  # Fix multi-word variable names: internal underscore → hyphen
  # (e.g. precip_pon → precip-pon, tmax_pctile → tmax-pctile)
  new_base="$base"
  new_base="$(echo "$new_base" | sed \
    -e 's/precip_pon_/precip-pon_/g' \
    -e 's/precip_dev_/precip-dev_/g' \
    -e 's/precip_pctile_/precip-pctile_/g' \
    -e 's/precip_in_/precip-in_/g' \
    -e 's/vpd_pon_/vpd-pon_/g' \
    -e 's/vpd_dev_/vpd-dev_/g' \
    -e 's/vpd_pctile_/vpd-pctile_/g' \
    -e 's/tmax_pctile_/tmax-pctile_/g')"

  # Fix reference period separator: rolling_N → rolling-N, fixed_Y_Y → fixed-Y-Y
  new_base="$(echo "$new_base" | sed 's/_rolling_\([0-9]*\)/_rolling-\1/g')"
  new_base="$(echo "$new_base" | sed 's/_fixed_\([0-9][0-9]*\)_\([0-9][0-9]*\)/_fixed-\1-\2/g')"

  # Strip trailing _YYYY-MM-DD for .tif files in latest/ prefixes
  if [ "$is_latest" = "1" ] && [[ "$new_base" == *.tif ]]; then
    new_base="$(echo "$new_base" | sed 's/_[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\.tif$/.tif/')"
  fi

  echo "$dir/$new_base"
}

# Rename one S3 object: copy to new key, delete old key (if key changed).
rename_object() {
  local old_key="$1" new_key_val="$2"

  if [ "$old_key" = "$new_key_val" ]; then
    n_skipped=$(( n_skipped + 1 ))
    return 0
  fi

  log "  $old_key"
  log "    → $new_key_val"

  if [ "$DRY_RUN" = "1" ]; then
    n_renamed=$(( n_renamed + 1 ))
    return 0
  fi

  # Copy with server-side copy (preserves metadata, no data transfer cost)
  $AWS_API copy-object \
    --copy-source "${BUCKET}/${old_key}" \
    --bucket "$BUCKET" \
    --key "$new_key_val" \
    --metadata-directive COPY \
    >/dev/null

  # Delete old object
  $AWS_API delete-object \
    --bucket "$BUCKET" \
    --key "$old_key" \
    >/dev/null

  n_renamed=$(( n_renamed + 1 ))
  n_deleted=$(( n_deleted + 1 ))
}

# Process all objects under a given S3 prefix.
# Args: $1 = prefix (without leading s3://bucket/), $2 = is_latest flag
process_prefix() {
  local prefix="$1" is_latest="${2:-0}"
  log "Processing prefix: s3://${BUCKET}/${prefix} (is_latest=${is_latest})"

  local keys
  keys=$(aws s3 ls "s3://${BUCKET}/${prefix}" --profile "$PROFILE" \
    | awk '{print $4}' \
    | grep -v '^$' || true)

  while IFS= read -r filename; do
    [ -n "$filename" ] || continue
    local old_key="${prefix}${filename}"
    local nk
    nk="$(new_key "$old_key" "$is_latest")"
    rename_object "$old_key" "$nk"
  done <<< "$keys"
}

# Process a date-stamped archive subfolder
process_dated_prefix() {
  local base_prefix="$1"
  log "Scanning for date-stamped subfolders under: s3://${BUCKET}/${base_prefix}"

  local folders
  folders=$(aws s3 ls "s3://${BUCKET}/${base_prefix}" --profile "$PROFILE" \
    | grep "PRE " \
    | awk '{print $2}' \
    | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}/$' \
    | grep -v '^$' || true)

  while IFS= read -r folder; do
    [ -n "$folder" ] || continue
    process_prefix "${base_prefix}${folder}" "0"
  done <<< "$folders"
}

echo "============================================================"
echo " MCO Drought CONUS — S3 filename migration"
echo " Bucket  : $BUCKET"
echo " Profile : $PROFILE"
echo " Dry run : $DRY_RUN"
echo "============================================================"
echo ""

# ---- conus_drought -----------------------------------------------------------
log "=== derived/conus_drought/ ==="

# Root-level files (no date, just slug fix)
process_prefix "derived/conus_drought/" "0"

# latest/ (slug fix + strip dates from .tif)
process_prefix "derived/conus_drought/latest/" "1"

# Date-stamped archive folders (slug fix only)
process_dated_prefix "derived/conus_drought/"

# ---- conus_drought_web -------------------------------------------------------
log "=== derived/conus_drought_web/ ==="

process_prefix "derived/conus_drought_web/" "0"
process_prefix "derived/conus_drought_web/latest/" "1"
process_dated_prefix "derived/conus_drought_web/"

# ---- Summary -----------------------------------------------------------------
echo ""
echo "============================================================"
if [ "$DRY_RUN" = "1" ]; then
  echo " DRY RUN — no changes made"
fi
echo " Objects renamed : $n_renamed"
echo " Objects skipped (no change needed): $n_skipped"
echo "============================================================"
