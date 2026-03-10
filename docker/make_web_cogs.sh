#!/usr/bin/env bash
# make_web_cogs.sh
# Convert conus_drought/ COGs to lightweight web-optimized COGs in conus_drought_web/.
# Rounds values to 2 decimal places, adds an overview pyramid, uses DEFLATE compression.
# Raw files in conus_drought/ are never modified.
#
# Usage:
#   ./make_web_cogs.sh [SRC_DIR [WEB_DIR]]
#
# Defaults (matching pipeline conventions):
#   SRC_DIR = ${DATA_DIR:-$HOME/mco-drought-conus-data}/derived/conus_drought
#   WEB_DIR = ${DATA_DIR:-$HOME/mco-drought-conus-data}/derived/conus_drought_web

set -euo pipefail

DATA_DIR="${DATA_DIR:-$HOME/mco-drought-conus-data}"
SRC_DIR="${1:-$DATA_DIR/derived/conus_drought}"
WEB_DIR="${2:-$DATA_DIR/derived/conus_drought_web}"
TMPDIR="${TMPDIR:-/tmp}"

echo "=== $(date) — make_web_cogs.sh ==="
echo "    Source : $SRC_DIR"
echo "    Output : $WEB_DIR"

mkdir -p "$WEB_DIR"

shopt -s nullglob
tif_files=("$SRC_DIR"/*.tif)

if [ ${#tif_files[@]} -eq 0 ]; then
  echo "No .tif files found in $SRC_DIR — nothing to do."
  exit 0
fi

echo "    Files  : ${#tif_files[@]}"
echo ""

n_done=0
for src_tif in "${tif_files[@]}"; do
  base="$(basename "$src_tif")"
  tmp_tif="$TMPDIR/webcog_tmp_$$_${base}"
  dst_tif="$WEB_DIR/$base"

  # 1 — Normalize nodata: terra writes masked pixels as NaN (not -9999).
  #     gdalwarp with -srcnodata nan converts NaN/mask-band pixels → -9999 Float32,
  #     giving gdal_calc a clean, uniform nodata value to work with.
  tmp_norm="$TMPDIR/webcog_norm_$$_${base}"
  gdalwarp -q -srcnodata nan -dstnodata -9999 "$src_tif" "$tmp_norm"

  # 2 — Scale to Int16 (multiply by 100, nodata=-9999 fits in Int16 range).
  #     SCALE=0.01 metadata lets clients recover original float values automatically.
  gdal_calc.py -A "$tmp_norm" \
    --outfile="$tmp_tif" \
    --calc="numpy.where(A==-9999, numpy.int16(-9999), numpy.round(A.astype(numpy.float64) * 100).astype(numpy.int16))" \
    --type=Int16 --NoDataValue=-9999 --overwrite --quiet
  rm -f "$tmp_norm"

  # 3 — Build overview pyramid
  gdaladdo -r average \
    --config COMPRESS_OVERVIEW DEFLATE \
    --config PREDICTOR_OVERVIEW 2 \
    "$tmp_tif" 2 4 8 16 32 2>/dev/null

  # 4 — Write web COG with scale metadata so clients decode Int16 → float correctly
  gdal_translate "$tmp_tif" "$dst_tif" \
    -of COG \
    -co COMPRESS=DEFLATE \
    -co PREDICTOR=2 \
    -co ZLEVEL=9 \
    -co COPY_SRC_OVERVIEWS=YES \
    -co RESAMPLING=AVERAGE \
    -mo "SCALE=0.01" \
    -mo "OFFSET=0" \
    -q

  rm -f "$tmp_tif"

  n_done=$(( n_done + 1 ))
  echo "  [${n_done}/${#tif_files[@]}] $base"
done

# Copy timestamp metadata files
for txt in "$SRC_DIR"/*_time.txt; do
  if [ -f "$txt" ]; then cp "$txt" "$WEB_DIR/"; fi
done

echo ""
echo "=== $(date) — Done. ${n_done} web COGs written to $WEB_DIR ==="
