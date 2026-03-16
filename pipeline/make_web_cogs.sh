#!/usr/bin/env bash
# make_web_cogs.sh
# Convert conus_drought/ COGs to lightweight web-optimized COGs in conus_drought_web/.
# Rounds values to 2 decimal places, adds an overview pyramid, uses DEFLATE compression.
# Preserves band description and DATE_TIME metadata from source files.
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

# Find the Python that has osgeo (GDAL's Python, not necessarily bare python3)
GDAL_PYTHON="python3"
if ! "$GDAL_PYTHON" -c "from osgeo import gdal" 2>/dev/null; then
  # Use the same interpreter that gdal_calc.py uses
  _shebang="$(head -1 "$(command -v gdal_calc.py 2>/dev/null)" 2>/dev/null | sed 's/^#! *//')"
  if [ -n "$_shebang" ] && "$_shebang" -c "from osgeo import gdal" 2>/dev/null; then
    GDAL_PYTHON="$_shebang"
  fi
fi

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

  # 1 — Extract band description and DATE_TIME from source before conversion
  band_desc="$(gdalinfo "$src_tif" 2>/dev/null \
    | grep -m1 'Description =' | sed 's/.*Description = //' || true)"
  date_time="$(gdalinfo "$src_tif" 2>/dev/null \
    | grep -m1 'DATE_TIME=' | sed 's/.*DATE_TIME=//' || true)"

  # 2 — Normalize nodata: terra writes masked pixels as NaN (not -9999).
  #     gdalwarp with -srcnodata nan converts NaN/mask-band pixels → -9999 Float32,
  #     giving gdal_calc a clean, uniform nodata value to work with.
  tmp_norm="$TMPDIR/webcog_norm_$$_${base}"
  gdalwarp -q -srcnodata nan -dstnodata -9999 "$src_tif" "$tmp_norm"

  # 3 — Scale to integer (multiply by 100). Most metrics fit in Int16 (-327..327),
  #     but ancillary metrics can exceed that range:
  #       precip-mm:  up to ~16,000 mm at long timescales
  #       precip-pon: up to ~720% of normal
  #       precip-dev: -1800 to +3100 mm at long timescales
  #       vpd-pon:    up to ~356% of normal
  #     These use Int32 instead (-21M..21M range).
  if [[ "$base" == precip-mm_* || "$base" == precip-pon_* || "$base" == precip-dev_* || "$base" == vpd-pon_* ]]; then
    gdal_calc.py -A "$tmp_norm" \
      --outfile="$tmp_tif" \
      --calc="numpy.where(A==-9999, numpy.int32(-9999), numpy.round(A.astype(numpy.float64) * 100).astype(numpy.int32))" \
      --type=Int32 --NoDataValue=-9999 --overwrite --quiet
  else
    gdal_calc.py -A "$tmp_norm" \
      --outfile="$tmp_tif" \
      --calc="numpy.where(A==-9999, numpy.int16(-9999), numpy.round(A.astype(numpy.float64) * 100).astype(numpy.int16))" \
      --type=Int16 --NoDataValue=-9999 --overwrite --quiet
  fi
  rm -f "$tmp_norm"

  # 4 — Stamp band metadata on the intermediate GeoTIFF BEFORE COG conversion.
  #     COG files block in-place updates, so we set band description and
  #     DATE_TIME here. gdal_translate -of COG carries band metadata through.
  "$GDAL_PYTHON" -c "
from osgeo import gdal
gdal.UseExceptions()
ds = gdal.Open('${tmp_tif}', gdal.GA_Update)
band = ds.GetRasterBand(1)
if '${band_desc}': band.SetDescription('${band_desc}')
if '${date_time}': band.SetMetadataItem('DATE_TIME', '${date_time}')
ds.FlushCache()
ds = None
" 2>/dev/null || true

  # 5 — Write web COG (the COG driver auto-generates overviews)
  mo_args=(-mo "SCALE=0.01" -mo "OFFSET=0")
  [ -n "$date_time" ] && mo_args+=(-mo "data_date=$date_time")

  gdal_translate "$tmp_tif" "$dst_tif" \
    -of COG \
    -co COMPRESS=DEFLATE \
    -co PREDICTOR=2 \
    -co LEVEL=9 \
    -co OVERVIEW_COMPRESS=DEFLATE \
    -co OVERVIEW_PREDICTOR=2 \
    -co RESAMPLING=AVERAGE \
    "${mo_args[@]}" \
    -q

  rm -f "$tmp_tif"

  n_done=$(( n_done + 1 ))
  echo "  [${n_done}/${#tif_files[@]}] $base"
done

# Copy timestamp metadata files and manifest
for txt in "$SRC_DIR"/*_time.txt; do
  if [ -f "$txt" ]; then cp "$txt" "$WEB_DIR/"; fi
done
if [ -f "$SRC_DIR/manifest.csv" ]; then
  cp "$SRC_DIR/manifest.csv" "$WEB_DIR/manifest.csv"
fi

echo ""
echo "=== $(date) — Done. ${n_done} web COGs written to $WEB_DIR ==="
