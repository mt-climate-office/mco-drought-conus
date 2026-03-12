#!/usr/bin/env bash
##############################################################
# make_latest.sh — Strip dates from conus_drought_web files
#                  to produce an S3-compatible "latest/" layout
#                  for local file server prototyping.
#
# Usage:
#   bash scripts/make_latest.sh
#   DATA_DIR=/path/to/data bash scripts/make_latest.sh
##############################################################
set -euo pipefail

DATA_DIR="${DATA_DIR:-$HOME/mco-drought-conus-data}"
SRC_DIR="$DATA_DIR/derived/conus_drought_web"
DST_DIR="$DATA_DIR/derived/conus_drought_web_latest"

if [ ! -d "$SRC_DIR" ]; then
  echo "ERROR: Source directory not found: $SRC_DIR" >&2
  exit 1
fi

count=$(find "$SRC_DIR" -maxdepth 1 -name "*.tif" 2>/dev/null | wc -l | tr -d ' ')
if [ "$count" -eq 0 ]; then
  echo "ERROR: No .tif files found in $SRC_DIR" >&2
  exit 1
fi

# Extract data date from the first .tif filename
DATA_DATE=$(find "$SRC_DIR" -maxdepth 1 -name "*.tif" | head -1 | xargs basename \
  | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || true)
DATA_DATE="${DATA_DATE:-$(date +%Y-%m-%d)}"

echo "Source:  $SRC_DIR"
echo "Output:  $DST_DIR"
echo "Date:    $DATA_DATE"
echo "Files:   $count"
echo ""

rm -rf "$DST_DIR"
mkdir -p "$DST_DIR"

# Copy .tif files with date stripped from filename
for f in "$SRC_DIR"/*.tif; do
  [ -f "$f" ] || continue
  base="$(basename "$f")"
  dateless="$(echo "$base" | sed 's/_[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\.tif$/.tif/')"
  cp "$f" "$DST_DIR/$dateless"
done

# Copy any non-.tif files unchanged
for f in "$SRC_DIR"/*; do
  [ -f "$f" ] || continue
  [[ "$f" == *.tif ]] && continue
  cp "$f" "$DST_DIR/"
done

# Write latest-date.txt sidecar (matches S3 layout)
echo "$DATA_DATE" > "$DST_DIR/latest-date.txt"

out_count=$(find "$DST_DIR" -maxdepth 1 -name "*.tif" | wc -l | tr -d ' ')
echo "Done — $out_count files written to $DST_DIR"
echo "Date sidecar: $DST_DIR/latest-date.txt"
