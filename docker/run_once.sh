#!/usr/bin/env bash
set -euo pipefail

export HOME=/home/rstudio
export PROJECT_DIR="${PROJECT_DIR:-$HOME/mco-drought-conus}"
export DATA_DIR="${DATA_DIR:-$HOME/mco-drought-conus-data}"

export CORES="${CORES:-12}"
export KEEP_TILES="${KEEP_TILES:-1}"
export CONUS_MASK="${CONUS_MASK:-1}"
export TILE_DX="${TILE_DX:-2}"
export TILE_DY="${TILE_DY:-2}"

# GridMET refresh controls (no merge)
export GRIDMET_REFRESH_YEARS="${GRIDMET_REFRESH_YEARS:-2}"
export GRIDMET_OVERWRITE_LAST="${GRIDMET_OVERWRITE_LAST:-1}"   # force delete+redownload
export START_YEAR="${START_YEAR:-1991}"

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

# Try to make the mount writable for the running user (best-effort).
chown -R rstudio:rstudio "$DATA_DIR" 2>/dev/null || true
chmod -R a+rwx "$DATA_DIR" 2>/dev/null || true

echo "=== $(date) — Refreshing last ${GRIDMET_REFRESH_YEARS} years of GridMET raws (pr, pet, vpd, tmmx — NO MERGE) ==="
Rscript - <<'RS'
suppressPackageStartupMessages({
  library(fs)
  library(terra)
})

terra::terraOptions(tempdir = Sys.getenv("TERRA_TEMP_DIR", unset = tempdir()))

source(file.path(Sys.getenv("PROJECT_DIR"), "R", "1_gridmet-cache.R"))

.ensure_recent_raw <- function(var) {
  n_refresh_years = as.integer(Sys.getenv("GRIDMET_REFRESH_YEARS", "2"))
  overwrite_last  = identical(Sys.getenv("GRIDMET_OVERWRITE_LAST", "0"), "1")

  cy  = as.integer(format(Sys.Date(), "%Y"))
  y0  = cy - n_refresh_years + 1L
  yrs = seq.int(y0, cy)

  message("GridMET ", var, ": refreshing years: ", paste(yrs, collapse = ", "),
          " (overwrite_last=", overwrite_last, ")")

  dirs = .gridmet_dirs(var)
  fs::dir_create(dirs$raw_dir)

  for (yy in yrs) {
    f = .gridmet_year_nc(var, yy)
    if (fs::file_exists(f) && overwrite_last) {
      message("Deleting raw: ", fs::path_expand(f))
      fs::file_delete(f)
    }
    gridmet_download_year(var, yy, overwrite = overwrite_last)

    if (!fs::file_exists(f) || fs::file_info(f)$size <= 0) {
      stop("Missing/empty raw file after download: ", fs::path_expand(f))
    }
  }

  invisible(TRUE)
}

.ensure_recent_raw("pr")
.ensure_recent_raw("pet")
.ensure_recent_raw("vpd")
.ensure_recent_raw("tmmx")
message("Recent raw GridMET refresh complete (no merged NetCDF created).")
RS

echo "=== $(date) — Running precipitation metrics ==="
Rscript "$PROJECT_DIR/R/2_precipitation-metrics.R"

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
