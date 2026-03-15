##############################################################
# File: R/pipeline-common.R
# Title: Shared pipeline infrastructure for all metric scripts (2-6).
# Author: Dr. Zachary H. Hoylman
# Date: 3-12-2026
# Conventions: "=", |> , explicit pkg::fun namespaces.
#
# OVERVIEW
# --------
# This file is sourced by each metric script (2_metrics-precip.R through
# 6_metrics-tmax.R). It provides the entire pipeline framework so that
# individual metric scripts only need to define a config list and call
# run_metric_pipeline(config).
#
# The pipeline flow is:
#   1. Setup paths and directory structure (Section C)
#   2. Discover raw NetCDF files and extract date vectors (Section F)
#   3. Build a regular tile grid over the raster extent (Section D)
#   4. Parse climatology reference periods and timescales (Section E)
#   5. Dispatch parallel workers across tiles (Section L):
#      a. Each worker reads raw data for its tile (Section H)
#      b. Loops over climatology periods, calling the generic metric
#         computation engine (Section J) which:
#         - Builds an integrated matrix (pixels x reference years)
#         - Applies each metric spec via vectorized ops or per-pixel loops
#      c. Writes per-tile COG files (Section K)
#   6. Mosaic tile COGs into CONUS-wide outputs (Section K)
#   7. Report results and clean up
#
# PARALLEL STRATEGY
# -----------------
# mclapply(fork) is used, which is safe here because NO terra SpatRaster
# objects are passed across the fork boundary. Workers receive only plain R
# objects (file paths, date vectors, sf tiles) and open their own NetCDF
# handles + terra sessions independently.
#
# ENVIRONMENT VARIABLES
# ---------------------
# PROJECT_DIR    — path to the git repo (default: ~/mco-drought-conus)
# DATA_DIR       — path to the data directory (default: ~/mco-drought-conus-data)
# CORES          — number of parallel workers (default: 4, max: 12)
# TIMESCALES     — comma-separated day counts or "wy","ytd" (default: 15,30,...,wy,ytd)
# CLIM_PERIODS   — climatology spec (default: "rolling:30")
# TILE_IDS       — comma-separated tile IDs to process (default: all)
# TILE_DX/DY     — tile size in degrees (default: 2)
# CONUS_MASK     — clip tiles to CONUS boundary (default: 0)
# KEEP_TILES     — retain intermediate tile files after mosaic (default: 0)
# R_TEMP_DIR     — temp directory for R scratch files
# TERRA_TEMP_DIR — temp directory for terra scratch files
##############################################################

# ---- A. Core utilities -------------------------------------------------------

#' Null-coalescing operator. Returns `a` if non-NULL, otherwise `b`.
`%||%` = function(a, b) if (is.null(a)) b else a

#' Expand and resolve a path to an absolute canonical form.
.abs_path = function(p) as.character(fs::path_abs(fs::path_expand(p)))

suppressPackageStartupMessages({
  library(fs)
  library(sf)
  library(terra)
  library(purrr)
  library(readr)
  library(ncdf4)
  library(parallel)
  library(rnaturalearth)
  library(pbmcapply)
})

# ---- B. IO helpers -----------------------------------------------------------
# These handle directory creation, writability checks, atomic file writes,
# make-style freshness checks, and raster data validation.

#' Recursively create a directory if it doesn't exist. Returns the absolute path.
.dir_create_base = function(path) {
  path = .abs_path(path)
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

#' Check if a directory exists and is writable. Creates it if needed.
.is_writable_dir = function(path) {
  path = .abs_path(path)
  if (!dir.exists(path)) {
    ok = tryCatch({ .dir_create_base(path); TRUE }, error = function(e) FALSE)
    if (!ok) return(FALSE)
  }
  isTRUE(file.access(path, 2) == 0)
}

#' Assert a directory is writable; stop with a Docker-friendly error if not.
.ensure_writable_dir = function(path, label = "path") {
  path = .abs_path(path)
  .dir_create_base(path)
  if (!.is_writable_dir(path)) {
    stop("[EACCES] Not writable: ", label, " = ", path,
         "\nFix on host: ensure the bind-mounted folder is writable by Docker.",
         "\nExample: chmod -R u+rwX,g+rwX,o+rwX ", shQuote(path))
  }
  invisible(path)
}

#' Atomically move a file from src to dst. Writes go to a temp location first,
#' then this function copies to the final path and removes the temp. This avoids
#' partial/corrupt files if the process is interrupted mid-write.
.atomic_copy_into_place = function(src, dst) {
  src = .abs_path(src); dst = .abs_path(dst)
  .dir_create_base(fs::path_dir(dst))
  if (file.exists(dst)) try(unlink(dst), silent = TRUE)

  ok = file.copy(src, dst, overwrite = TRUE)
  if (!isTRUE(ok) || !file.exists(dst) || file.info(dst)$size <= 0) {
    stop("[EACCES/IO] Failed to copy '", src, "' -> '", dst, "'")
  }
  try(unlink(src), silent = TRUE)
  invisible(TRUE)
}

#' Make-style freshness check: returns TRUE if any raw input file is newer than
#' the oldest output file matching output_regexp. If no outputs exist, returns TRUE.
#' Used to skip recomputation when raw data hasn't changed.
.raw_newer_than_outputs = function(raw_files, output_dir, output_regexp) {
  raw_mtime = max(fs::file_info(as.character(raw_files))$modification_time, na.rm = TRUE)
  out_files = if (fs::dir_exists(output_dir))
    fs::dir_ls(output_dir, regexp = output_regexp, type = "file")
  else character(0)
  if (!length(out_files)) return(TRUE)
  out_mtime = min(fs::file_info(as.character(out_files))$modification_time, na.rm = TRUE)
  raw_mtime > out_mtime
}

#' Spot-check whether a SpatRaster contains any finite (non-NA, non-NaN) values.
#' Samples up to 2000 cells from the last layer for efficiency rather than
#' reading the entire raster into memory.
.has_any_data = function(r) {
  if (is.null(r)) return(FALSE)
  if (terra::nlyr(r) < 1) return(FALSE)
  n = terra::ncell(r)
  if (is.na(n) || n <= 0) return(FALSE)
  idx = unique(pmax(1, pmin(n, as.integer(seq(1, n, length.out = min(2000, n))))))
  v = terra::values(r[[terra::nlyr(r)]], cells = idx, mat = FALSE)
  any(is.finite(v))
}

# ---- C. Path setup -----------------------------------------------------------

#' Initialize the full directory tree for a pipeline run.
#'
#' Reads PROJECT_DIR and DATA_DIR from environment variables (with sensible
#' defaults), creates the directory hierarchy, validates writability, sets
#' terra's temp directory, and sources drought-functions.R into the global
#' environment so that metric functions (gamma_fit_spi, etc.) are available
#' to forked workers.
#'
#' Directory layout created under DATA_DIR:
#'   raw/              — raw GridMET NetCDF files (one subdir per variable)
#'   interim/          — intermediate products (currently unused)
#'   derived/
#'     <tiles_subdir>/ — per-tile COG files (e.g. precip_metrics/)
#'     conus_drought/  — final CONUS-wide mosaic COGs
#'   tmp/R/            — R scratch files
#'   tmp/terra/        — terra scratch files
#'
#' @param tiles_subdir Character. Subdirectory name under derived/ for tile
#'   output (e.g. "precip_metrics", "vpd_metrics").
#' @return Named list of absolute paths.
setup_pipeline_paths = function(tiles_subdir) {
  project_root = .abs_path(Sys.getenv("PROJECT_DIR", unset = "~/mco-drought-conus"))
  data_root    = .abs_path(Sys.getenv("DATA_DIR",    unset = "~/mco-drought-conus-data"))

  raw_dir     = .abs_path(fs::path(data_root, "raw"))
  interim_dir = .abs_path(fs::path(data_root, "interim"))
  derived_dir = .abs_path(fs::path(data_root, "derived"))
  tiles_root  = .abs_path(fs::path(derived_dir, tiles_subdir))
  conus_root  = .abs_path(fs::path(derived_dir, "conus_drought"))

  r_temp     = .abs_path(Sys.getenv("R_TEMP_DIR",     unset = fs::path(data_root, "tmp", "R")))
  terra_temp = .abs_path(Sys.getenv("TERRA_TEMP_DIR", unset = fs::path(data_root, "tmp", "terra")))

  .dir_create_base(data_root)
  .dir_create_base(fs::path(data_root, "tmp"))
  .dir_create_base(r_temp)
  .dir_create_base(terra_temp)

  .ensure_writable_dir(data_root,  "DATA_DIR")
  .ensure_writable_dir(r_temp,     "R_TEMP_DIR")
  .ensure_writable_dir(terra_temp, "TERRA_TEMP_DIR")

  terra::terraOptions(tempdir = terra_temp)

  .dir_create_base(raw_dir)
  .dir_create_base(interim_dir)
  .dir_create_base(derived_dir)
  .dir_create_base(tiles_root)
  .dir_create_base(conus_root)

  .ensure_writable_dir(derived_dir, "derived_dir")
  .ensure_writable_dir(tiles_root,  "tiles_root")
  .ensure_writable_dir(conus_root,  "conus_root")

  # Source drought functions into global environment
  df_path = .abs_path(fs::path(project_root, "R", "drought-functions.R"))
  if (!fs::file_exists(df_path)) stop("drought-functions.R not found at: ", df_path)
  source(df_path, local = globalenv())

  list(
    project_root = project_root, data_root = data_root,
    raw_dir = raw_dir, interim_dir = interim_dir, derived_dir = derived_dir,
    tiles_root = tiles_root, conus_root = conus_root,
    r_temp = r_temp, terra_temp = terra_temp
  )
}

# ---- D. Geometry & tiling ----------------------------------------------------

#' Build a dissolved CONUS Lower-48 boundary polygon from Natural Earth data.
#' Excludes Alaska, Hawaii, and Puerto Rico. Used to clip tiles when CONUS_MASK=1.
#' @return An sf object with a single multipolygon geometry in EPSG:4326.
conus_geometry = function() {
  st     = rnaturalearth::ne_states(country = "United States of America", returnclass = "sf")
  st_l48 = st[!(st$name %in% c("Alaska", "Hawaii", "Puerto Rico")), ]
  sf::st_union(sf::st_make_valid(st_l48)) |> sf::st_as_sf()
}

#' Divide a raster extent into a regular grid of rectangular tile polygons.
#'
#' Tile dimensions (dx, dy) are snapped to exact multiples of the raster
#' resolution so tile boundaries align with pixel edges. A small buffer
#' (EDGE_BUF_DEG, default 0.1 deg) is added around the extent to avoid
#' edge clipping. Tile IDs are numbered sequentially, column-major
#' (bottom-left to top-right).
#'
#' When CONUS_MASK=1 or CLIP_TO_CONUS=1, tiles are intersected with the
#' CONUS L48 boundary so that ocean-only tiles are dropped.
#'
#' @param dx,dy Tile dimensions in degrees (default: 2x2).
#' @param r_for_align A SpatRaster whose extent and resolution define the grid.
#' @param edge_buf_deg Optional buffer in degrees around the extent.
#' @return An sf data frame with columns: tile_id (integer), geometry (polygon).
build_tiles_from_extent = function(dx = 2, dy = 2, r_for_align, edge_buf_deg = NULL) {
  rs  = terra::res(r_for_align)
  ex  = terra::ext(r_for_align)

  buf = as.numeric(Sys.getenv("EDGE_BUF_DEG", "0.1"))
  if (!is.null(edge_buf_deg)) buf = edge_buf_deg
  exb = terra::ext(ex$xmin - buf, ex$xmax + buf, ex$ymin - buf, ex$ymax + buf)

  nx  = max(1L, round(dx / rs[1])); ny = max(1L, round(dy / rs[2]))
  dxA = nx * rs[1];                  dyA = ny * rs[2]

  xs = seq(exb$xmin, exb$xmax, by = dxA)
  if (tail(xs, 1) < exb$xmax - 1e-9) xs = c(xs, exb$xmax)
  ys = seq(exb$ymin, exb$ymax, by = dyA)
  if (tail(ys, 1) < exb$ymax - 1e-9) ys = c(ys, exb$ymax)

  eps   = 1e-6
  polys = vector("list", (length(xs) - 1) * (length(ys) - 1)); k = 0L
  for (i in seq_len(length(xs) - 1)) for (j in seq_len(length(ys) - 1)) {
    k = k + 1L
    xL = xs[i]     - eps; xR = xs[i + 1] + eps
    yB = ys[j]     - eps; yT = ys[j + 1] + eps
    polys[[k]] = sf::st_polygon(list(matrix(
      c(xL, yB, xR, yB, xR, yT, xL, yT, xL, yB), ncol = 2, byrow = TRUE
    )))
  }

  tiles = sf::st_sf(tile_id = seq_along(polys), geometry = sf::st_sfc(polys, crs = 4326))

  if (identical(Sys.getenv("CLIP_TO_CONUS", "0"), "1") ||
      identical(Sys.getenv("CONUS_MASK",    "0"), "1")) {
    mask  = conus_geometry()
    tiles = suppressWarnings(sf::st_intersection(sf::st_make_valid(tiles), mask)) |>
              sf::st_as_sf()
  }

  tiles
}

#' Subdivide a single tile sf polygon into 4 equal quadrants.
#'
#' Used for retry-after-failure: if a tile fails (e.g. OOM), splitting it into
#' smaller pieces reduces per-worker memory. Each sub-tile gets an ID like
#' "42_1", "42_2", etc. (parent ID + quadrant index).
#'
#' @param tile_sf Single-row sf object with tile_id and polygon geometry.
#' @return An sf data frame with 4 rows (quadrants), each with a sub-tile ID.
.subdivide_tile = function(tile_sf) {
  bb   = sf::st_bbox(tile_sf)
  xmid = (bb["xmin"] + bb["xmax"]) / 2
  ymid = (bb["ymin"] + bb["ymax"]) / 2
  eps  = 1e-6

  make_quad = function(xL, xR, yB, yT) {
    sf::st_polygon(list(matrix(
      c(xL, yB, xR, yB, xR, yT, xL, yT, xL, yB), ncol = 2, byrow = TRUE
    )))
  }

  quads = list(
    make_quad(bb["xmin"] - eps, xmid + eps, bb["ymin"] - eps, ymid + eps),  # SW
    make_quad(xmid - eps, bb["xmax"] + eps, bb["ymin"] - eps, ymid + eps),  # SE
    make_quad(bb["xmin"] - eps, xmid + eps, ymid - eps, bb["ymax"] + eps),  # NW
    make_quad(xmid - eps, bb["xmax"] + eps, ymid - eps, bb["ymax"] + eps)   # NE
  )

  parent_id = tile_sf$tile_id
  sf::st_sf(
    tile_id  = paste0(parent_id, "_", 1:4),
    geometry = sf::st_sfc(quads, crs = 4326)
  )
}

# ---- E. Climatology ----------------------------------------------------------

#' Parse the CLIM_PERIODS environment variable into a list of climatology specs.
#'
#' Supported formats:
#'   "rolling:30"        — most recent 30 years ending at the latest date
#'   "fixed:1991:2020"   — fixed reference period 1991-2020
#'   "full"              — use all available years
#'   "rolling:30,fixed:1991:2020" — multiple periods (comma-separated)
#'
#' Each spec is a list with: mode, years, start, end, slug (for file naming).
#' @param env_val Character string in the format above.
#' @return List of clim_spec lists.
parse_clim_periods = function(env_val = "rolling:30") {
  specs = strsplit(trimws(env_val), ",")[[1]]
  lapply(specs, function(s) {
    parts = strsplit(trimws(s), ":")[[1]]
    mode  = parts[1]
    switch(mode,
      rolling = list(mode = "rolling", years = as.integer(parts[2]),
                     start = NA_integer_, end = NA_integer_,
                     slug = paste0("rolling-", parts[2])),
      fixed   = list(mode = "fixed",   years = NA_integer_,
                     start = as.integer(parts[2]), end = as.integer(parts[3]),
                     slug = paste0("fixed-", parts[2], "-", parts[3])),
      full    = list(mode = "full",    years = NA_integer_,
                     start = NA_integer_, end = NA_integer_, slug = "full"),
      stop("Unknown CLIM_PERIODS mode: ", mode)
    )
  })
}

#' Find indices into the full date vector that correspond to the reference period.
#'
#' Identifies all dates that share the same month-day as the latest date in the
#' series (i.e. one per year), then filters to those falling within the
#' climatology reference period. For example, if the latest date is 2026-03-10
#' and clim_spec is rolling:30, this returns the indices for March 10 in each
#' of the years 1997-2026.
#'
#' @param dates Date vector (the full time series).
#' @param clim_spec A single clim_spec list from parse_clim_periods().
#' @return Integer vector of indices into `dates`.
ref_period_indices = function(dates, clim_spec) {
  d_last  = max(dates, na.rm = TRUE)
  md_last = format(d_last, "%m-%d")
  idx     = which(format(dates, "%m-%d") == md_last)
  yrs     = as.integer(format(dates[idx], "%Y"))
  keep = switch(clim_spec$mode,
    rolling = which(yrs >= (as.integer(format(d_last, "%Y")) - clim_spec$years + 1L)),
    fixed   = {
      if (is.na(clim_spec$start) || is.na(clim_spec$end))
        stop("fixed mode requires start and end years.")
      which(yrs >= clim_spec$start & yrs <= clim_spec$end)
    },
    full    = seq_along(yrs),
    stop("Unknown CLIM_PERIODS mode: ", clim_spec$mode)
  )
  idx[keep]
}

#' Build a list of contiguous index windows for a given timescale and reference period.
#'
#' For each anchor date (one per reference year, from ref_period_indices), creates
#' a vector of consecutive indices spanning n_days ending at that anchor. This
#' produces the "climatology slices" — e.g. for 30-day precip with rolling:30,
#' this returns 30 windows of 30 indices each (one per reference year).
#'
#' Windows that would start before the beginning of the time series are dropped.
#'
#' @param n_days Integer. Window length in days (the timescale).
#' @param dates Date vector (the full time series).
#' @param clim_spec A single clim_spec list.
#' @return List of integer vectors, each of length n_days.
build_slice_groups = function(n_days, dates, clim_spec) {
  anchors = ref_period_indices(dates, clim_spec)
  purrr::compact(purrr::map(anchors, function(end_i) {
    start_i = end_i - (n_days - 1)
    if (start_i < 1) return(NULL)
    seq(start_i, end_i)
  }))
}

#' Parse the TIMESCALES environment variable into day counts and display names.
#'
#' Supports numeric day counts (e.g. "30", "365") and two symbolic tokens:
#'   "wy"  — water year (days since Oct 1)
#'   "ytd" — year to date (days since Jan 1)
#'
#' The symbolic tokens are computed dynamically from the date vector.
#' Default: "15,30,45,60,90,120,180,365,730,wy,ytd"
#'
#' @param dates Date vector (needed to resolve wy/ytd to actual day counts).
#' @return List with $lengths (integer vector) and $names (character vector,
#'   e.g. "30d", "wy", "ytd").
standard_timescales_days = function(dates) {
  ts_env = Sys.getenv("TIMESCALES", unset = "15,30,45,60,90,120,180,365,730,wy,ytd")
  tokens = strsplit(trimws(ts_env), ",")[[1]]

  md = format(dates, "%m-%d")

  wy_idx  = which(md == "10-01")
  ytd_idx = which(md == "01-01")
  wy_len  = if (length(wy_idx))  (length(md) - tail(wy_idx, 1)) + 1 else NA_integer_
  ytd_len = if (length(ytd_idx)) (length(md) - tail(ytd_idx, 1)) + 1 else NA_integer_

  lengths   = integer(length(tokens))
  names_out = character(length(tokens))
  for (i in seq_along(tokens)) {
    tok = trimws(tolower(tokens[i]))
    if (tok == "wy") {
      if (is.na(wy_len)) { warning("TIMESCALES includes 'wy' but no Oct-01 found in dates; skipping."); next }
      lengths[i] = wy_len; names_out[i] = "wy"
    } else if (tok == "ytd") {
      if (is.na(ytd_len)) { warning("TIMESCALES includes 'ytd' but no Jan-01 found in dates; skipping."); next }
      lengths[i] = ytd_len; names_out[i] = "ytd"
    } else {
      lengths[i] = as.numeric(tokens[i]); names_out[i] = paste0(tokens[i], "d")
    }
  }

  # Drop any skipped entries (length == 0)
  keep = lengths > 0 & nzchar(names_out)
  lengths   = lengths[keep]
  names_out = names_out[keep]
  list(lengths = lengths, names = names_out)
}

# ---- F. File discovery -------------------------------------------------------
# Functions that locate raw GridMET NetCDF files on disk and extract their
# date dimensions. Each variable is stored as yearly files named
# <var_prefix>_YYYY.nc (e.g. pr_2024.nc, vpd_2025.nc).

#' Find and sort raw NetCDF files matching <var_prefix>_YYYY.nc in a directory.
.list_raw_files = function(dir, var_prefix) {
  dir = .abs_path(dir)
  if (!fs::dir_exists(dir)) stop("Raw ", var_prefix, " directory not found: ", dir)
  files = fs::dir_ls(dir, regexp = paste0(var_prefix, "_[0-9]{4}\\.nc$"), type = "file")
  if (!length(files)) stop("No raw ", var_prefix, " NetCDF files found in: ", dir)
  yrs = suppressWarnings(as.integer(gsub("[^0-9]", "", fs::path_file(files))))
  files[order(yrs)]
}

#' Read the time dimension from a NetCDF file and convert to R Date objects.
#' Supports both "day" and "time" dimension names. Interprets the "days since"
#' units string to determine the origin date.
.nc_time_dates = function(nc_path) {
  nc = ncdf4::nc_open(nc_path)
  on.exit(ncdf4::nc_close(nc), add = TRUE)

  tname = NULL
  if ("day"  %in% names(nc$dim)) tname = "day"
  if (is.null(tname) && "time" %in% names(nc$dim)) tname = "time"
  if (is.null(tname)) stop("No time/day dimension found in: ", nc_path)

  vals   = nc$dim[[tname]]$vals
  units  = nc$dim[[tname]]$units %||% "days since 1900-01-01"
  origin = sub("^days since\\s+", "", units)
  as.Date(origin) + as.integer(vals)
}

#' Concatenate dates from all yearly NetCDF files, deduplicate, and return
#' a sorted date vector alongside the file paths.
collect_all_dates = function(files) {
  dd   = lapply(as.character(files), .nc_time_dates)
  d    = as.Date(unlist(dd, use.names = FALSE))
  keep = !duplicated(d)
  list(dates = d[keep], files = files)
}

#' Align two variables' date vectors to their common (overlapping) dates.
#'
#' Used for dual-variable metrics (SPEI = PR - PET, EDDI = PET). Reads dates
#' from both sets of files, finds the intersection, and returns indices into
#' each variable's date vector so that layers can be subset to matching dates
#' before combining.
#'
#' @param files1,files2 Character vectors of NetCDF file paths.
#' @param label1,label2 Labels for log messages.
#' @return List with: dates (common Date vector), idx1/idx2 (integer indices),
#'   files1/files2 (pass-through).
align_two_var_dates = function(files1, files2, label1 = "var1", label2 = "var2") {
  info1 = collect_all_dates(files1)
  info2 = collect_all_dates(files2)

  common = intersect(as.character(info1$dates), as.character(info2$dates))
  if (!length(common)) stop(label1, "/", label2, " date alignment failed: no overlapping dates.")

  common = sort(as.Date(common))
  idx1 = match(as.character(common), as.character(info1$dates))
  idx2 = match(as.character(common), as.character(info2$dates))

  message(sprintf("%s/%s aligned: %s .. %s (%d days)",
                  label1, label2, format(min(common)), format(max(common)), length(common)))

  list(dates = common, idx1 = idx1, idx2 = idx2,
       files1 = files1, files2 = files2)
}

# ---- G. Aggregation helpers --------------------------------------------------
# These are the `agg_fn` functions passed to compute_metrics_generic. They
# operate on the full pixel x time matrix and a vector of column indices
# (one time window), returning a vector of aggregated values (one per pixel).
# Precip uses sum (total accumulation), VPD/tmax use mean.

#' Row-wise sum over selected columns. NA propagation (na.rm = FALSE) ensures
#' that pixels with any missing day in the window produce NA.
.safe_window_sum = function(vals_mat, idx_vec) {
  sub = vals_mat[, idx_vec, drop = FALSE]
  rowSums(sub, na.rm = FALSE)
}

#' Row-wise mean over selected columns. Same NA propagation as above.
.safe_window_mean = function(vals_mat, idx_vec) {
  sub = vals_mat[, idx_vec, drop = FALSE]
  rowMeans(sub, na.rm = FALSE)
}

# ---- H. Generic tile readers ------------------------------------------------
# These functions run INSIDE each forked parallel worker. They open NetCDF files
# independently (no terra objects cross the fork boundary), stack all yearly
# layers, crop/mask to the tile polygon, and extract a plain R matrix.
#
# The returned list contains:
#   $vals   — numeric matrix [pixels x days], the raw daily values for this tile
#   $base_r — a single-layer SpatRaster template (all NA) with the tile's
#             extent/resolution, used as a template for writing metric output
#   $msg    — error/skip message (empty string on success)

#' Read a single climate variable for one tile from yearly NetCDF files.
#'
#' Opens each file via NETCDF: subdataset syntax, stacks all layers, crops to
#' the tile extent (with snap="out" to avoid partial pixels), masks to the tile
#' polygon, and extracts the values matrix. If the tile is entirely over ocean
#' or outside the data domain, returns vals=NULL with a descriptive message.
#'
#' @param tile_sf sf object (single-row) defining the tile polygon.
#' @param nc_files Character vector of NetCDF file paths (sorted by year).
#' @param dates Date vector matching the total number of layers across all files.
#' @param nc_varname NetCDF variable name (e.g. "precipitation_amount").
#' @param terra_temp_dir Path for terra's scratch files in this worker.
#' @return List with vals (matrix or NULL), base_r (SpatRaster or NULL), msg.
read_tile_single_var = function(tile_sf, nc_files, dates, nc_varname, terra_temp_dir) {
  suppressPackageStartupMessages(library(terra))
  terra::terraOptions(tempdir = terra_temp_dir)

  tile_sf = sf::st_transform(sf::st_buffer(sf::st_make_valid(tile_sf), 1e-6), 4326)
  v_tile  = terra::vect(tile_sf)

  rr_list = vector("list", length(nc_files))
  for (k in seq_along(nc_files)) {
    r_k = try(
      terra::rast(paste0("NETCDF:", as.character(nc_files[[k]]), ":", nc_varname)),
      silent = TRUE
    )
    if (inherits(r_k, "try-error")) {
      return(list(vals = NULL, base_r = NULL, msg = paste("read error:", nc_files[[k]])))
    }
    rr_list[[k]] = r_k
  }

  rfull = do.call(c, rr_list)
  rm(rr_list); gc(verbose = FALSE)

  if (terra::nlyr(rfull) == length(dates)) {
    terra::time(rfull) = dates
  }

  r_tile = try(
    terra::crop(rfull, v_tile, snap = "out") |> terra::mask(v_tile),
    silent = TRUE
  )
  rm(rfull); gc(verbose = FALSE)

  if (inherits(r_tile, "try-error")) {
    return(list(vals = NULL, base_r = NULL, msg = "crop/mask error"))
  }

  dxy = dim(r_tile)
  if (is.null(dxy) || prod(dxy[1:2]) == 0 || terra::nlyr(r_tile) == 0) {
    return(list(vals = NULL, base_r = NULL, msg = "empty tile after crop/mask"))
  }

  vals = terra::values(r_tile, mat = TRUE)
  if (is.null(vals) || nrow(vals) == 0 || ncol(vals) == 0) {
    return(list(vals = NULL, base_r = NULL, msg = "no values matrix"))
  }

  sample_v = vals[, ncol(vals)]
  if (!any(is.finite(sample_v))) {
    return(list(vals = NULL, base_r = NULL, msg = "tile all-NA"))
  }

  base_r = r_tile[[1]]
  terra::values(base_r) = NA_real_
  names(base_r) = "metric"

  list(vals = vals, base_r = base_r, msg = "")
}

#' Read two climate variables for one tile, align dates, and combine.
#'
#' Used for dual-variable metrics like SPEI (precipitation minus PET) and
#' EDDI (PET alone, but aligned to precip dates). Reads both variables,
#' subsets to pre-aligned date indices (from align_two_var_dates), crops
#' both to the tile, and applies combine_fn (default: subtraction for
#' water balance). Returns the same list structure as read_tile_single_var.
#'
#' @param tile_sf sf object (single-row) defining the tile polygon.
#' @param files1,files2 Character vectors of NetCDF file paths for each variable.
#' @param varname1,varname2 NetCDF variable names.
#' @param idx1,idx2 Integer index vectors from align_two_var_dates.
#' @param dates Date vector (the common/aligned dates).
#' @param combine_fn Function(rast1, rast2) -> rast. Default: subtraction.
#' @param terra_temp_dir Path for terra's scratch files in this worker.
#' @return List with vals (matrix or NULL), base_r (SpatRaster or NULL), msg.
read_tile_two_var = function(tile_sf, files1, files2, varname1, varname2,
                             idx1, idx2, dates, combine_fn, terra_temp_dir) {
  suppressPackageStartupMessages(library(terra))
  terra::terraOptions(tempdir = terra_temp_dir)

  tile_sf = sf::st_transform(sf::st_buffer(sf::st_make_valid(tile_sf), 1e-6), 4326)
  v_tile  = terra::vect(tile_sf)

  # Read and stack var1
  rr1 = vector("list", length(files1))
  for (k in seq_along(files1)) {
    r_k = try(terra::rast(paste0("NETCDF:", as.character(files1[[k]]), ":", varname1)), silent = TRUE)
    if (inherits(r_k, "try-error")) return(list(vals = NULL, base_r = NULL, msg = paste("var1 read error:", files1[[k]])))
    rr1[[k]] = r_k
  }
  rfull1 = do.call(c, rr1); rm(rr1); gc(verbose = FALSE)

  # Read and stack var2
  rr2 = vector("list", length(files2))
  for (k in seq_along(files2)) {
    r_k = try(terra::rast(paste0("NETCDF:", as.character(files2[[k]]), ":", varname2)), silent = TRUE)
    if (inherits(r_k, "try-error")) return(list(vals = NULL, base_r = NULL, msg = paste("var2 read error:", files2[[k]])))
    rr2[[k]] = r_k
  }
  rfull2 = do.call(c, rr2); rm(rr2); gc(verbose = FALSE)

  # Subset to aligned date indices
  rfull1 = rfull1[[idx1]]
  rfull2 = rfull2[[idx2]]

  # Crop to tile
  r1_t = try(terra::crop(rfull1, v_tile, snap = "out") |> terra::mask(v_tile), silent = TRUE)
  rm(rfull1); gc(verbose = FALSE)
  r2_t = try(terra::crop(rfull2, v_tile, snap = "out") |> terra::mask(v_tile), silent = TRUE)
  rm(rfull2); gc(verbose = FALSE)

  if (inherits(r1_t, "try-error") || inherits(r2_t, "try-error")) {
    return(list(vals = NULL, base_r = NULL, msg = "crop/mask error"))
  }

  dxy = dim(r1_t)
  if (is.null(dxy) || prod(dxy[1:2]) == 0 || terra::nlyr(r1_t) == 0) {
    return(list(vals = NULL, base_r = NULL, msg = "empty tile after crop/mask"))
  }

  # Combine (e.g. PR - PET for water balance)
  combined = combine_fn(r1_t, r2_t)
  rm(r1_t, r2_t); gc(verbose = FALSE)

  vals = terra::values(combined, mat = TRUE)
  if (is.null(vals) || nrow(vals) == 0 || ncol(vals) == 0) {
    return(list(vals = NULL, base_r = NULL, msg = "no values matrix"))
  }

  sample_v = vals[, ncol(vals)]
  if (!any(is.finite(sample_v))) {
    return(list(vals = NULL, base_r = NULL, msg = "tile all-NA"))
  }

  base_r = combined[[1]]
  terra::values(base_r) = NA_real_
  names(base_r) = "metric"
  rm(combined); gc(verbose = FALSE)

  list(vals = vals, base_r = base_r, msg = "")
}

# ---- I. Shared metric wrappers ----------------------------------------------
# Thin error-safe wrappers around the functions defined in drought-functions.R.
# These are used as compute_fn in metric_spec entries for per-pixel vapply loops.
# They return NA_real_ on any error, preventing a single bad pixel from crashing
# the entire tile. For simple metrics (pon, dev, pctile), the vectorize_type
# path in compute_metrics_generic bypasses these wrappers entirely.

#' Minimum number of reference years required to compute a metric.
#' Pixels with fewer finite values in the integ matrix are set to NA.
MIN_YEARS = 10L

#' Look up a function by name in the global environment. Stops with a clear
#' error if not found (e.g. if drought-functions.R wasn't sourced).
.require_fun = function(name) {
  fn = try(get(name, envir = globalenv()), silent = TRUE)
  if (inherits(fn, "try-error") || !is.function(fn)) {
    stop(name, "() not found; check drought-functions.R")
  }
  fn
}

#' Percent of normal: (latest / mean) * 100. Wrapper for vapply fallback.
.pon_latest = function(x_vec, clim_len) {
  val = try(percent_of_normal(x_vec, clim_len), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) NA_real_ else as.numeric(val)
}

#' Deviation from normal: latest - mean. Wrapper for vapply fallback.
.dev_latest = function(x_vec, clim_len) {
  val = try(deviation_from_normal(x_vec, clim_len), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) NA_real_ else as.numeric(val)
}

#' Empirical percentile: fraction of reference values <= latest. Wrapper for vapply fallback.
.pctile_latest = function(x_vec, clim_len) {
  val = try(compute_percentile(x_vec, clim_len), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) NA_real_ else as.numeric(val)
}

# ---- J. Generic metric computation engine ------------------------------------

#' Compute all drought metrics for a single tile across all timescales.
#'
#' This is the core computation function. For each timescale (e.g. 30d, 90d, wy):
#'
#' 1. Build the `integ` matrix (pixels x reference years):
#'    - build_slice_groups() returns a list of index windows, one per reference year
#'    - agg_fn (sum or mean) is applied to each window, producing one aggregated
#'      value per pixel per year
#'    - Result: integ[i, j] = aggregated value for pixel i in reference year j
#'    - The last column is the "latest" (current) period
#'
#' 2. Cache finite_counts = rowSums(is.finite(integ)) once, reused across all
#'    metric specs to determine which pixels have enough data (>= MIN_YEARS).
#'
#' 3. Loop over metric_specs. Each spec defines one output layer. Three
#'    computation paths (checked in order):
#'
#'    a. raw_latest = TRUE: Simply extract the latest column value. Used for
#'       precip-mm (current period total). Optional transform function applied.
#'
#'    b. vectorize_type = "pon" | "dev" | "pctile": Matrix operations applied
#'       to all qualifying pixels at once (no per-pixel loop). This is a
#'       performance optimization for simple metrics:
#'         - "pon":    (latest / rowMeans(integ)) * 100
#'         - "dev":    latest - rowMeans(integ)
#'         - "pctile": rowSums(integ <= latest) / rowSums(is.finite(integ))
#'
#'    c. vapply fallback: Per-pixel loop calling spec$compute_fn(integ[i,], clim_len).
#'       Used for complex distribution-fitting metrics (SPI gamma, SPEI GLO,
#'       SVPDI gamma, EDDI nonparametric) that require per-pixel L-moment
#'       fitting and can't be vectorized.
#'
#' 4. Each result is written into a copy of base_r (the single-layer template).
#'
#' @param vals Numeric matrix [pixels x days] from read_tile_single_var/two_var.
#' @param dates Date vector matching ncol(vals).
#' @param base_r Single-layer SpatRaster template for this tile.
#' @param timescale_info List with $lengths and $names from standard_timescales_days.
#' @param clim_spec Single clim_spec list from parse_clim_periods.
#' @param metric_specs List of metric spec lists. Each spec has:
#'   - prefix: output name prefix (e.g. "spi", "precip-pon")
#'   - band_name: layer name in the output raster
#'   - compute_fn: function(x_vec, clim_len) -> numeric(1) [for vapply path]
#'   - vectorize_type: "pon", "dev", or "pctile" [optional, for matrix path]
#'   - raw_latest: TRUE to just use the latest value [optional]
#'   - transform: function(x) applied after raw_latest extraction [optional]
#'   - min_years: override MIN_YEARS for this spec [optional]
#' @param agg_fn Aggregation function (safe_window_sum or safe_window_mean).
#' @return Named list of single-layer SpatRasters, or list(.msg = "...") if all NA.
compute_metrics_generic = function(vals, dates, base_r, timescale_info,
                                   clim_spec, metric_specs, agg_fn) {
  groups_per_period = purrr::map(timescale_info$lengths, build_slice_groups,
                                 dates = dates, clim_spec = clim_spec)

  compute_one_period = function(p_i) {
    groups = groups_per_period[[p_i]]
    nm     = timescale_info$names[[p_i]]

    out_nms = vapply(metric_specs, function(s) paste0(s$prefix, "_", nm), "")

    if (length(groups) < MIN_YEARS) {
      z = terra::setValues(base_r, NA_real_)
      return(setNames(rep(list(z), length(metric_specs)), out_nms))
    }

    clim_len = length(groups)
    integ = matrix(NA_real_, nrow = nrow(vals), ncol = length(groups))
    for (g in seq_along(groups)) integ[, g] = agg_fn(vals, groups[[g]])

    # Cache finite counts to avoid redundant rowSums across metric_specs
    finite_counts = rowSums(is.finite(integ))

    results = vector("list", length(metric_specs))
    for (s_i in seq_along(metric_specs)) {
      spec     = metric_specs[[s_i]]
      spec_min = spec$min_years %||% MIN_YEARS
      ok_rows  = which(finite_counts >= spec_min)
      out_vals = rep(NA_real_, nrow(integ))

      if (length(ok_rows) > 0) {
        if (isTRUE(spec$raw_latest)) {
          # Special case: use latest window value with optional transform
          out_vals[ok_rows] = integ[ok_rows, ncol(integ)]
          if (!is.null(spec$transform)) {
            out_vals[ok_rows] = spec$transform(out_vals[ok_rows])
          }
        } else if (!is.null(spec$vectorize_type)) {
          # Vectorized matrix operations for simple metrics
          vtype  = spec$vectorize_type
          sub    = integ[ok_rows, , drop = FALSE]
          latest = sub[, ncol(sub)]
          if (vtype == "pon") {
            means = rowMeans(sub, na.rm = FALSE)
            out_vals[ok_rows] = (latest / means) * 100
          } else if (vtype == "dev") {
            means = rowMeans(sub, na.rm = FALSE)
            out_vals[ok_rows] = latest - means
          } else if (vtype == "pctile") {
            n = rowSums(is.finite(sub))
            out_vals[ok_rows] = rowSums(sub <= latest, na.rm = FALSE) / n
          }
        } else {
          out_vals[ok_rows] = vapply(ok_rows, function(i)
            spec$compute_fn(integ[i, ], clim_len), numeric(1))
        }
      }

      # Scrub Inf/NaN to NA (can arise from division by zero in PON/percentile)
      out_vals[!is.finite(out_vals)] = NA_real_

      r = base_r
      names(r) = spec$band_name
      r = terra::setValues(r, out_vals)
      results[[s_i]] = r
    }

    setNames(results, out_nms)
  }

  rs  = lapply(seq_along(timescale_info$lengths), compute_one_period)
  out = do.call(c, rs)

  any_data = any(vapply(out, .has_any_data, logical(1)))
  if (!isTRUE(any_data)) return(list(.msg = "all metrics are NA for this tile"))

  out
}

# ---- K. Write & mosaic -------------------------------------------------------
# Functions for writing per-tile COGs, mosaicking tiles into CONUS-wide COGs,
# and embedding date metadata. The write path uses atomic temp-file-then-move
# to avoid corrupt partial writes.

#' Embed date metadata into a SpatRaster before writing.
#'
#' Sets three forms of date information:
#'   - names(r): the layer name (e.g. "spi_30d_rolling-30_2026-03-10")
#'   - terra::time(r): R Date object, readable via rast("file.tif") |> time()
#'   - terra::metags(r): GDAL metadata tag "data_date", readable by any
#'     GDAL-aware tool without loading the full raster
#'
#' @param r SpatRaster (single-layer).
#' @param date_iso Character, ISO date string (e.g. "2026-03-10").
#' @param layer_name Optional full layer name; defaults to date_iso.
#' @return The modified SpatRaster (in-place modification).
.stamp_date = function(r, date_iso, layer_name = NULL) {
  names(r) = layer_name %||% date_iso
  terra::time(r) = as.Date(date_iso)
  terra::metags(r) = c(data_date = date_iso)
  r
}

#' Write a single-layer SpatRaster to a COG file with validation.
#'
#' Validates the raster has data (not all-NA), sets NAflag to -9999, writes to
#' a temp file first (COG format preferred, falls back to compressed GeoTIFF),
#' then atomically moves to the final path.
#'
#' @param r SpatRaster to write.
#' @param out_path Final output file path.
#' @param tile_id Integer tile ID (used in temp filename to avoid collisions).
#' @param r_temp Directory for temp files.
#' @return TRUE on success (stops on failure).
.write_checked = function(r, out_path, tile_id = NA_integer_, r_temp) {
  out_path = .abs_path(out_path)
  out_dir  = fs::path_dir(out_path)
  .dir_create_base(out_dir)
  if (!.is_writable_dir(out_dir)) stop("[EACCES] Output directory not writable: ", out_dir)
  if (!.has_any_data(r)) stop("Refusing to write all-NA raster: ", out_path)

  terra::NAflag(r) = -9999

  pid = Sys.getpid()
  tmp = fs::path(r_temp, paste0("tmp_", pid, "_tile_", tile_id, "_", fs::path_file(out_path)))
  if (fs::file_exists(tmp)) try(fs::file_delete(tmp), silent = TRUE)

  ok = FALSE
  try({ terra::writeRaster(r, tmp, filetype = "COG", overwrite = TRUE); ok = TRUE }, silent = TRUE)
  if (!ok) {
    terra::writeRaster(r, tmp, overwrite = TRUE, gdal = c("COMPRESS=LZW", "BIGTIFF=IF_SAFER"))
  }

  if (!fs::file_exists(tmp) || fs::file_info(tmp)$size <= 0) {
    stop("Write failed or empty tmp file for: ", out_path)
  }

  .atomic_copy_into_place(tmp, out_path)
  TRUE
}

#' Check if gdalUtilities package is available (used for VRT -> COG mosaicking).
.has_gdal_utils = function() requireNamespace("gdalUtilities", quietly = TRUE)

#' Mosaic multiple tile COGs into a single CONUS-wide COG.
#'
#' Preferred path: build a GDAL VRT (virtual mosaic) from all tile files, then
#' gdal_translate to COG with LZW compression. This is memory-efficient because
#' the VRT is just a metadata file referencing the tiles.
#'
#' Fallback (if gdalUtilities not installed): load all tiles into terra, mosaic
#' with mean, and write directly. This uses more memory.
#'
#' @param src_files Character vector of tile COG file paths.
#' @param out_tif Output COG file path.
#' @param r_temp Temp directory for the intermediate VRT.
#' @param label Label for temp file naming.
#' @return TRUE on success.
.mosaic_vrt_to_cog = function(src_files, out_tif, r_temp, label = "metric") {
  out_tif = .abs_path(out_tif)
  .dir_create_base(fs::path_dir(out_tif))

  src_files = src_files[file.info(src_files)$size > 0]
  if (!length(src_files)) stop("No non-empty tiles provided for mosaic: ", out_tif)

  if (.has_gdal_utils()) {
    vrt_path = fs::path(r_temp, paste0(label, "_", as.integer(stats::runif(1, 1, 1e9)), ".vrt"))
    on.exit(try(fs::file_delete(vrt_path), silent = TRUE), add = TRUE)

    gdalUtilities::gdalbuildvrt(gdalfile = src_files, output.vrt = vrt_path)
    gdalUtilities::gdal_translate(
      src_dataset = vrt_path,
      dst_dataset = out_tif,
      of          = "COG",
      co          = c("COMPRESS=LZW", "PREDICTOR=2", "BIGTIFF=IF_SAFER")
    )

    ok = fs::file_exists(out_tif) && fs::file_info(out_tif)$size > 0
    if (!ok) stop("Mosaic write failed (GDAL): ", out_tif)
    TRUE
  } else {
    rr  = terra::rast(src_files)
    mos = terra::mosaic(rr, fun = "mean", na.rm = TRUE)
    terra::NAflag(mos) = -9999
    terra::writeRaster(mos, out_tif, filetype = "COG", overwrite = TRUE)
    ok = fs::file_exists(out_tif) && fs::file_info(out_tif)$size > 0
    if (!ok) stop("Mosaic write failed (terra): ", out_tif)
    TRUE
  }
}

#' Mosaic all tile COGs for one metric/timescale/period combination.
#'
#' Finds all tile_*.tif files in the period directory, mosaics them to a
#' CONUS-wide COG, then stamps the date metadata (layer name, time, GDAL tag).
#' The date stamping requires a read-back + rewrite because the VRT -> COG
#' path doesn't preserve terra::time(). The rewrite goes through a temp GeoTIFF
#' (which preserves time) then converts back to COG.
#'
#' Optionally deletes the tile directory after mosaicking (default behavior,
#' controlled by KEEP_TILES env var).
#'
#' @param period_dir_name Directory name (e.g. "spi_30d_rolling-30").
#' @param last_date_iso ISO date string for the latest data date.
#' @param tiles_root Parent directory containing period subdirectories.
#' @param conus_root Output directory for CONUS-wide COGs.
#' @param r_temp Temp directory for intermediate files.
#' @param label Label for log messages.
#' @param keep_tiles Logical, whether to keep tile files after mosaic.
#' @return TRUE if mosaic was written, FALSE otherwise.
mosaic_period_dir = function(period_dir_name, last_date_iso,
                             tiles_root, conus_root, r_temp, label = "metric",
                             keep_tiles = FALSE) {
  tile_dir = .abs_path(fs::path(tiles_root, period_dir_name))
  if (!fs::dir_exists(tile_dir)) return(FALSE)

  tifs = fs::dir_ls(tile_dir, glob = "*.tif", type = "file")
  if (length(tifs) == 0) return(FALSE)

  out_tif = fs::path(conus_root, paste0(period_dir_name, "_", last_date_iso, ".tif"))
  message("Mosaicking ", length(tifs), " tiles for ", period_dir_name)

  ok = FALSE
  try({ ok = .mosaic_vrt_to_cog(tifs, out_tif, r_temp, label) }, silent = FALSE)
  if (!isTRUE(ok)) return(FALSE)

  # Stamp date into layer name, time, and GDAL metadata
  try({
    r = terra::rast(out_tif)
    r = .stamp_date(r, last_date_iso, paste0(period_dir_name, "_", last_date_iso))
    tmp_tif = paste0(out_tif, ".tmp.tif")
    terra::writeRaster(r, tmp_tif, overwrite = TRUE)
    if (.has_gdal_utils()) {
      gdalUtilities::gdal_translate(
        src_dataset = tmp_tif, dst_dataset = out_tif,
        of = "COG", co = c("COMPRESS=LZW", "PREDICTOR=2", "BIGTIFF=IF_SAFER")
      )
    } else {
      file.copy(tmp_tif, out_tif, overwrite = TRUE)
    }
    unlink(tmp_tif)
  }, silent = TRUE)

  if (!keep_tiles) try(fs::dir_delete(tile_dir), silent = TRUE)

  message("Wrote mosaic: ", out_tif)
  TRUE
}

#' Mosaic all period directories and optionally clean up tile files.
#' Loops over all metric/timescale/period combinations found in the tiles_root.
mosaic_all_periods_and_cleanup = function(period_dir_names, last_date_iso,
                                          tiles_root, conus_root, r_temp,
                                          label = "metric") {
  keep_tiles = identical(Sys.getenv("KEEP_TILES", "0"), "1")
  wrote_any  = FALSE
  for (pd in period_dir_names) {
    ok = FALSE
    try({
      ok = mosaic_period_dir(pd, last_date_iso, tiles_root, conus_root,
                             r_temp, label, keep_tiles)
    }, silent = FALSE)
    wrote_any = wrote_any || isTRUE(ok)
  }
  wrote_any
}

# ---- L. Parallel backend -----------------------------------------------------

#' Dispatch tile-processing workers in parallel using forked mclapply.
#'
#' Uses pbmcapply (mclapply with a progress bar). mc.preschedule = FALSE ensures
#' tiles are assigned dynamically (load-balanced), which helps when tile sizes
#' vary (ocean tiles are fast, land tiles are slow).
#'
#' @param tiles_list List of single-row sf objects, one per tile.
#' @param worker_fn Function(i) that processes tile i and returns a result list.
#' @param cores Number of parallel workers.
#' @return List of raw worker results (may include non-list entries from OOM kills).
.run_workers_generic = function(tiles_list, worker_fn, cores) {
  message("Parallel backend: mclapply (", cores, " workers) — reading from files per worker")

  pbmcapply::pbmclapply(seq_along(tiles_list), worker_fn,
                         mc.cores = cores, mc.preschedule = FALSE,
                         ignore.interactive = TRUE)
}

#' Defensive post-processing of worker results.
#'
#' Forked workers that are OOM-killed return non-list objects (often NULL or a
#' raw error). This function normalizes all results to a consistent structure
#' with fields: ok, wrote, paths, tile_id, msg.
.normalize_results = function(results_raw) {
  lapply(results_raw, function(x) {
    if (!is.list(x)) {
      return(list(ok = FALSE, wrote = 0L, paths = character(0),
                  tile_id = NA_integer_, msg = "non-list result (worker likely OOM-killed)"))
    }
    if (is.null(x$msg) || !length(x$msg)) x$msg = ""
    x$ok      = isTRUE(x$ok)
    x$wrote   = as.integer(x$wrote   %||% 0L)
    x$paths   = as.character(x$paths %||% character(0))
    x$tile_id = suppressWarnings(as.integer(x$tile_id))
    x$msg     = as.character(x$msg)[1]
    x
  })
}

# ---- M. Main runner ----------------------------------------------------------

#' Run the full metric pipeline for a single variable/metric family.
#'
#' This is the top-level orchestrator called by each metric script (2-6).
#' It receives a config list that fully defines the metric computation and
#' executes the entire pipeline:
#'
#'   1. SETUP: Read env vars, initialize paths, pin thread counts to 1
#'      (prevents BLAS/OpenMP contention with forked workers).
#'
#'   2. DISCOVER FILES: Find raw NetCDF files, check freshness against
#'      existing outputs (skip if up-to-date), extract date vectors.
#'      For dual-variable metrics, align date vectors across both variables.
#'
#'   3. BUILD TIMESCALES: Parse TIMESCALES env var into day counts and names.
#'
#'   4. BUILD TILE GRID: Create a regular grid of tile polygons from the
#'      raster extent, optionally filtered to specific TILE_IDS.
#'
#'   5. PARSE CLIMATOLOGY: Parse CLIM_PERIODS env var into reference period specs.
#'
#'   6. BUILD WORKER CLOSURE: Create a function that captures all needed
#'      variables. Each worker:
#'      a. Reads raw data for its tile ONCE (read_tile_single_var or two_var)
#'      b. Loops over climatology periods
#'      c. Calls compute_metrics_generic on the in-memory matrix
#'      d. Writes per-tile COG files with date metadata
#'
#'   7. RUN IN PARALLEL: Dispatch workers across tiles via forked mclapply.
#'
#'   8. REPORT: Log success/failure counts, skip reasons, and errors.
#'
#'   9. MOSAIC: Combine tile COGs into CONUS-wide COGs per metric/timescale/period.
#'
#'  10. CLEANUP: Optionally delete intermediate tile directories.
#'
#' @param config Named list with:
#'   - metric_label: Human-readable name for log messages (e.g. "precipitation")
#'   - tiles_subdir: Subdirectory name under derived/ for tiles (e.g. "precip_metrics")
#'   - raw_vars: List of lists, each with var_prefix, raw_subdir, nc_varname
#'   - output_regexp: Regex to match existing output files for freshness check
#'   - input_mode: "single" (one variable) or "dual_subtract" (two variables combined)
#'   - agg_fn: Aggregation function (.safe_window_sum or .safe_window_mean)
#'   - metric_specs: List of metric spec lists (see compute_metrics_generic)
#'   - combine_fn: [dual_subtract only] Function(rast1, rast2) -> rast
#'
#' @return Invisible. Writes COG files to disk as side effect.
#'
#' @examples
#' # Example config for precipitation (from 2_metrics-precip.R):
#' config = list(
#'   metric_label  = "precipitation",
#'   tiles_subdir  = "precip_metrics",
#'   raw_vars      = list(
#'     list(var_prefix = "pr", raw_subdir = "pr", nc_varname = "precipitation_amount")
#'   ),
#'   output_regexp = "spi_.*\\.tif$",
#'   input_mode    = "single",
#'   agg_fn        = .safe_window_sum,
#'   metric_specs  = list(
#'     list(prefix = "spi", compute_fn = .spi_from_gamma, band_name = "spi"),
#'     list(prefix = "precip-pon", band_name = "pon", vectorize_type = "pon"),
#'     list(prefix = "precip-mm",  band_name = "precip_mm", raw_latest = TRUE, min_years = 1L)
#'   )
#' )
#' run_metric_pipeline(config)
run_metric_pipeline = function(config) {
  message(Sys.time(), " — Starting ", config$metric_label, " metrics run")
  t_start = proc.time()

  Sys.setenv(
    OMP_NUM_THREADS      = "1",
    OPENBLAS_NUM_THREADS = "1",
    MKL_NUM_THREADS      = "1",
    GDAL_NUM_THREADS     = "1"
  )

  dx           = as.numeric(Sys.getenv("TILE_DX",      unset = "2"))
  dy           = as.numeric(Sys.getenv("TILE_DY",      unset = "2"))
  periods_env  = Sys.getenv("PERIODS_DAYS", unset = "")
  tile_ids_env = Sys.getenv("TILE_IDS",     unset = "")

  requested = as.integer(Sys.getenv("CORES", unset = "4"))
  if (is.na(requested)) requested = 4L
  cores = max(1L, min(12L, requested))
  message("Using ", cores, " worker(s).")

  # Setup paths
  paths = setup_pipeline_paths(config$tiles_subdir)

  # Discover files and dates
  if (config$input_mode == "single") {
    var = config$raw_vars[[1]]
    raw_var_dir = .abs_path(fs::path(paths$data_root, "raw", var$raw_subdir))
    nc_files = .list_raw_files(raw_var_dir, var$var_prefix)

    if (!.raw_newer_than_outputs(nc_files, paths$conus_root, config$output_regexp)) {
      message(Sys.time(), " — ", config$metric_label,
              " outputs are up to date; skipping.")
      return(invisible(FALSE))
    }

    date_info = collect_all_dates(nc_files)
    dates     = date_info$dates

    # For building tile grid, use first var's first file
    nc_varname_meta = var$nc_varname
    meta_file       = nc_files[[1]]

  } else if (config$input_mode == "dual_subtract") {
    var1 = config$raw_vars[[1]]
    var2 = config$raw_vars[[2]]
    raw_dir1 = .abs_path(fs::path(paths$data_root, "raw", var1$raw_subdir))
    raw_dir2 = .abs_path(fs::path(paths$data_root, "raw", var2$raw_subdir))
    files1 = .list_raw_files(raw_dir1, var1$var_prefix)
    files2 = .list_raw_files(raw_dir2, var2$var_prefix)

    if (!.raw_newer_than_outputs(c(files1, files2), paths$conus_root, config$output_regexp)) {
      message(Sys.time(), " — ", config$metric_label,
              " outputs are up to date; skipping.")
      return(invisible(FALSE))
    }

    aln   = align_two_var_dates(files1, files2, var1$var_prefix, var2$var_prefix)
    dates = aln$dates

    nc_varname_meta = var1$nc_varname
    meta_file       = files1[[1]]
  }

  last_date_iso = format(max(dates, na.rm = TRUE))
  message("Last date: ", last_date_iso)

  # Build timescale info
  if (nzchar(periods_env)) {
    pd = as.numeric(strsplit(periods_env, ",")[[1]])
    timescale_info = list(lengths = pd, names = paste0(pd, "d"))
  } else {
    timescale_info = standard_timescales_days(dates)
  }

  # Build tile grid
  r_meta = terra::rast(paste0("NETCDF:", as.character(meta_file), ":", nc_varname_meta))[[1]]
  tiles  = build_tiles_from_extent(dx = dx, dy = dy, r_for_align = r_meta)
  rm(r_meta); gc(verbose = FALSE)

  if (nzchar(tile_ids_env)) {
    keep_ids = as.integer(strsplit(tile_ids_env, ",")[[1]])
    tiles    = tiles[tiles$tile_id %in% keep_ids, ]
  }

  n_tiles = nrow(tiles)
  message("Processing ", n_tiles, " tile(s)...")
  if (n_tiles == 0) stop("No tiles to process (extent/params mismatch).")

  tiles_list = lapply(seq_len(n_tiles), function(i) tiles[i, ])

  clim_periods = parse_clim_periods(Sys.getenv("CLIM_PERIODS", unset = "rolling:30"))
  message("Reference periods: ", paste(sapply(clim_periods, `[[`, "slug"), collapse = ", "))

  # Shared post-read worker logic: after a tile's raw data has been read into
  # memory (io$vals matrix), this function loops over climatology periods,
  # calls compute_metrics_generic for each, and writes per-tile COG files.
  # This separation means tile data is read ONCE and reused across all
  # climatology periods (e.g. rolling:30 and fixed:1991:2020).
  .worker_compute_and_write = function(io, tile, clim_periods, dates, timescale_info,
                                       metric_specs, agg_fn, tiles_root, r_temp,
                                       last_date_iso) {
    if (is.null(io$vals)) {
      return(list(ok = TRUE, wrote = 0L, paths = character(0),
                  tile_id = tile$tile_id, msg = io$msg %||% "no tile data"))
    }

    wrote = 0L; all_paths = character(0)
    for (clim_spec in clim_periods) {
      rs = compute_metrics_generic(
        vals           = io$vals,
        dates          = dates,
        base_r         = io$base_r,
        timescale_info = timescale_info,
        clim_spec      = clim_spec,
        metric_specs   = metric_specs,
        agg_fn         = agg_fn
      )

      if (is.list(rs) && length(rs) == 1 && identical(names(rs), ".msg")) next
      if (length(rs) == 0) next

      for (nm in names(rs)) {
        period_dir = fs::path(tiles_root, paste0(nm, "_", clim_spec$slug))
        .dir_create_base(period_dir)
        out = fs::path(period_dir, paste0("tile_", tile$tile_id, ".tif"))
        full_name = paste0(nm, "_", clim_spec$slug, "_", last_date_iso)
        .write_checked(.stamp_date(rs[[nm]], last_date_iso, full_name), out,
                       tile_id = tile$tile_id, r_temp = r_temp)
        wrote = wrote + 1L
        all_paths = c(all_paths, out)
      }
    }

    list(ok = TRUE, wrote = wrote, paths = all_paths, tile_id = tile$tile_id, msg = "")
  }

  # Build worker function (closure captures all needed variables).
  # Key optimization: tile data is read ONCE, then clim loop runs on in-memory matrix.
  if (config$input_mode == "single") {
    var = config$raw_vars[[1]]
    worker_fn = function(i) {
      tile = tiles_list[[i]]
      tryCatch({
        io = read_tile_single_var(
          tile_sf        = tile,
          nc_files       = nc_files,
          dates          = dates,
          nc_varname     = var$nc_varname,
          terra_temp_dir = paths$terra_temp
        )
        .worker_compute_and_write(io, tile, clim_periods, dates, timescale_info,
                                  config$metric_specs, config$agg_fn,
                                  paths$tiles_root, paths$r_temp,
                                  last_date_iso)
      },
      error = function(e) list(
        ok = FALSE, wrote = 0L, paths = character(0),
        tile_id = tile$tile_id, msg = conditionMessage(e)
      ))
    }

  } else if (config$input_mode == "dual_subtract") {
    combine_fn = config$combine_fn %||% function(a, b) a - b
    worker_fn = function(i) {
      tile = tiles_list[[i]]
      tryCatch({
        io = read_tile_two_var(
          tile_sf        = tile,
          files1         = aln$files1,
          files2         = aln$files2,
          varname1       = config$raw_vars[[1]]$nc_varname,
          varname2       = config$raw_vars[[2]]$nc_varname,
          idx1           = aln$idx1,
          idx2           = aln$idx2,
          dates          = dates,
          combine_fn     = combine_fn,
          terra_temp_dir = paths$terra_temp
        )
        .worker_compute_and_write(io, tile, clim_periods, dates, timescale_info,
                                  config$metric_specs, config$agg_fn,
                                  paths$tiles_root, paths$r_temp,
                                  last_date_iso)
      },
      error = function(e) list(
        ok = FALSE, wrote = 0L, paths = character(0),
        tile_id = tile$tile_id, msg = conditionMessage(e)
      ))
    }
  }

  # Run workers
  results_raw = .run_workers_generic(tiles_list, worker_fn, cores)

  # Process results
  res       = .normalize_results(results_raw)
  ok_vec    = vapply(res, function(x) isTRUE(x$ok), TRUE)
  wrote_ct  = vapply(res, function(x) as.integer(x$wrote), 0L)
  all_paths = unlist(lapply(res, `[[`, "paths"), use.names = FALSE)

  message(sprintf("Tiles OK: %d/%d; total files written: %d",
                  sum(ok_vec), n_tiles, sum(wrote_ct)))

  # ---- Retry failed tiles, then subdivide if still failing -------------------
  failed_idx = which(!ok_vec)
  if (length(failed_idx) > 0) {
    failed_ids = vapply(res[failed_idx], function(x) as.character(x$tile_id), "")
    failed_msgs = vapply(res[failed_idx], function(x) as.character(x$msg)[1], "")
    message(sprintf("Retrying %d failed tile(s): %s",
                    length(failed_idx), paste(failed_ids, collapse = ", ")))
    for (fm in unique(failed_msgs[nzchar(failed_msgs)])) message("  reason: ", fm)

    # Pass 1: simple retry (catches transient errors)
    # Build a new tiles_list for retry so indices map correctly
    retry_tiles = tiles_list[failed_idx]
    retry_worker = function(i) {
      tile = retry_tiles[[i]]
      tryCatch({
        if (config$input_mode == "single") {
          var = config$raw_vars[[1]]
          io  = read_tile_single_var(
            tile_sf = tile, nc_files = nc_files, dates = dates,
            nc_varname = var$nc_varname, terra_temp_dir = paths$terra_temp
          )
        } else {
          io = read_tile_two_var(
            tile_sf = tile, files1 = aln$files1, files2 = aln$files2,
            varname1 = config$raw_vars[[1]]$nc_varname,
            varname2 = config$raw_vars[[2]]$nc_varname,
            idx1 = aln$idx1, idx2 = aln$idx2, dates = dates,
            combine_fn = config$combine_fn %||% function(a, b) a - b,
            terra_temp_dir = paths$terra_temp
          )
        }
        .worker_compute_and_write(io, tile, clim_periods, dates, timescale_info,
                                  config$metric_specs, config$agg_fn,
                                  paths$tiles_root, paths$r_temp, last_date_iso)
      },
      error = function(e) list(
        ok = FALSE, wrote = 0L, paths = character(0),
        tile_id = tile$tile_id, msg = conditionMessage(e)
      ))
    }
    retry_raw = .run_workers_generic(retry_tiles, retry_worker, cores)
    retry_res = .normalize_results(retry_raw)

    retry_ok = vapply(retry_res, function(x) isTRUE(x$ok), TRUE)
    if (any(retry_ok)) {
      n_recovered = sum(retry_ok)
      message(sprintf("  Retry recovered %d/%d tile(s)", n_recovered, length(failed_idx)))
      retry_paths = unlist(lapply(retry_res[retry_ok], `[[`, "paths"), use.names = FALSE)
      all_paths   = c(all_paths, retry_paths)
      wrote_ct[failed_idx[retry_ok]] = vapply(retry_res[retry_ok],
                                               function(x) as.integer(x$wrote), 0L)
      ok_vec[failed_idx[retry_ok]]   = TRUE
    }

    # Pass 2: subdivide tiles that still fail
    still_failed = failed_idx[!retry_ok]
    if (length(still_failed) > 0) {
      message(sprintf("Subdividing %d tile(s) that failed retry...", length(still_failed)))
      sub_tiles_list = list()
      for (si in still_failed) {
        sub_4 = .subdivide_tile(tiles_list[[si]])
        for (q in seq_len(nrow(sub_4))) sub_tiles_list[[length(sub_tiles_list) + 1]] = sub_4[q, ]
      }

      # Build a worker that takes a sub-tile directly (same closure vars as worker_fn)
      sub_worker_fn = function(j) {
        tile = sub_tiles_list[[j]]
        tryCatch({
          if (config$input_mode == "single") {
            var = config$raw_vars[[1]]
            io  = read_tile_single_var(
              tile_sf        = tile,
              nc_files       = nc_files,
              dates          = dates,
              nc_varname     = var$nc_varname,
              terra_temp_dir = paths$terra_temp
            )
          } else {
            io = read_tile_two_var(
              tile_sf        = tile,
              files1         = aln$files1,
              files2         = aln$files2,
              varname1       = config$raw_vars[[1]]$nc_varname,
              varname2       = config$raw_vars[[2]]$nc_varname,
              idx1           = aln$idx1,
              idx2           = aln$idx2,
              dates          = dates,
              combine_fn     = config$combine_fn %||% function(a, b) a - b,
              terra_temp_dir = paths$terra_temp
            )
          }
          .worker_compute_and_write(io, tile, clim_periods, dates, timescale_info,
                                    config$metric_specs, config$agg_fn,
                                    paths$tiles_root, paths$r_temp,
                                    last_date_iso)
        },
        error = function(e) list(
          ok = FALSE, wrote = 0L, paths = character(0),
          tile_id = tile$tile_id, msg = conditionMessage(e)
        ))
      }

      sub_raw = .run_workers_generic(sub_tiles_list, sub_worker_fn, cores)
      sub_res = .normalize_results(sub_raw)
      sub_ok  = vapply(sub_res, function(x) isTRUE(x$ok), TRUE)

      n_sub_ok   = sum(sub_ok)
      n_sub_fail = sum(!sub_ok)
      message(sprintf("  Sub-tiles: %d/%d OK", n_sub_ok, length(sub_res)))

      sub_paths = unlist(lapply(sub_res[sub_ok], `[[`, "paths"), use.names = FALSE)
      all_paths = c(all_paths, sub_paths)

      if (n_sub_fail > 0) {
        sub_errs = unique(vapply(sub_res[!sub_ok], function(x) as.character(x$msg)[1], ""))
        sub_errs = sub_errs[nzchar(sub_errs)]
        message("  Sub-tile errors:")
        for (e in utils::head(sub_errs, 12)) message("    \u2022 ", e)
      }
    }
  }

  # Final summary
  errs = unique(vapply(res[!ok_vec], function(x) as.character(x$msg)[1], ""))
  errs = errs[nzchar(errs)]

  skipped_msgs = unique(vapply(res[ok_vec], function(x) as.character(x$msg)[1], ""))
  skipped_msgs = skipped_msgs[nzchar(skipped_msgs)]
  if (length(skipped_msgs) > 0) {
    message("Skip reasons (ok=TRUE, wrote=0):")
    for (m in utils::head(skipped_msgs, 10)) message("  \u2022 ", m)
  }

  if (length(errs) > 0) {
    message("Remaining errors after retry/subdivide:")
    for (e in utils::head(errs, 12)) message("  \u2022 ", e)
  }

  # Mosaic all period directories
  .dir_create_base(paths$conus_root)

  period_dir_names = unique(basename(fs::path_dir(all_paths)))
  if (!length(period_dir_names) && fs::dir_exists(paths$tiles_root)) {
    period_dir_names = basename(fs::dir_ls(paths$tiles_root, type = "directory"))
  }

  message("Mosaicking ", length(period_dir_names), " period(s) to ", paths$conus_root)

  mosaic_any = mosaic_all_periods_and_cleanup(
    period_dir_names, last_date_iso,
    paths$tiles_root, paths$conus_root, paths$r_temp, config$metric_label
  )
  if (!mosaic_any) message("No mosaics written (no usable tiles found).")

  elapsed_min = round((proc.time() - t_start)[["elapsed"]] / 60, 1)
  message(Sys.time(), " — ", config$metric_label, " metrics run complete. Elapsed: ", elapsed_min, " min.")
}
