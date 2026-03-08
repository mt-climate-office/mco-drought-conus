##############################################################
# File: R/3_metrics-spei.R
# Title: SPEI (CONUS, tiled, parallel) from local GridMET raws:
#        Standardized Precipitation Evapotranspiration Index (GLO fit).
#        30-year calendar reference; per-tile GeoTIFF/COG; VRT->COG mosaics.
# Author: Dr. Zachary H. Hoylman
# Date: 3-4-2026
# Conventions: "=", |> , explicit pkg::fun namespaces.
#
# PARALLEL STRATEGY NOTE:
#   mclapply(fork) is used, which is safe here because NO terra SpatRaster
#   objects are passed across the fork boundary. Workers receive only plain R
#   objects (file paths, date vectors, sf tiles) and open their own NetCDF
#   handles + terra sessions independently. Mirrors 2_precipitation-metrics.R.
##############################################################

`%||%` = function(a, b) if (is.null(a)) b else a

.abs_path = function(p) as.character(fs::path_abs(fs::path_expand(p)))

# ---- Packages / startup ------------------------------------------------------
suppressPackageStartupMessages({
  library(fs)
  library(sf)
  library(terra)
  library(purrr)
  library(readr)
  library(ncdf4)
  library(parallel)
  library(rnaturalearth)
})

# ---- Small IO helpers --------------------------------------------------------
.dir_create_base = function(path) {
  path = .abs_path(path)
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

.is_writable_dir = function(path) {
  path = .abs_path(path)
  if (!dir.exists(path)) {
    ok = tryCatch({ .dir_create_base(path); TRUE }, error = function(e) FALSE)
    if (!ok) return(FALSE)
  }
  isTRUE(file.access(path, 2) == 0)
}

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

# Copy-then-delete (more reliable than rename/move across bind mounts)
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

# Quick check for "all nodata / all NA"
.has_any_data = function(r) {
  if (is.null(r)) return(FALSE)
  if (terra::nlyr(r) < 1) return(FALSE)
  n = terra::ncell(r)
  if (is.na(n) || n <= 0) return(FALSE)
  idx = unique(pmax(1, pmin(n, as.integer(seq(1, n, length.out = min(2000, n))))))
  v = terra::values(r[[terra::nlyr(r)]], cells = idx, mat = FALSE)
  any(is.finite(v))
}

# ---- Paths -------------------------------------------------------------------
project_root = Sys.getenv("PROJECT_DIR", unset = "~/mco-drought-conus")
data_root    = Sys.getenv("DATA_DIR",    unset = "~/mco-drought-conus-data")

project_root = .abs_path(project_root)
data_root    = .abs_path(data_root)

interim_dir = .abs_path(fs::path(data_root, "interim"))
derived_dir = .abs_path(fs::path(data_root, "derived"))

tiles_root  = .abs_path(fs::path(derived_dir, "spei_metrics"))
conus_root  = .abs_path(fs::path(derived_dir, "conus_drought"))

raw_pr_dir  = .abs_path(fs::path(interim_dir, "gridmet", "pr",  "raw"))
raw_pet_dir = .abs_path(fs::path(interim_dir, "gridmet", "pet", "raw"))

# temp dirs
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

.dir_create_base(interim_dir)
.dir_create_base(derived_dir)
.dir_create_base(tiles_root)
.dir_create_base(conus_root)

.ensure_writable_dir(derived_dir, "derived_dir")
.ensure_writable_dir(tiles_root,  "tiles_root")
.ensure_writable_dir(conus_root,  "conus_root")

# ---- Load drought functions into GLOBAL env ----------------------------------
source_drought_functions = function(path = fs::path(project_root, "R", "drought-functions.R")) {
  path = .abs_path(path)
  if (!fs::file_exists(path)) stop("drought-functions.R not found at: ", path)
  source(path, local = globalenv())
  invisible(TRUE)
}
source_drought_functions()

# ---- Helpers -----------------------------------------------------------------
conus_geometry = function() {
  st     = rnaturalearth::ne_states(country = "United States of America", returnclass = "sf")
  st_l48 = st[!(st$name %in% c("Alaska", "Hawaii", "Puerto Rico")), ]
  sf::st_union(sf::st_make_valid(st_l48)) |> sf::st_as_sf()
}

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

# Climatological reference period helpers
parse_clim_periods = function(env_val = "rolling:30") {
  specs = strsplit(trimws(env_val), ",")[[1]]
  lapply(specs, function(s) {
    parts = strsplit(trimws(s), ":")[[1]]
    mode  = parts[1]
    switch(mode,
      rolling = list(mode="rolling", years=as.integer(parts[2]),
                     start=NA_integer_, end=NA_integer_,
                     slug=paste0("rolling_", parts[2])),
      fixed   = list(mode="fixed",   years=NA_integer_,
                     start=as.integer(parts[2]), end=as.integer(parts[3]),
                     slug=paste0("fixed_", parts[2], "_", parts[3])),
      full    = list(mode="full",    years=NA_integer_,
                     start=NA_integer_, end=NA_integer_, slug="full"),
      stop("Unknown CLIM_PERIODS mode: ", mode)
    )
  })
}

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

build_slice_groups = function(n_days, dates, clim_spec) {
  anchors = ref_period_indices(dates, clim_spec)
  purrr::compact(purrr::map(anchors, function(end_i) {
    start_i = end_i - (n_days - 1)
    if (start_i < 1) return(NULL)
    seq(start_i, end_i)
  }))
}

spei_timescales_days = function(dates) {
  md      = format(dates, "%m-%d")
  wy_len  = (length(md) - tail(which(md == "10-01"), 1)) + 1
  ytd_len = (length(md) - tail(which(md == "01-01"), 1)) + 1
  list(
    lengths = c(15, 30, 45, 60, 90, 120, 180, 365, 730, wy_len, ytd_len),
    names   = c("15d","30d","45d","60d","90d","120d","180d","365d","730d","wy","ytd")
  )
}

# ---- Raw file discovery + date parsing ---------------------------------------
.list_raw_files = function(dir, var) {
  dir   = .abs_path(dir)
  if (!fs::dir_exists(dir)) stop("Raw ", var, " directory not found: ", dir)
  files = fs::dir_ls(dir, regexp = paste0(var, "_[0-9]{4}\\.nc$"), type = "file")
  if (!length(files)) stop("No raw ", var, " NetCDF files found in: ", dir)
  yrs   = suppressWarnings(as.integer(gsub("[^0-9]", "", fs::path_file(files))))
  files[order(yrs)]
}

.nc_time_dates = function(nc_path) {
  nc    = ncdf4::nc_open(nc_path)
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

collect_all_dates = function(files) {
  dd   = lapply(as.character(files), .nc_time_dates)
  d    = as.Date(unlist(dd, use.names = FALSE))
  keep = !duplicated(d)
  list(dates = d[keep], files = files)
}

# Align PR and PET date vectors — return only the overlapping dates and
# corresponding file-level indices so workers can find the right layers.
align_pr_pet_dates = function(pr_files, pet_files) {
  pr_info  = collect_all_dates(pr_files)
  pet_info = collect_all_dates(pet_files)

  # Build per-file layer offsets so we can map a global date index -> (file, layer)
  pr_dates  = pr_info$dates
  pet_dates = pet_info$dates

  # Inner join on date
  common = intersect(as.character(pr_dates), as.character(pet_dates))
  if (!length(common)) stop("PR/PET date alignment failed: no overlapping dates.")

  common = sort(as.Date(common))
  pr_idx  = match(as.character(common), as.character(pr_dates))
  pet_idx = match(as.character(common), as.character(pet_dates))

  message(sprintf("PR/PET aligned: %s .. %s (%d days)",
                  format(min(common)), format(max(common)), length(common)))

  list(dates = common, pr_idx = pr_idx, pet_idx = pet_idx,
       pr_files = pr_files, pet_files = pet_files,
       pr_dates_all = pr_dates, pet_dates_all = pet_dates)
}

# ---- Per-worker tile reader: reads PR-PET from files, returns values matrix --
read_tile_wb_from_files = function(tile_sf, pr_files, pet_files,
                                   pr_idx, pet_idx, dates,
                                   terra_temp_dir) {
  suppressPackageStartupMessages(library(terra))
  terra::terraOptions(tempdir = terra_temp_dir)

  tile_sf = sf::st_transform(sf::st_buffer(sf::st_make_valid(tile_sf), 1e-6), 4326)
  v_tile  = terra::vect(tile_sf)

  # Read and stack full PR
  rr_pr = vector("list", length(pr_files))
  for (k in seq_along(pr_files)) {
    r_k = try(terra::rast(paste0("NETCDF:", as.character(pr_files[[k]]), ":precipitation_amount")), silent = TRUE)
    if (inherits(r_k, "try-error")) return(list(vals = NULL, base_r = NULL, msg = paste("PR read error:", pr_files[[k]])))
    rr_pr[[k]] = r_k
  }
  rfull_pr = do.call(c, rr_pr); rm(rr_pr); gc(verbose = FALSE)

  # Read and stack full PET
  rr_pet = vector("list", length(pet_files))
  for (k in seq_along(pet_files)) {
    r_k = try(terra::rast(paste0("NETCDF:", as.character(pet_files[[k]]), ":potential_evapotranspiration")), silent = TRUE)
    if (inherits(r_k, "try-error")) return(list(vals = NULL, base_r = NULL, msg = paste("PET read error:", pet_files[[k]])))
    rr_pet[[k]] = r_k
  }
  rfull_pet = do.call(c, rr_pet); rm(rr_pet); gc(verbose = FALSE)

  # Subset to aligned date indices
  rfull_pr  = rfull_pr[[pr_idx]]
  rfull_pet = rfull_pet[[pet_idx]]

  # Crop to tile
  r_pr_t = try(terra::crop(rfull_pr,  v_tile, snap = "out") |> terra::mask(v_tile), silent = TRUE)
  rm(rfull_pr); gc(verbose = FALSE)
  r_pet_t = try(terra::crop(rfull_pet, v_tile, snap = "out") |> terra::mask(v_tile), silent = TRUE)
  rm(rfull_pet); gc(verbose = FALSE)

  if (inherits(r_pr_t, "try-error") || inherits(r_pet_t, "try-error")) {
    return(list(vals = NULL, base_r = NULL, msg = "crop/mask error"))
  }

  dxy = dim(r_pr_t)
  if (is.null(dxy) || prod(dxy[1:2]) == 0 || terra::nlyr(r_pr_t) == 0) {
    return(list(vals = NULL, base_r = NULL, msg = "empty tile after crop/mask"))
  }

  # Water balance = PR - PET
  wb   = r_pr_t - r_pet_t
  rm(r_pr_t, r_pet_t); gc(verbose = FALSE)

  vals = terra::values(wb, mat = TRUE)
  if (is.null(vals) || nrow(vals) == 0 || ncol(vals) == 0) {
    return(list(vals = NULL, base_r = NULL, msg = "no values matrix"))
  }

  sample_v = vals[, ncol(vals)]
  if (!any(is.finite(sample_v))) {
    return(list(vals = NULL, base_r = NULL, msg = "tile all-NA"))
  }

  base_r = wb[[1]]
  terra::values(base_r) = NA_real_
  names(base_r) = "metric"
  rm(wb); gc(verbose = FALSE)

  list(vals = vals, base_r = base_r, msg = "")
}

# ---- Metric engine -----------------------------------------------------------
MIN_YEARS = 10L

.require_fun = function(name) {
  fn = try(get(name, envir = globalenv()), silent = TRUE)
  if (inherits(fn, "try-error") || !is.function(fn)) {
    stop(name, "() not found; check drought-functions.R")
  }
  fn
}

.spei_from_glo = function(x_vec, clim_len) {
  x = as.numeric(x_vec)
  x = x[is.finite(x)]
  if (length(x) < 3 || stats::sd(x) == 0) return(NA_real_)
  fn  = .require_fun("glo_fit_spei")
  val = try(fn(x, export_opts = "SPEI", return_latest = TRUE, climatology_length = clim_len), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) return(NA_real_)
  as.numeric(val)
}

.safe_window_sum = function(vals_mat, idx_vec) {
  sub = vals_mat[, idx_vec, drop = FALSE]
  rowSums(sub, na.rm = FALSE)
}

compute_spei_from_matrix = function(vals, dates, base_r, periods_days = NULL, clim_spec) {
  period_info       = if (is.null(periods_days)) spei_timescales_days(dates)
                      else list(lengths = periods_days, names = paste0(periods_days, "d"))
  groups_per_period = purrr::map(period_info$lengths, build_slice_groups,
                                 dates = dates, clim_spec = clim_spec)

  compute_one_period = function(p_i) {
    groups = groups_per_period[[p_i]]
    nm     = period_info$names[[p_i]]
    nm_out = paste0("spei_", nm)

    if (length(groups) < MIN_YEARS) {
      z = terra::setValues(base_r, NA_real_)
      return(setNames(list(z), nm_out))
    }

    clim_len = length(groups)
    integ = matrix(NA_real_, nrow = nrow(vals), ncol = length(groups))
    for (g in seq_along(groups)) integ[, g] = .safe_window_sum(vals, groups[[g]])

    ok_rows  = which(rowSums(is.finite(integ)) >= MIN_YEARS)
    out_spei = rep(NA_real_, nrow(integ))

    if (length(ok_rows) > 0) {
      out_spei[ok_rows] = vapply(ok_rows, function(i) .spei_from_glo(integ[i, ], clim_len), numeric(1))
    }

    r_spei = base_r
    names(r_spei) = "spei"
    r_spei = terra::setValues(r_spei, out_spei)
    setNames(list(r_spei), nm_out)
  }

  rs  = lapply(seq_along(period_info$lengths), compute_one_period)
  out = do.call(c, rs)

  any_data = any(vapply(out, .has_any_data, logical(1)))
  if (!isTRUE(any_data)) return(list(.msg = "all SPEI metrics are NA for this tile"))

  out
}

# ---- Write per-tile rasters --------------------------------------------------
.write_checked = function(r, out_path, tile_id = NA_integer_) {
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

write_spei_for_tile = function(tile_sf, pr_files, pet_files,
                               pr_idx, pet_idx, dates,
                               periods_days = NULL,
                               clim_spec,
                               out_dir = tiles_root,
                               terra_temp_dir = terra_temp) {
  out_dir = .abs_path(out_dir)
  .dir_create_base(out_dir)
  if (!.is_writable_dir(out_dir)) stop("[EACCES] tiles_root not writable: ", out_dir)

  # Step 1: read water balance tile from files
  io = read_tile_wb_from_files(
    tile_sf        = tile_sf,
    pr_files       = pr_files,
    pet_files      = pet_files,
    pr_idx         = pr_idx,
    pet_idx        = pet_idx,
    dates          = dates,
    terra_temp_dir = terra_temp_dir
  )

  if (is.null(io$vals)) {
    return(list(ok = TRUE, wrote = 0L, paths = character(0),
                tile_id = tile_sf$tile_id, msg = io$msg %||% "no tile data"))
  }

  # Step 2: compute SPEI from plain R matrix
  rs = compute_spei_from_matrix(
    vals         = io$vals,
    dates        = dates,
    base_r       = io$base_r,
    periods_days = periods_days,
    clim_spec    = clim_spec
  )

  if (is.list(rs) && length(rs) == 1 && identical(names(rs), ".msg")) {
    return(list(ok = TRUE, wrote = 0L, paths = character(0),
                tile_id = tile_sf$tile_id, msg = rs$.msg))
  }
  if (length(rs) == 0) {
    return(list(ok = TRUE, wrote = 0L, paths = character(0),
                tile_id = tile_sf$tile_id, msg = "no output"))
  }

  # Step 3: write per-period GeoTIFFs (tile subdir tagged with clim slug)
  wrote = 0L
  paths = character(0)

  for (nm in names(rs)) {
    period_dir = fs::path(out_dir, paste0(nm, "_", clim_spec$slug))
    .dir_create_base(period_dir)
    out = fs::path(period_dir, paste0("tile_", tile_sf$tile_id, ".tif"))
    .write_checked(rs[[nm]], out, tile_id = tile_sf$tile_id)
    wrote = wrote + 1L
    paths = c(paths, out)
  }

  list(ok = TRUE, wrote = wrote, paths = paths, tile_id = tile_sf$tile_id, msg = "")
}

# ---- Mosaic (GDAL VRT->COG; terra fallback) ----------------------------------
.has_gdal_utils = function() requireNamespace("gdalUtilities", quietly = TRUE)

.mosaic_vrt_to_cog = function(src_files, out_tif) {
  out_tif   = .abs_path(out_tif)
  .dir_create_base(fs::path_dir(out_tif))

  src_files = src_files[file.info(src_files)$size > 0]
  if (!length(src_files)) stop("No non-empty tiles provided for mosaic: ", out_tif)

  if (.has_gdal_utils()) {
    vrt_path = fs::path(r_temp, paste0("spei_", as.integer(stats::runif(1, 1, 1e9)), ".vrt"))
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
    ok  = fs::file_exists(out_tif) && fs::file_info(out_tif)$size > 0
    if (!ok) stop("Mosaic write failed (terra): ", out_tif)
    TRUE
  }
}

mosaic_period_dir = function(period_dir_name, last_date_iso, keep_tiles = FALSE) {
  tile_dir = .abs_path(fs::path(tiles_root, period_dir_name))
  if (!fs::dir_exists(tile_dir)) return(FALSE)

  tifs = fs::dir_ls(tile_dir, glob = "*.tif", type = "file")
  if (length(tifs) == 0) return(FALSE)

  out_tif = fs::path(conus_root, paste0(period_dir_name, ".tif"))
  message("Mosaicking ", length(tifs), " tiles for ", period_dir_name)

  ok = FALSE
  try({ ok = .mosaic_vrt_to_cog(tifs, out_tif) }, silent = FALSE)
  if (!isTRUE(ok)) return(FALSE)

  readr::write_lines(last_date_iso, fs::path(conus_root, paste0(period_dir_name, "_time.txt")))
  if (!keep_tiles) try(fs::dir_delete(tile_dir), silent = TRUE)

  message("Wrote mosaic: ", out_tif)
  TRUE
}

mosaic_all_periods_and_cleanup = function(period_dir_names, last_date_iso) {
  keep_tiles = identical(Sys.getenv("KEEP_TILES", "0"), "1")
  wrote_any  = FALSE
  for (pd in period_dir_names) {
    ok = FALSE
    try({ ok = mosaic_period_dir(pd, last_date_iso, keep_tiles = keep_tiles) }, silent = FALSE)
    wrote_any = wrote_any || isTRUE(ok)
  }
  wrote_any
}

# ---- Parallel backend --------------------------------------------------------
.run_workers = function(tiles_list, pr_files, pet_files, pr_idx, pet_idx,
                        dates, periods_days, terra_temp_dir, out_dir, cores, clim_spec) {

  message("Parallel backend: mclapply (", cores, " workers) — reading from files per worker")

  worker_fn = function(i) {
    tile = tiles_list[[i]]
    tryCatch(
      write_spei_for_tile(
        tile_sf        = tile,
        pr_files       = pr_files,
        pet_files      = pet_files,
        pr_idx         = pr_idx,
        pet_idx        = pet_idx,
        dates          = dates,
        periods_days   = periods_days,
        clim_spec      = clim_spec,
        out_dir        = out_dir,
        terra_temp_dir = terra_temp_dir
      ),
      error = function(e) list(
        ok = FALSE, wrote = 0L, paths = character(0),
        tile_id = tile$tile_id, msg = conditionMessage(e)
      )
    )
  }

  pbmcapply::pbmclapply(seq_along(tiles_list), worker_fn,
                        mc.cores = cores, mc.preschedule = FALSE,
                        ignore.interactive = TRUE)
}

# ---- Result normalizer -------------------------------------------------------
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

# ---- Runner ------------------------------------------------------------------
run_spei_metrics = function() {
  message(Sys.time(), " — Starting SPEI metrics run")
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

  periods_days = if (nzchar(periods_env)) as.numeric(strsplit(periods_env, ",")[[1]]) else NULL

  requested = as.integer(Sys.getenv("CORES", unset = "4"))
  if (is.na(requested)) requested = 4L
  cores = max(1L, min(12L, requested))
  message("Using ", cores, " worker(s).")

  # Discover files + align dates (metadata only — no raster data in main process)
  pr_files  = .list_raw_files(raw_pr_dir,  "pr")
  pet_files = .list_raw_files(raw_pet_dir, "pet")

  aln           = align_pr_pet_dates(pr_files, pet_files)
  dates         = aln$dates
  pr_idx        = aln$pr_idx
  pet_idx       = aln$pet_idx
  last_date_iso = format(max(dates, na.rm = TRUE))
  message("Aligned last date: ", last_date_iso)

  # Build tile grid from a single lightweight PR layer
  r_meta = terra::rast(paste0("NETCDF:", as.character(pr_files[[1]]), ":precipitation_amount"))[[1]]
  tiles  = build_tiles_from_extent(dx = dx, dy = dy, r_for_align = r_meta)
  rm(r_meta); gc(verbose = FALSE)

  if (nzchar(tile_ids_env)) {
    keep_ids = as.integer(strsplit(tile_ids_env, ",")[[1]])
    tiles    = tiles[tiles$tile_id %in% keep_ids, ]
  }

  n_tiles = nrow(tiles)
  message("Processing ", n_tiles, " tile(s)…")
  if (n_tiles == 0) stop("No tiles to process (extent/params mismatch).")

  tiles_list = lapply(seq_len(n_tiles), function(i) tiles[i, ])

  clim_periods = parse_clim_periods(Sys.getenv("CLIM_PERIODS", unset = "rolling:30"))
  message("Reference periods: ", paste(sapply(clim_periods, `[[`, "slug"), collapse = ", "))

  for (clim_spec in clim_periods) {
    message("--- Running period: ", clim_spec$slug, " ---")

    results_raw = .run_workers(
      tiles_list     = tiles_list,
      pr_files       = pr_files,
      pet_files      = pet_files,
      pr_idx         = pr_idx,
      pet_idx        = pet_idx,
      dates          = dates,
      periods_days   = periods_days,
      terra_temp_dir = terra_temp,
      out_dir        = tiles_root,
      cores          = cores,
      clim_spec      = clim_spec
    )

    res       = .normalize_results(results_raw)
    ok_vec    = vapply(res, function(x) isTRUE(x$ok), TRUE)
    wrote_ct  = vapply(res, function(x) as.integer(x$wrote), 0L)
    all_paths = unlist(lapply(res, `[[`, "paths"), use.names = FALSE)

    errs = unique(vapply(res[!ok_vec], function(x) as.character(x$msg)[1], ""))
    errs = errs[nzchar(errs)]

    message(sprintf("Tiles OK: %d/%d; total files written: %d",
                    sum(ok_vec), n_tiles, sum(wrote_ct)))

    skipped_msgs = unique(vapply(res[ok_vec], function(x) as.character(x$msg)[1], ""))
    skipped_msgs = skipped_msgs[nzchar(skipped_msgs)]
    if (length(skipped_msgs) > 0) {
      message("Skip reasons (ok=TRUE, wrote=0):")
      for (m in utils::head(skipped_msgs, 10)) message("  • ", m)
    }

    if (length(errs) > 0) {
      message("Errors:")
      for (e in utils::head(errs, 12)) message("  • ", e)
    }

    .dir_create_base(conus_root)

    period_dir_names = unique(basename(fs::path_dir(all_paths)))
    if (!length(period_dir_names) && fs::dir_exists(tiles_root)) {
      all_dirs = basename(fs::dir_ls(tiles_root, type = "directory"))
      period_dir_names = all_dirs[grepl(paste0("_", clim_spec$slug, "$"), all_dirs)]
    }

    message("Mosaicking ", length(period_dir_names), " period(s) to ", conus_root)

    mosaic_any = mosaic_all_periods_and_cleanup(period_dir_names, last_date_iso)
    if (!mosaic_any) message("No mosaics written (no usable tiles found).")
  }

  elapsed_min = round((proc.time() - t_start)[["elapsed"]] / 60, 1)
  message(Sys.time(), " — SPEI metrics run complete. Elapsed: ", elapsed_min, " min.")
}

if (sys.nframe() == 0) run_spei_metrics()