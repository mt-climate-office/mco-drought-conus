##############################################################
# File: R/precipitation-metrics.R
# Title: Precip metrics (CONUS, tiled, parallel<=12) from local GridMET:
#        SPI (gamma), % of normal, deviation from normal, percentile.
#        30-year calendar reference; per-tile COGs; VRT→COG mosaics.
# Author: Dr. Zachary H. Hoylman
# Date: 10-28-2025
# Conventions: "=", |> , explicit pkg::fun namespaces.
##############################################################

`%||%` = function(a, b) if (is.null(a)) b else a
.abs_path = function(p) as.character(fs::path_abs(fs::path_expand(p)))

# --- Paths --------------------------------------------------------------------
project_root = "~/mco-drought-conus"
data_root    = "~/mco-drought-conus-data"

raw_dir      = .abs_path(fs::path(data_root, "raw"))
interim_dir  = .abs_path(fs::path(data_root, "interim"))
derived_dir  = .abs_path(fs::path(data_root, "derived"))

tiles_root   = .abs_path(fs::path(derived_dir, "precip_metrics"))          # temp tiles (all metrics)
conus_root   = .abs_path(fs::path(derived_dir, "conus_drought"))           # mosaics root
merged_pr    = .abs_path(fs::path(interim_dir, "gridmet", "pr", "merged", "gridmet_pr.nc"))

invisible(fs::dir_create(c(raw_dir, interim_dir, derived_dir, tiles_root, conus_root)))

# --- Load your drought functions (gamma_fit_spi, etc.) ------------------------
source_drought_functions = function(path = fs::path(project_root, "drought-functions.R")) {
  path = .abs_path(path)
  if (fs::file_exists(path)) source(path, local = TRUE)
  invisible(TRUE)
}
source_drought_functions()

# --- Helpers ------------------------------------------------------------------
dates_from_layernames = function(spat) {
  nms = names(spat)
  digits = gsub("[^0-9]", "", nms)
  as.Date("1900-01-01") + suppressWarnings(as.numeric(digits))
}

conus_geometry = function() {
  st = rnaturalearth::ne_states(country = "United States of America", returnclass = "sf")
  st_l48 = st[!(st$name %in% c("Alaska","Hawaii","Puerto Rico")), ]
  sf::st_union(sf::st_make_valid(st_l48)) |> sf::st_as_sf()
}

# Env: EDGE_BUF_DEG (default 0.1); CLIP_TO_CONUS ("1" to intersect)
build_tiles_from_extent = function(dx = 2, dy = 2, r_for_align, edge_buf_deg = NULL) {
  rs  = terra::res(r_for_align)                 # ~0.0416667
  ex  = terra::ext(r_for_align)
  buf = as.numeric(Sys.getenv("EDGE_BUF_DEG", "0.1"))
  if (!is.null(edge_buf_deg)) buf = edge_buf_deg
  exb = terra::ext(ex$xmin - buf, ex$xmax + buf, ex$ymin - buf, ex$ymax + buf)
  
  nx  = max(1L, round(dx / rs[1])); ny  = max(1L, round(dy / rs[2]))
  dxA = nx * rs[1]; dyA = ny * rs[2]
  
  xs = seq(exb$xmin, exb$xmax, by = dxA); if (tail(xs,1) < exb$xmax - 1e-9) xs = c(xs, exb$xmax)
  ys = seq(exb$ymin, exb$ymax, by = dyA); if (tail(ys,1) < exb$ymax - 1e-9) ys = c(ys, exb$ymax)
  
  eps = 1e-6
  polys = vector("list", (length(xs)-1)*(length(ys)-1)); k = 0L
  for (i in seq_len(length(xs)-1)) for (j in seq_len(length(ys)-1)) {
    k = k + 1L
    xL = xs[i] - eps; xR = xs[i+1] + eps
    yB = ys[j] - eps; yT = ys[j+1] + eps
    polys[[k]] = sf::st_polygon(list(matrix(c(xL,yB, xR,yB, xR,yT, xL,yT, xL,yB), ncol=2, byrow=TRUE)))
  }
  tiles = sf::st_sf(tile_id = seq_along(polys), geometry = sf::st_sfc(polys, crs = 4326))
  if (identical(Sys.getenv("CLIP_TO_CONUS", "0"), "1")) {
    mask = conus_geometry()
    tiles = suppressWarnings(sf::st_intersection(sf::st_make_valid(tiles), mask)) |> sf::st_as_sf()
  }
  tiles
}

# 30-year calendar ref
last_30y_indices = function(dates) {
  d_last  = max(dates, na.rm = TRUE)
  md_last = format(d_last, "%m-%d")
  idx     = which(format(dates, "%m-%d") == md_last)
  yrs     = as.integer(format(dates[idx], "%Y"))
  keep    = which(yrs >= (as.integer(format(d_last, "%Y")) - 29))
  idx[keep]
}
build_slice_groups = function(n_days, dates) {
  anchors = last_30y_indices(dates)
  purrr::compact(purrr::map(anchors, function(end_i) {
    start_i = end_i - (n_days - 1)
    if (start_i < 1) return(NULL)
    seq(start_i, end_i)
  }))
}
spi_timescales_days = function(dates) {
  md = format(dates, "%m-%d")
  wy_len  = (length(md) - tail(which(md == "10-01"), 1)) + 1
  ytd_len = (length(md) - tail(which(md == "01-01"), 1)) + 1
  list(
    lengths = c(15,30,45,60,90,120,180,365,730, wy_len, ytd_len),
    names   = c("15d","30d","45d","60d","90d","120d","180d","365d","730d","wy","ytd")
  )
}

# --- Read PR with terra -------------------------------------------------------
read_pr_terra_full = function(path = merged_pr) {
  path = .abs_path(path)
  if (!fs::file_exists(path)) stop("Merged PR not found: ", path)
  r = terra::rast(path)
  d = dates_from_layernames(r)
  if (terra::nlyr(r) != length(d)) {
    stop("Layer/time length mismatch: nlyr=", terra::nlyr(r), " vs length(dates)=", length(d))
  }
  list(r = r, dates = d, path = path)
}
read_pr_terra_tile = function(tile_sf) {
  io = read_pr_terra_full()
  tile_sf = sf::st_transform(sf::st_buffer(sf::st_make_valid(tile_sf), 1e-6), 4326)
  v_tile  = terra::vect(tile_sf)
  r_tile  = try(terra::crop(io$r, v_tile, snap = "out") |> terra::mask(v_tile), silent = TRUE)
  if (inherits(r_tile, "try-error")) return(list(r = NULL, dates = io$dates))
  dxy = dim(r_tile)
  if (is.null(dxy) || prod(dxy[1:2]) == 0 || terra::nlyr(r_tile) == 0) return(list(r = NULL, dates = io$dates))
  list(r = r_tile, dates = io$dates)
}

# --- Metric engines -----------------------------------------------------------
MIN_YEARS = 10L

.require_fun = function(name) {
  fn = try(get(name), silent = TRUE)
  if (inherits(fn, "try-error") || !is.function(fn)) stop(name, "() not found; check drought-functions.R")
  fn
}

.spi_from_gamma = function(x_vec) {
  x = as.numeric(x_vec); x = x[is.finite(x)]
  if (length(x) < 3 || all(x == 0) || sd(x) == 0) return(NA_real_)
  fn = .require_fun("gamma_fit_spi")
  val = try(fn(x, export_opts = "SPI", return_latest = TRUE, climatology_length = 30), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) return(NA_real_)
  as.numeric(val)
}
.pon_latest    = function(x_vec) { val = try(percent_of_normal(x_vec, 30), silent = TRUE); if (inherits(val,"try-error")||!is.finite(val)) NA_real_ else as.numeric(val) }
.dev_latest    = function(x_vec) { val = try(deviation_from_normal(x_vec, 30), silent = TRUE); if (inherits(val,"try-error")||!is.finite(val)) NA_real_ else as.numeric(val) }
.pctile_latest = function(x_vec) { val = try(compute_percentile(x_vec, 30), silent = TRUE);    if (inherits(val,"try-error")||!is.finite(val)) NA_real_ else as.numeric(val) }

metrics_for_tile_daily = function(tile_sf, periods_days = NULL) {
  io = read_pr_terra_tile(tile_sf); if (is.null(io$r)) return(list())
  r = io$r; dates = io$dates
  vals = terra::values(r, mat = TRUE) # [cells x time]
  if (is.null(vals)) return(list())
  
  period_info = if (is.null(periods_days)) spi_timescales_days(dates)
  else list(lengths = periods_days, names = paste0(periods_days, "d"))
  
  groups_per_period = purrr::map(period_info$lengths, build_slice_groups, dates = dates)
  
  compute_one_period = function(p_i) {
    groups = groups_per_period[[p_i]]
    nm = period_info$names[[p_i]]
    
    # desired output layer names
    spi_nm = paste0("spi_", nm)
    pon_nm = paste0("precip_pon_", nm)
    dev_nm = paste0("precip_dev_", nm)
    pct_nm = paste0("precip_pctile_", nm)
    
    base = r[[1]]; names(base) = "metric"
    
    if (length(groups) < MIN_YEARS) {
      z = terra::setValues(base, NA_real_)
      return(setNames(list(z, z, z, z), c(spi_nm, pon_nm, dev_nm, pct_nm)))
    }
    
    integ = matrix(NA_real_, nrow = nrow(vals), ncol = length(groups))
    for (g in seq_along(groups)) integ[, g] = rowSums(vals[, groups[[g]], drop = FALSE], na.rm = TRUE)
    
    ok_rows = which(rowSums(is.finite(integ)) >= MIN_YEARS)
    
    out_spi = out_pon = out_dev = out_pct = rep(NA_real_, nrow(integ))
    if (length(ok_rows) > 0) {
      out_spi[ok_rows] = vapply(ok_rows, function(i) .spi_from_gamma(integ[i, ]), numeric(1))
      out_pon[ok_rows] = vapply(ok_rows, function(i) .pon_latest(integ[i, ]),   numeric(1))
      out_dev[ok_rows] = vapply(ok_rows, function(i) .dev_latest(integ[i, ]),   numeric(1))
      out_pct[ok_rows] = vapply(ok_rows, function(i) .pctile_latest(integ[i, ]),numeric(1))
    }
    
    r_spi = r_pon = r_dev = r_pct = base
    names(r_spi) = "spi";    r_spi = terra::setValues(r_spi, out_spi)
    names(r_pon) = "pon";    r_pon = terra::setValues(r_pon, out_pon)
    names(r_dev) = "dev";    r_dev = terra::setValues(r_dev, out_dev)
    names(r_pct) = "pctile"; r_pct = terra::setValues(r_pct, out_pct)
    
    setNames(list(r_spi, r_pon, r_dev, r_pct), c(spi_nm, pon_nm, dev_nm, pct_nm))
  }
  
  rs = lapply(seq_along(period_info$lengths), compute_one_period)
  do.call(c, rs) # flatten one level
}

# --- Write per-tile COGs ------------------------------------------------------
.write_checked = function(r, out_path) {
  out_path = .abs_path(out_path)
  fs::dir_create(fs::path_dir(out_path))
  terra::NAflag(r) = -9999
  ok = FALSE
  try({ terra::writeRaster(r, out_path, filetype = "COG", overwrite = TRUE) ; ok = TRUE }, silent = TRUE)
  if (!ok) terra::writeRaster(r, out_path, overwrite = TRUE, gdal = c("COMPRESS=LZW","BIGTIFF=IF_SAFER"))
  if (!fs::file_exists(out_path) || fs::file_info(out_path)$size <= 0) stop("Write failed or empty file: ", out_path)
  TRUE
}
write_metrics_tiles_daily = function(tile_sf, periods_days = NULL, out_dir = tiles_root) {
  out_dir = .abs_path(out_dir); fs::dir_create(out_dir)
  rs = metrics_for_tile_daily(tile_sf = tile_sf, periods_days = periods_days)
  if (length(rs) == 0) { message("Tile ", tile_sf$tile_id, ": no overlap — skipping."); return(list(ok=TRUE,wrote=0L,paths=character(0),tile_id=tile_sf$tile_id,msg="no-overlap")) }
  wrote = 0L; paths = character(0)
  for (nm in names(rs)) {
    period_dir = fs::path(out_dir, nm); fs::dir_create(period_dir)
    out = fs::path(period_dir, paste0("tile_", tile_sf$tile_id, ".tif"))
    .write_checked(rs[[nm]], out)
    wrote = wrote + 1L; paths = c(paths, out)
  }
  list(ok = TRUE, wrote = wrote, paths = paths, tile_id = tile_sf$tile_id, msg = "")
}

# --- Mosaic (GDAL VRT→COG; terra fallback) -----------------------------------
.has_gdal_utils = function() requireNamespace("gdalUtilities", quietly = TRUE)

.mosaic_vrt_to_cog = function(src_files, out_tif) {
  out_tif = .abs_path(out_tif)
  fs::dir_create(fs::path_dir(out_tif))
  
  if (.has_gdal_utils()) {
    vrt_path = fs::path_temp(paste0("precip_", as.integer(runif(1,1,1e9))), ext = ".vrt")
    on.exit(try(fs::file_delete(vrt_path), silent = TRUE), add = TRUE)
    # keep the simple, previously-working call
    gdalUtilities::gdalbuildvrt(gdalfile = src_files, output.vrt = vrt_path)
    gdalUtilities::gdal_translate(src_dataset = vrt_path, dst_dataset = out_tif, of = "COG",
                                  co = c("COMPRESS=LZW","PREDICTOR=2","BIGTIFF=IF_SAFER"))
    ok = fs::file_exists(out_tif) && fs::file_info(out_tif)$size > 0
    if (!ok) stop("Mosaic write failed (GDAL): ", out_tif)
    TRUE
  } else {
    # Fallback: terra mosaic (mean over overlaps) — FIXED argument construction
    sr  = terra::sprc(src_files)
    mos = try(do.call(terra::mosaic, list(sr, fun = "mean", na.rm = TRUE)), silent = TRUE)
    if (inherits(mos, "try-error")) {
      # try an alternate path: read as SpatRaster stack and mosaic
      rr  = terra::rast(src_files)
      mos = terra::mosaic(rr, fun = "mean", na.rm = TRUE)
    }
    terra::NAflag(mos) = -9999
    terra::writeRaster(mos, out_tif, filetype = "COG", overwrite = TRUE)
    ok = fs::file_exists(out_tif) && fs::file_info(out_tif)$size > 0
    if (!ok) stop("Mosaic write failed (terra): ", out_tif)
    TRUE
  }
}

# Tiny feather (optional)
.feather_mosaic = function(tif_path) {
  if (identical(Sys.getenv("FEATHER_SEAMS", "1"), "0")) return(invisible(TRUE))
  k = as.integer(Sys.getenv("FEATHER_SIZE", "3")); if (k %% 2 == 0) k = k + 1L
  r = terra::rast(tif_path)
  if (!is.numeric(terra::minmax(r)[1])) return(invisible(TRUE))
  w = matrix(1, nrow = k, ncol = k)
  r2 = terra::focal(r, w = w, fun = median, na.policy = "omit", na.rm = TRUE, pad = TRUE)
  tmp = fs::path_temp(paste0("feather_", basename(tif_path)))
  terra::NAflag(r2) = -9999
  terra::writeRaster(r2, tmp, filetype = "COG", overwrite = TRUE,
                     gdal = c("COMPRESS=LZW","PREDICTOR=2","BIGTIFF=IF_SAFER"))
  fs::file_move(tmp, tif_path)
  invisible(TRUE)
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
  if (!isTRUE(ok)) { warning("Mosaic failed for ", period_dir_name); return(FALSE) }
  
  try(.feather_mosaic(out_tif), silent = TRUE)
  readr::write_lines(last_date_iso, fs::path(conus_root, paste0(period_dir_name, "_time.txt")))
  
  if (!keep_tiles) { message("Deleting tiles dir: ", tile_dir); fs::dir_delete(tile_dir) }
  message("Wrote mosaic: ", out_tif)
  TRUE
}

mosaic_all_periods_and_cleanup = function(period_dir_names, last_date_iso) {
  keep_tiles = identical(Sys.getenv("KEEP_TILES", "0"), "1")
  wrote_any = FALSE
  for (pd in period_dir_names) {
    ok = FALSE
    try({ ok = mosaic_period_dir(pd, last_date_iso, keep_tiles = keep_tiles) }, silent = FALSE)
    wrote_any = wrote_any || isTRUE(ok)
  }
  wrote_any
}

# --- Runner -------------------------------------------------------------------
run_precip_metrics = function() {
  message(Sys.time(), " — Starting precipitation metrics run")
  Sys.setenv(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1")
  
  dx = as.numeric(Sys.getenv("TILE_DX", unset = "2"))
  dy = as.numeric(Sys.getenv("TILE_DY", unset = "2"))
  periods_env = Sys.getenv("PERIODS_DAYS", unset = "")
  tile_ids_env= Sys.getenv("TILE_IDS", unset = "")
  
  periods_days = if (nzchar(periods_env)) as.numeric(strsplit(periods_env, ",")[[1]]) else NULL
  
  requested = as.integer(Sys.getenv("CORES", unset = "12")); if (is.na(requested)) requested = 12
  cores = max(1, min(12, requested))
  message("Using ", cores, " worker(s).")
  
  io_full = read_pr_terra_full(merged_pr)
  rfull   = io_full$r
  dates   = io_full$dates
  last_date_iso = format(max(dates, na.rm = TRUE))
  message("Merged PR last date: ", last_date_iso)
  
  tiles = build_tiles_from_extent(dx = dx, dy = dy, r_for_align = rfull)
  if (nzchar(tile_ids_env)) { keep_ids = as.integer(strsplit(tile_ids_env, ",")[[1]]); tiles = tiles[tiles$tile_id %in% keep_ids, ] }
  n_tiles = nrow(tiles)
  message("Processing ", n_tiles, " tile(s)…")
  if (n_tiles == 0) stop("No tiles to process (extent/params mismatch).")
  
  worker_fun = function(i) {
    tile = tiles[i, ]
    tryCatch(write_metrics_tiles_daily(tile_sf = tile, periods_days = periods_days, out_dir = tiles_root),
             error = function(e) list(ok = FALSE, wrote = 0L, paths = character(0),
                                      tile_id = tile$tile_id, msg = conditionMessage(e)))
  }
  
  results_raw = NULL
  use_mclapply = .Platform$OS.type != "windows" && ("mc.cores" %in% names(formals(parallel::mclapply)))
  if (use_mclapply) {
    results_raw = try(parallel::mclapply(seq_len(n_tiles), worker_fun, mc.cores = cores, mc.preschedule = FALSE), silent = TRUE)
    if (inherits(results_raw, "try-error")) use_mclapply = FALSE
  }
  if (!use_mclapply) {
    cl = parallel::makeCluster(cores)
    on.exit(try(parallel::stopCluster(cl), silent = TRUE), add = TRUE)
    parallel::clusterExport(cl,
                            varlist = c("tiles","n_tiles","periods_days","tiles_root","write_metrics_tiles_daily",
                                        "read_pr_terra_tile","read_pr_terra_full","metrics_for_tile_daily",".spi_from_gamma",
                                        "build_slice_groups","last_30y_indices","dates_from_layernames","merged_pr",
                                        "spi_timescales_days","MIN_YEARS",".abs_path",".write_checked","%||%"),
                            envir = environment())
    parallel::clusterEvalQ(cl, {
      suppressPackageStartupMessages({ library(terra); library(sf); library(fs); library(purrr) })
      source(file.path(path.expand("~"), "mco-drought-conus", "drought-functions.R"))
      TRUE
    })
    results_raw = parallel::parLapplyLB(cl, seq_len(n_tiles), worker_fun)
  }
  
  res = lapply(results_raw, function(x){
    if (!is.list(x)) return(list(ok=FALSE,wrote=0L,paths=character(0),tile_id=NA,msg=as.character(x)))
    x$ok = isTRUE(x$ok); x$wrote = as.integer(x$wrote %||% 0L)
    x$paths = as.character(x$paths %||% character(0))
    x$tile_id = suppressWarnings(as.integer(x$tile_id))
    x$msg = as.character(x$msg %||% ""); x
  })
  ok_vec   = vapply(res, function(x) x$ok, TRUE)
  wrote_ct = vapply(res, function(x) x$wrote, 0L)
  all_errs = unique(vapply(res[!ok_vec], function(x) x$msg, ""))
  all_paths= unlist(lapply(res, `[[`, "paths"), use.names = FALSE)
  
  message(sprintf("Tiles OK: %d/%d; total files written: %d", sum(ok_vec), n_tiles, sum(wrote_ct)))
  if (length(all_errs) > 0) { message("First few unique errors:"); for (e in utils::head(all_errs, 6)) message("  • ", e) }
  
  # Mosaics
  fs::dir_create(conus_root)
  written_dirs_full = unique(fs::path_dir(all_paths))
  period_dir_names  = unique(basename(written_dirs_full))
  if (!length(period_dir_names) && fs::dir_exists(tiles_root))
    period_dir_names = basename(fs::dir_ls(tiles_root, type = "directory"))
  message("Mosaicking ", length(period_dir_names), " period(s) to ", conus_root)
  
  mosaic_any = mosaic_all_periods_and_cleanup(period_dir_names, last_date_iso)
  if (!mosaic_any) message("No mosaics written (no tiles found?).")
  message(Sys.time(), " — precipitation metrics run complete.")
}

# Auto-run if called via Rscript
if (sys.nframe() == 0) run_precip_metrics()