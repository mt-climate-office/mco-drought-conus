##############################################################
# File: R/metrics_spei.R
# Title: SPEI (CONUS, tiled, parallel<=12) using LOCAL GridMET (NetCDF),
#        30-year calendar reference, glo_fit_spei(); per-tile COGs,
#        mosaics to CONUS (by period), CONUS mask.
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

tiles_root   = .abs_path(fs::path(derived_dir, "spei"))                     # temp tiles
conus_root   = .abs_path(fs::path(derived_dir, "conus_drought", "spei"))    # mosaics

merged_pr    = .abs_path(fs::path(interim_dir, "gridmet", "pr",  "merged", "gridmet_pr.nc"))
merged_pet   = .abs_path(fs::path(interim_dir, "gridmet", "pet", "merged", "gridmet_pet.nc"))

invisible(fs::dir_create(c(raw_dir, interim_dir, derived_dir, tiles_root, conus_root)))

# --- Load drought functions (expects glo_fit_spei) ----------------------------
source_drought_functions = function(path = fs::path(project_root, "drought-functions.R")) {
  path = .abs_path(path)
  if (fs::file_exists(path)) source(path, local = TRUE)
  invisible(TRUE)
}
source_drought_functions()

.require_glo_fun = function() {
  fn = try(get("glo_fit_spei"), silent = TRUE)
  if (inherits(fn, "try-error") || !is.function(fn)) {
    stop("glo_fit_spei() not found or not a function. Make sure drought-functions.R is sourced.")
  }
  fn
}

# --- Dates helper (IDENTICAL logic to precipitation script) -------------------
dates_from_layernames = function(spat) {
  nms = names(spat)
  digits = gsub("[^0-9]", "", nms)
  as.Date("1900-01-01") + suppressWarnings(as.numeric(digits))
}

# robust-ish: prefer layer-name decoding; fall back to terra::time()
.read_nc_with_dates = function(path) {
  r = terra::rast(.abs_path(path))
  d = dates_from_layernames(r)
  # Fallback only if the name-based decode failed
  if (all(is.na(d))) {
    tt = try(terra::time(r), silent = TRUE)
    if (!inherits(tt, "try-error") && length(tt)) {
      d = as.Date(tt)
    }
  }
  list(r = r, dates = d)
}

# --- Align PR & PET -----------------------------------------------------------
read_pr_pet_aligned = function() {
  io_pr  = .read_nc_with_dates(merged_pr)
  io_pet = .read_nc_with_dates(merged_pet)
  
  pr_ok  = length(io_pr$dates)  && any(is.finite(as.numeric(io_pr$dates)))
  pet_ok = length(io_pet$dates) && any(is.finite(as.numeric(io_pet$dates)))
  message(sprintf("PR range:  %s .. %s (%d layers)",
                  if (pr_ok) format(min(io_pr$dates), "%Y-%m-%d") else "NA",
                  if (pr_ok) format(max(io_pr$dates), "%Y-%m-%d") else "NA",
                  terra::nlyr(io_pr$r)))
  message(sprintf("PET range: %s .. %s (%d layers)",
                  if (pet_ok) format(min(io_pet$dates), "%Y-%m-%d") else "NA",
                  if (pet_ok) format(max(io_pet$dates), "%Y-%m-%d") else "NA",
                  terra::nlyr(io_pet$r)))
  if (!pr_ok || !pet_ok) stop("Could not parse PR or PET dates from layer names.")
  
  # Build index tables and inner-join on date to guarantee valid, aligned indices
  pr_df  = data.frame(date = io_pr$dates,  i_pr  = seq_len(terra::nlyr(io_pr$r)))
  pet_df = data.frame(date = io_pet$dates, i_pet = seq_len(terra::nlyr(io_pet$r)))
  
  # Remove duplicated dates within each source (keep first occurrence); warn if any
  if (anyDuplicated(pr_df$date))  warning("Duplicate PR dates found; keeping first occurrence for each date.")
  if (anyDuplicated(pet_df$date)) warning("Duplicate PET dates found; keeping first occurrence for each date.")
  pr_df  = pr_df[!duplicated(pr_df$date), , drop = FALSE]
  pet_df = pet_df[!duplicated(pet_df$date), , drop = FALSE]
  
  aln = merge(pr_df, pet_df, by = "date", all = FALSE, sort = TRUE)
  if (!nrow(aln)) stop("PR/PET date alignment failed (no overlapping dates).")
  
  # Ensure strictly increasing by date
  aln = aln[order(aln$date), , drop = FALSE]
  
  # Safe, NA-free, in-bounds indices
  idx_pr  = aln$i_pr
  idx_pet = aln$i_pet
  
  # Final safety checks
  if (length(idx_pr) != length(idx_pet)) stop("PR/PET index length mismatch after alignment.")
  if (length(idx_pr) == 0L) stop("PR/PET date alignment yielded zero rows.")
  
  # Subset rasters using guaranteed-good indices
  r_pr_aln  = io_pr$r[[idx_pr]]
  r_pet_aln = io_pet$r[[idx_pet]]
  
  list(r_pr = r_pr_aln, r_pet = r_pet_aln, dates = aln$date)
}

# --- Geometry + tiles ---------------------------------------------------------
conus_geometry = function() {
  st = rnaturalearth::ne_states(country = "United States of America", returnclass = "sf")
  st_l48 = st[!(st$name %in% c("Alaska","Hawaii","Puerto Rico")), ]
  sf::st_union(sf::st_make_valid(st_l48)) |> sf::st_as_sf()
}
.mask_to_conus = function(r) terra::mask(r, terra::vect(conus_geometry()))

# Env override:
#   EDGE_BUF_DEG  — buffer (deg) added around raster extent (default 0.1)
#   CLIP_TO_CONUS — "1" to intersect tiles with CONUS mask after building (default "0")
build_tiles_from_extent = function(dx = 2, dy = 2, r_for_align, edge_buf_deg = NULL) {
  rs  = terra::res(r_for_align)
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

# --- Time windows (as in SPI script) ------------------------------------------
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
spei_timescales_days = function(dates) {
  md = format(dates, "%m-%d")
  wy_len  = (length(md) - tail(which(md == "10-01"), 1)) + 1
  ytd_len = (length(md) - tail(which(md == "01-01"), 1)) + 1
  list(
    lengths = c(15,30,45,60,90,120,180,365,730, wy_len, ytd_len),
    names   = c("15d","30d","45d","60d","90d","120d","180d","365d","730d","wy","ytd")
  )
}

# --- SPEI core (terra in/out) -------------------------------------------------
MIN_YEARS = 10L
.spei_from_glo = function(x_vec) {
  x = as.numeric(x_vec); x = x[is.finite(x)]
  if (length(x) < 3 || sd(x) == 0) return(NA_real_)
  fn = .require_glo_fun()
  val = try(fn(x, export_opts = "SPEI", return_latest = TRUE, climatology_length = 30), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) return(NA_real_)
  as.numeric(val)
}

spei_for_tile_daily = function(tile_sf, periods_days = NULL, r_pr, r_pet, dates) {
  tile_sf = sf::st_transform(sf::st_buffer(sf::st_make_valid(tile_sf), 1e-6), 4326)
  v_tile  = terra::vect(tile_sf)
  r_pr_t  = try(terra::mask(terra::crop(r_pr,  v_tile, snap = "out"), v_tile), silent = TRUE)
  r_pet_t = try(terra::mask(terra::crop(r_pet, v_tile, snap = "out"), v_tile), silent = TRUE)
  if (inherits(r_pr_t, "try-error") || inherits(r_pet_t, "try-error") ||
      terra::nlyr(r_pr_t) == 0 || terra::nlyr(r_pet_t) == 0) return(list())
  vals = terra::values(r_pr_t - r_pet_t, mat = TRUE)  # [cells x time]
  if (is.null(vals)) return(list())
  
  period_info = if (is.null(periods_days)) spei_timescales_days(dates)
  else list(lengths = periods_days, names = paste0(periods_days, "d"))
  groups_per_period = purrr::map(period_info$lengths, build_slice_groups, dates = dates)
  
  compute_period = function(p_i) {
    groups = groups_per_period[[p_i]]; nm = period_info$names[[p_i]]
    base = r_pr_t[[1]]; names(base) = "spei"
    if (length(groups) < MIN_YEARS) return(setNames(list(terra::setValues(base, NA_real_)), paste0("spei_", nm))[[1]])
    integ = matrix(NA_real_, nrow = nrow(vals), ncol = length(groups))
    for (g in seq_along(groups)) integ[, g] = rowSums(vals[, groups[[g]], drop = FALSE], na.rm = TRUE)
    out = rep(NA_real_, nrow(integ))
    ok  = which(rowSums(is.finite(integ)) >= MIN_YEARS)
    if (length(ok)) out[ok] = vapply(ok, function(i) .spei_from_glo(integ[i, ]), numeric(1))
    terra::setValues(base, out)
  }
  
  rs = lapply(seq_along(period_info$lengths), compute_period)
  names(rs) = paste0("spei_", period_info$names)
  rs
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
write_spei_tiles_daily = function(tile_sf, periods_days = NULL, out_dir = tiles_root, r_pr, r_pet, dates) {
  rs = spei_for_tile_daily(tile_sf = tile_sf, periods_days = periods_days, r_pr = r_pr, r_pet = r_pet, dates = dates)
  if (length(rs) == 0) return(list(ok=TRUE,wrote=0L,paths=character(0),tile_id=tile_sf$tile_id,msg="no-overlap"))
  wrote = 0L; paths = character(0)
  for (nm in names(rs)) {
    period_dir = fs::path(out_dir, nm); fs::dir_create(period_dir)
    out = fs::path(period_dir, paste0("tile_", tile_sf$tile_id, ".tif"))
    .write_checked(rs[[nm]], out)
    wrote = wrote + 1L; paths = c(paths, out)
  }
  list(ok = TRUE, wrote = wrote, paths = paths, tile_id = tile_sf$tile_id, msg = "")
}

# --- Mosaic (GDAL VRT→COG) + CONUS mask --------------------------------------
.has_gdal_utils = function() requireNamespace("gdalUtilities", quietly = TRUE)
.mosaic_vrt_to_cog = function(src_files, out_tif) {
  out_tif = .abs_path(out_tif)
  fs::dir_create(fs::path_dir(out_tif))
  if (.has_gdal_utils()) {
    vrt_path = fs::path_temp(paste0("spei_", as.integer(runif(1,1,1e9))), ext = ".vrt")
    on.exit(try(fs::file_delete(vrt_path), silent = TRUE), add = TRUE)
    gdalUtilities::gdalbuildvrt(gdalfile = src_files, output.vrt = vrt_path)
    gdalUtilities::gdal_translate(src_dataset = vrt_path, dst_dataset = out_tif, of = "COG",
                                  co = c("COMPRESS=LZW","PREDICTOR=2","BIGTIFF=IF_SAFER"))
    ok = fs::file_exists(out_tif) && fs::file_info(out_tif)$size > 0
    if (!ok) stop("Mosaic write failed (GDAL): ", out_tif)
    TRUE
  } else {
    sr  = terra::sprc(src_files)
    mos = do.call(terra::mosaic, c(sr, list(fun = "mean", na.rm = TRUE)))
    terra::NAflag(mos) = -9999
    terra::writeRaster(mos, out_tif, filetype = "COG", overwrite = TRUE)
    ok = fs::file_exists(out_tif) && fs::file_info(out_tif)$size > 0
    if (!ok) stop("Mosaic write failed (terra): ", out_tif)
    TRUE
  }
}
.mask_conus_file = function(tif_path) {
  r = terra::rast(tif_path)
  r2 = .mask_to_conus(r)
  tmp = fs::path_temp(paste0("mask_", basename(tif_path)))
  terra::NAflag(r2) = -9999
  terra::writeRaster(r2, tmp, filetype = "COG", overwrite = TRUE,
                     gdal = c("COMPRESS=LZW","PREDICTOR=2","BIGTIFF=IF_SAFER"))
  fs::file_move(tmp, tif_path)
}

mosaic_period_dir = function(period_dir_name, last_date_iso, keep_tiles = FALSE) {
  tile_dir = .abs_path(fs::path(tiles_root, period_dir_name))
  if (!fs::dir_exists(tile_dir)) return(FALSE)
  tifs = fs::dir_ls(tile_dir, glob = "*.tif", type = "file")
  if (length(tifs) == 0) return(FALSE)
  
  message("Mosaicking ", length(tifs), " tiles for ", period_dir_name)
  out_tif = fs::path(conus_root, paste0(period_dir_name, ".tif"))
  ok = FALSE
  try({ ok = .mosaic_vrt_to_cog(tifs, out_tif) }, silent = FALSE)
  if (!isTRUE(ok)) { warning("Mosaic failed for ", period_dir_name); return(FALSE) }
  
  # mask to CONUS
  try(.mask_conus_file(out_tif), silent = TRUE)
  
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

# --- Runner (≤12 cores; mclapply→PSOCK fallback + mosaics) --------------------
if (sys.nframe() == 0) {
  message(Sys.time(), " — Starting SPEI run from metrics_spei.R")
  Sys.setenv(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1")
  
  dx = as.numeric(Sys.getenv("TILE_DX", unset = "2"))
  dy = as.numeric(Sys.getenv("TILE_DY", unset = "2"))
  periods_env = Sys.getenv("PERIODS_DAYS", unset = "")
  tile_ids_env= Sys.getenv("TILE_IDS", unset = "")
  periods_days = if (nzchar(periods_env)) as.numeric(strsplit(periods_env, ",")[[1]]) else NULL
  
  requested = as.integer(Sys.getenv("CORES", unset = "12")); if (is.na(requested)) requested = 12
  cores = max(1, min(12, requested))
  message("Using ", cores, " worker(s).")
  
  aln = read_pr_pet_aligned()
  r_pr = aln$r_pr; r_pet = aln$r_pet; dates = aln$dates
  last_date_iso = format(max(dates, na.rm = TRUE))
  
  tiles = build_tiles_from_extent(dx = dx, dy = dy, r_for_align = r_pr)
  if (nzchar(tile_ids_env)) { keep_ids = as.integer(strsplit(tile_ids_env, ",")[[1]]); tiles = tiles[tiles$tile_id %in% keep_ids, ] }
  n_tiles = nrow(tiles)
  message("Processing ", n_tiles, " tile(s)…")
  if (n_tiles == 0) stop("No tiles to process (extent/params mismatch).")
  
  worker_fun = function(i) {
    tile = tiles[i, ]
    tryCatch(write_spei_tiles_daily(tile_sf = tile, periods_days = periods_days, out_dir = tiles_root,
                                    r_pr = r_pr, r_pet = r_pet, dates = dates),
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
                            varlist = c("tiles","n_tiles","periods_days","tiles_root","write_spei_tiles_daily",
                                        "spei_for_tile_daily",".spei_from_glo","read_pr_pet_aligned",
                                        "build_slice_groups","last_30y_indices","spei_timescales_days",
                                        "dates_from_layernames","merged_pr","merged_pet",
                                        ".abs_path",".write_checked","%||%","MIN_YEARS"),
                            envir = environment())
    parallel::clusterEvalQ(cl, {
      suppressPackageStartupMessages({ library(terra); library(sf); library(fs); library(purrr) })
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
  
  # Mosaics (CONUS masked)
  fs::dir_create(conus_root)
  written_dirs_full = unique(fs::path_dir(all_paths))
  period_dir_names  = unique(basename(written_dirs_full))
  if (!length(period_dir_names) && fs::dir_exists(tiles_root))
    period_dir_names = basename(fs::dir_ls(tiles_root, type = "directory"))
  message("Mosaicking ", length(period_dir_names), " period(s) to ", conus_root)
  mosaic_any = mosaic_all_periods_and_cleanup(period_dir_names, last_date_iso)
  if (!mosaic_any) message("No mosaics written (no tiles found?).")
  message(Sys.time(), " — SPEI run complete.")
}