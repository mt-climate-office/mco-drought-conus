##############################################################
# File: R/gridmet_cache.R
# Title: GridMET local cache (refresh last N years, rebuild merged)
# Author: Dr. Zachary H. Hoylman
# Date: 10-28-2025
# Conventions: "=", |> , explicit pkg::fun namespaces.
##############################################################

# ---- roots -------------------------------------------------------------------
project_root = "~/mco-drought-conus"
data_root    = "~/mco-drought-conus-data"
interim_dir  = fs::path(data_root, "interim")
base_gridmet = fs::path(interim_dir, "gridmet")
invisible(fs::dir_create(c(interim_dir, base_gridmet)))

# ---- path helpers ------------------------------------------------------------
.abs = function(p) as.character(fs::path_abs(fs::path_expand(p)))
.sh  = function(p) shQuote(.abs(p), type = "sh")

# ---- configuration -----------------------------------------------------------
.gridmet_var_to_ncname = function(var) {
  switch(var,
         pr  = "precipitation_amount",
         pet = "potential_evapotranspiration",
         NULL
  )
}

.gridmet_url_for_year = function(var, year) {
  sprintf("https://www.northwestknowledge.net/metdata/data/%s_%d.nc", var, year)
}

.gridmet_dirs = function(var) {
  raw_dir    = fs::path(base_gridmet, var, "raw")
  merged_dir = fs::path(base_gridmet, var, "merged")
  list(raw_dir = raw_dir, merged_dir = merged_dir)
}

.gridmet_year_nc = function(var, year) {
  d = .gridmet_dirs(var)
  fs::path(d$raw_dir, sprintf("%s_%d.nc", var, year))
}

.gridmet_merged_nc = function(var, merged_name = NULL) {
  d = .gridmet_dirs(var)
  if (is.null(merged_name)) merged_name = sprintf("gridmet_%s.nc", var)
  fs::path(d$merged_dir, merged_name)
}

# ---- utilities ---------------------------------------------------------------
.retry = function(n, sleep_sec = 2, expr) {
  last = NULL
  for (i in seq_len(n)) {
    out = try(force(expr), silent = TRUE)
    if (!inherits(out, "try-error")) return(out)
    last = out
    if (i < n) Sys.sleep(sleep_sec)
  }
  stop(last)
}

.gridmet_dates_from_nc = function(nc_path) {
  rb = raster::brick(.abs(nc_path))
  nms = names(rb)
  dn  = suppressWarnings(as.numeric(substring(nms, 2)))
  as.Date(dn, origin = "1900-01-01")
}

# ---- download raw annuals (idempotent per year) ------------------------------
gridmet_download_year = function(var, year, overwrite = FALSE) {
  dirs = .gridmet_dirs(var)
  fs::dir_create(dirs$raw_dir)
  url = .gridmet_url_for_year(var, year)
  out = .gridmet_year_nc(var, year)
  
  if (fs::file_exists(out) && !overwrite && fs::file_info(out)$size > 0) {
    message("Exists: ", fs::path_expand(out))
    return(invisible(out))
  }
  
  message("Downloading ", var, " ", year)
  .retry(3, 3, {
    utils::download.file(url, out, mode = "wb", quiet = TRUE)
    out
  })
  
  if (!fs::file_exists(out) || fs::file_info(out)$size <= 0)
    stop("Download failed or empty file: ", out)
  invisible(out)
}

gridmet_download_range = function(var, start_year, end_year = as.integer(format(Sys.Date(), "%Y"))) {
  for (yy in seq.int(start_year, end_year)) gridmet_download_year(var, yy)
  invisible(TRUE)
}

# ---- merge all annuals (fresh build) -----------------------------------------
gridmet_merge_all = function(var, start_year = 1991, merged_name = NULL) {
  yrs = start_year:as.integer(format(Sys.Date(), "%Y"))
  files = vapply(yrs, function(yy) .gridmet_year_nc(var, yy), character(1))
  files = files[fs::file_exists(files)]
  if (length(files) == 0L) stop("No raw GridMET files found for ", var)
  
  dst = .gridmet_merged_nc(var, merged_name)
  fs::dir_create(fs::path_dir(dst))
  
  tmp = paste0(.abs(dst), ".tmp")
  cmd = sprintf("cdo -O mergetime %s %s",
                paste(vapply(files, function(f) .sh(f), character(1)), collapse = " "),
                shQuote(tmp, type = "sh"))
  status = system(cmd)
  if (status != 0) stop("CDO mergetime failed creating merged: ", dst)
  
  fs::file_move(tmp, .abs(dst))
  invisible(dst)
}

# ---- open merged brick -------------------------------------------------------
gridmet_open_brick = function(var, merged_name = NULL) {
  nc = .gridmet_merged_nc(var, merged_name)
  if (!fs::file_exists(nc)) stop("Merged file not found: ", nc)
  
  vname = .gridmet_var_to_ncname(var)
  rb = try(raster::brick(.abs(nc), varname = vname), silent = TRUE)
  if (inherits(rb, "try-error")) rb = raster::brick(.abs(nc))
  
  nms = names(rb)
  dn  = suppressWarnings(as.numeric(substring(nms, 2)))
  dts = as.Date(dn, origin = "1900-01-01")
  
  list(rb = rb, dates = dts)
}

# ---- rebuild merged (purge & rebuild) ----------------------------------------
gridmet_rebuild_merged = function(var, start_year = 1991, merged_name = NULL) {
  dst = .gridmet_merged_nc(var, merged_name)
  if (fs::file_exists(dst)) {
    message("Deleting merged: ", fs::path_expand(dst))
    fs::file_delete(dst)
  }
  gridmet_merge_all(var, start_year = start_year, merged_name = merged_name)
  invisible(TRUE)
}

# ---- REFRESH last N years, then rebuild --------------------------------------
# Behavior:
#   - Ensures historic raw files exist for years [start_year, current_year]
#   - Deletes & re-downloads the last n_refresh_years raw annuals
#   - Deletes merged NetCDF and rebuilds from all raw files
gridmet_refresh_last_years_and_rebuild = function(
    var,
    start_year       = 1991,
    n_refresh_years  = 2,
    merged_name      = NULL
) {
  cy  = as.integer(format(Sys.Date(), "%Y"))
  y0  = max(start_year, cy - n_refresh_years + 1L)
  yrs_refresh = seq.int(y0, cy)
  
  # 1) Ensure historic raws exist (so rebuild won’t miss old years)
  message("Ensuring historic raw files for ", var, " (", start_year, ":", cy, ")")
  gridmet_download_range(var, start_year = start_year, end_year = cy)
  
  # 2) Delete & re-download last N years
  message("Refreshing last ", n_refresh_years, " year(s) for ", var, ": ",
          paste(yrs_refresh, collapse = ", "))
  for (yy in yrs_refresh) {
    f = .gridmet_year_nc(var, yy)
    if (fs::file_exists(f)) {
      message("Deleting raw: ", fs::path_expand(f))
      fs::file_delete(f)
    }
    gridmet_download_year(var, yy, overwrite = TRUE)
  }
  
  # 3) Delete merged and rebuild from all raws
  message("Rebuilding merged for ", var)
  gridmet_rebuild_merged(var, start_year = start_year, merged_name = merged_name)
  invisible(TRUE)
}

# ---- convenience: refresh PR & PET -------------------------------------------
# Env overrides:
#   START_YEAR (default 1991)
#   GRIDMET_REFRESH_YEARS (default 2)
gridmet_refresh_pr_pet = function(
    start_year       = as.integer(Sys.getenv("START_YEAR", "1991")),
    n_refresh_years  = as.integer(Sys.getenv("GRIDMET_REFRESH_YEARS", "2")),
    pr_merged_name   = NULL,
    pet_merged_name  = NULL
) {
  gridmet_refresh_last_years_and_rebuild("pr",  start_year, n_refresh_years, pr_merged_name)
  gridmet_refresh_last_years_and_rebuild("pet", start_year, n_refresh_years, pet_merged_name)
  invisible(TRUE)
}

# ---- time CSV writer (optional) ----------------------------------------------
gridmet_write_time_csv = function(var, merged_name = NULL) {
  nc = .gridmet_merged_nc(var, merged_name)
  if (!fs::file_exists(nc)) stop("Merged file not found: ", nc)
  dts = .gridmet_dates_from_nc(nc)
  out_csv = fs::path(fs::path_dir(nc), sprintf("gridmet_%s_time.csv", var))
  readr::write_csv(tibble::tibble(datetime = dts), out_csv)
  invisible(out_csv)
}

# ---- auto-run: refresh PR + PET ----------------------------------------------
# Set GRIDMET_REFRESH_YEARS=2 (default) and START_YEAR=1991 by environment if needed.
if (sys.nframe() == 0) {
  message(Sys.time(), " — GridMET refresh (PR + PET): delete last N years, re-download, rebuild")
  gridmet_refresh_pr_pet()  # uses env overrides
  message(Sys.time(), " — GridMET refresh complete")
}