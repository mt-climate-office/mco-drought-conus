##############################################################
# File: R/1_gridmet-cache.R
# Title: GridMET local cache (delete + refresh last N annual raws; NO merge)
# Author: Dr. Zachary H. Hoylman
# Date: 3-4-2026
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

# ---- configuration -----------------------------------------------------------
.gridmet_url_for_year = function(var, year) {
  sprintf("https://www.northwestknowledge.net/metdata/data/%s_%d.nc", var, year)
}

.gridmet_dirs = function(var) {
  raw_dir = fs::path(base_gridmet, var, "raw")
  list(raw_dir = raw_dir)
}

.gridmet_year_nc = function(var, year) {
  d = .gridmet_dirs(var)
  fs::path(d$raw_dir, sprintf("%s_%d.nc", var, year))
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

# Robust duplicate-time check across given annual files (no merge needed)
.assert_no_duplicate_times_raw = function(nc_files) {
  nc_files = as.character(nc_files)
  nc_files = nc_files[fs::file_exists(nc_files)]
  if (!length(nc_files)) stop("No raw GridMET files found to check.")

  get_dates = function(f) {
    r = terra::rast(f)
    tt = try(terra::time(r), silent = TRUE)
    if (!inherits(tt, "try-error") && !is.null(tt) && length(tt) == terra::nlyr(r)) {
      return(as.Date(tt))
    }
    nms    = names(r)
    digits = gsub("[^0-9]", "", nms)
    d      = suppressWarnings(as.numeric(digits))
    as.Date("1900-01-01") + d
  }

  d_all = unlist(lapply(nc_files, get_dates), use.names = FALSE)
  d_all = d_all[is.finite(as.numeric(d_all))]

  if (anyDuplicated(d_all)) stop("Duplicate timesteps detected across raw files.")
  invisible(TRUE)
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

  if (!fs::file_exists(out) || fs::file_info(out)$size <= 0) {
    stop("Download failed or empty file: ", out)
  }

  invisible(out)
}

gridmet_download_range = function(var, start_year, end_year = as.integer(format(Sys.Date(), "%Y"))) {
  for (yy in seq.int(start_year, end_year)) gridmet_download_year(var, yy)
  invisible(TRUE)
}

# ---- refresh last N raw annuals (NO merge) -----------------------------------
# Behavior:
#   - Identifies the last n_refresh_years: [cy - n_refresh_years + 1, cy]
#   - Deletes those annual raws if present
#   - Re-downloads those same years
#   - Runs a duplicate-time sanity check on just those refreshed years
gridmet_refresh_last_years_raw = function(
    var,
    start_year      = 1991,
    n_refresh_years = 2
) {
  cy = as.integer(format(Sys.Date(), "%Y"))
  y0 = max(start_year, cy - n_refresh_years + 1L)
  yrs_refresh = seq.int(y0, cy)

  message("Refreshing last ", n_refresh_years, " year(s) for ", var, ": ",
          paste(yrs_refresh, collapse = ", "))

  # ensure raw dir exists
  dirs = .gridmet_dirs(var)
  fs::dir_create(dirs$raw_dir)

  # delete + re-download just the last N years
  for (yy in yrs_refresh) {
    f = .gridmet_year_nc(var, yy)
    if (fs::file_exists(f)) {
      message("Deleting raw: ", fs::path_expand(f))
      fs::file_delete(f)
    }
    gridmet_download_year(var, yy, overwrite = TRUE)
  }

  # sanity check on refreshed years only
  files = vapply(yrs_refresh, function(yy) .gridmet_year_nc(var, yy), character(1))
  files = files[fs::file_exists(files)]
  .assert_no_duplicate_times_raw(files)

  invisible(TRUE)
}

gridmet_refresh_pr_pet_vpd_tmmx_raw = function(
    start_year      = as.integer(Sys.getenv("START_YEAR", "1991")),
    n_refresh_years = as.integer(Sys.getenv("GRIDMET_REFRESH_YEARS", "2"))
) {
  gridmet_refresh_last_years_raw("pr",   start_year = start_year, n_refresh_years = n_refresh_years)
  gridmet_refresh_last_years_raw("pet",  start_year = start_year, n_refresh_years = n_refresh_years)
  gridmet_refresh_last_years_raw("vpd",  start_year = start_year, n_refresh_years = n_refresh_years)
  gridmet_refresh_last_years_raw("tmmx", start_year = start_year, n_refresh_years = n_refresh_years)
  invisible(TRUE)
}

# ---- auto-run (optional) -----------------------------------------------------
if (sys.nframe() == 0) {
  message(Sys.time(), " — GridMET refresh (PR + PET + VPD + TMMX): delete + re-download last N years (NO merge)")
  gridmet_refresh_pr_pet_vpd_tmmx_raw()
  message(Sys.time(), " — GridMET refresh complete")
}