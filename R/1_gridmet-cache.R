##############################################################
# File: R/1_gridmet-cache.R
# Title: GridMET local cache (timestamp-conditional refresh; NO merge)
# Author: Dr. Zachary H. Hoylman
# Date: 3-4-2026
# Conventions: "=", |> , explicit pkg::fun namespaces.
##############################################################

# ---- roots -------------------------------------------------------------------
project_root = Sys.getenv("PROJECT_DIR", unset = "~/mco-drought-conus")
data_root    = Sys.getenv("DATA_DIR",    unset = "~/mco-drought-conus-data")
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

# ---- download raw annuals (timestamp-conditional) ----------------------------
# Uses curl -R (preserve server Last-Modified as local mtime) and
# -z <file> (skip download if remote is not newer than local file).
gridmet_download_year = function(var, year) {
  dirs = .gridmet_dirs(var)
  fs::dir_create(dirs$raw_dir)

  url = .gridmet_url_for_year(var, year)
  out = .gridmet_year_nc(var, year)

  # -R: set local mtime to server Last-Modified
  # -z: send If-Modified-Since; skip download if remote is not newer
  extra = if (fs::file_exists(out)) c("-R", "-z", shQuote(out)) else "-R"

  message("Checking ", var, " ", year)
  .retry(3, 3, {
    utils::download.file(url, out, method = "curl", extra = extra, mode = "wb", quiet = TRUE)
    out
  })

  if (!fs::file_exists(out) || fs::file_info(out)$size <= 0) {
    stop("Download failed or empty file: ", out)
  }

  invisible(out)
}

# ---- refresh all raw annuals (NO merge) --------------------------------------
# Loops over every year from start_year to the current year.
# Each file is only re-downloaded if the remote copy is newer (via curl -z).
gridmet_refresh_raw = function(var, start_year = 1979) {
  cy  = as.integer(format(Sys.Date(), "%Y"))
  yrs = seq.int(start_year, cy)

  message("Syncing ", var, " ", start_year, "-", cy)
  fs::dir_create(.gridmet_dirs(var)$raw_dir)

  for (yy in yrs) gridmet_download_year(var, yy)

  files = vapply(yrs, function(yy) .gridmet_year_nc(var, yy), character(1))
  .assert_no_duplicate_times_raw(files[fs::file_exists(files)])
  invisible(TRUE)
}

gridmet_refresh_pr_pet_vpd_tmmx_raw = function(
    start_year = as.integer(Sys.getenv("START_YEAR", "1979"))
) {
  for (v in c("pr", "pet", "vpd", "tmmx")) gridmet_refresh_raw(v, start_year = start_year)
  invisible(TRUE)
}

# ---- auto-run (optional) -----------------------------------------------------
if (sys.nframe() == 0) {
  message(Sys.time(), " — GridMET sync (PR + PET + VPD + TMMX): timestamp-conditional refresh")
  gridmet_refresh_pr_pet_vpd_tmmx_raw()
  message(Sys.time(), " — GridMET sync complete")
}
