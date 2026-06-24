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
raw_base     = fs::path(data_root, "raw")
invisible(fs::dir_create(raw_base))

# ---- path helpers ------------------------------------------------------------
.abs = function(p) as.character(fs::path_abs(fs::path_expand(p)))

# ---- configuration -----------------------------------------------------------
.gridmet_url_for_year = function(var, year) {
  sprintf("https://www.northwestknowledge.net/metdata/data/%s_%d.nc", var, year)
}

.gridmet_dirs = function(var) {
  raw_dir = fs::path(raw_base, var)
  list(raw_dir = raw_dir)
}

.gridmet_year_nc = function(var, year) {
  d = .gridmet_dirs(var)
  fs::path(d$raw_dir, sprintf("%s_%d.nc", var, year))
}

# ---- TLS: supply GridMET's missing intermediate CA --------------------------
# WHY: since ~2026-06-11 www.northwestknowledge.net (the GridMET / metdata host)
# serves an INCOMPLETE certificate chain. The leaf (CN=northwestknowledge.net) is
# issued by "InCommon RSA OV SSL CA 3", but the server omits that intermediate and
# ships the wrong one, so strict TLS clients (curl in cron/containers) fail with
# "unable to get local issuer certificate" (openssl verify code 21). Browsers and
# any box that already cached the intermediate still work, which makes it look like
# a local misconfig — it is NOT. It is a SERVER-side bug (report to the admin).
#
# We unblock our side by pinning the missing intermediate (committed at
# R/certs/InCommonRSAOVSSLCA3.pem, valid to 2035-11-05) and verifying against a
# bundle of [system CA roots + that intermediate]. We do NOT disable verification
# (no -k/--insecure). REMOVE this workaround once SSL Labs shows the chain is
# complete again: https://www.ssllabs.com/ssltest/analyze.html?d=www.northwestknowledge.net
.gridmet_intermediate_pem = .abs(fs::path(project_root, "R", "certs", "InCommonRSAOVSSLCA3.pem"))

.build_ca_bundle = function() {
  if (!fs::file_exists(.gridmet_intermediate_pem))
    stop("GridMET intermediate CA missing: ", .gridmet_intermediate_pem,
         " (needed to complete the server's incomplete TLS chain)")
  # The CA roots the client already trusts. The pipeline runs on Debian/Ubuntu
  # (container); other paths cover RHEL and macOS. Override with GRIDMET_CA_ROOTS.
  roots_candidates = c(
    Sys.getenv("GRIDMET_CA_ROOTS", unset = ""),
    "/etc/ssl/certs/ca-certificates.crt",  # Debian/Ubuntu (pipeline container)
    "/etc/pki/tls/certs/ca-bundle.crt",    # RHEL/Fedora
    "/etc/ssl/cert.pem"                    # macOS (Homebrew/LibreSSL), BSD
  )
  ok = nzchar(roots_candidates) & fs::file_exists(roots_candidates)
  if (!any(ok))
    stop("No system CA root bundle found; set GRIDMET_CA_ROOTS to your CA roots .pem")
  roots  = roots_candidates[ok][1]
  bundle = fs::file_temp("gridmet-ca-bundle-", ext = "pem")
  # roots first, then the pinned intermediate; curl reads everything as trusted.
  writeLines(c(readLines(roots), readLines(.gridmet_intermediate_pem)), bundle)
  .abs(bundle)
}

# Built once per run; passed to curl via --cacert below.
.gridmet_ca_bundle = .build_ca_bundle()

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
  # --cacert: verify against [system roots + pinned intermediate] so the server's
  #           incomplete chain validates (see .build_ca_bundle above). NOT --insecure.
  extra = c(
    if (fs::file_exists(out)) c("-R", "-z", shQuote(out)) else "-R",
    "--cacert", shQuote(.gridmet_ca_bundle)
  )

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
