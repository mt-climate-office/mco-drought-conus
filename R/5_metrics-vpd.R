##############################################################
# File: R/5_metrics-vpd.R
# Title: VPD metrics (CONUS, tiled, parallel) from local GridMET raws.
#        SVPDI (gamma), % of normal, deviation from normal, percentile.
#        Sign convention: positive = drought (high VPD), matches EDDI.
#        Multi-scale calendar reference; per-tile GeoTIFF/COG;
#        VRT->COG mosaics to conus_drought.
# Author: Dr. Zachary H. Hoylman
# Date: 3-4-2026
##############################################################

source(file.path(Sys.getenv("PROJECT_DIR", unset = "~/mco-drought-conus"), "R", "pipeline-common.R"))

# ---- VPD-specific metric wrappers -------------------------------------------
.svpdi_from_gamma = function(x_vec, clim_len) {
  x = as.numeric(x_vec)
  x = x[is.finite(x)]
  if (length(x) < 3 || all(x == 0) || stats::sd(x) == 0) return(NA_real_)
  fn  = .require_fun("gamma_fit_vpdi")
  val = try(fn(x, export_opts = "SVPDI", return_latest = TRUE,
               climatology_length = clim_len), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) return(NA_real_)
  as.numeric(val)
}

# ---- Configuration -----------------------------------------------------------
config = list(
  metric_label  = "VPD",
  tiles_subdir  = "vpd_metrics",
  raw_vars      = list(
    list(var_prefix = "vpd", raw_subdir = "vpd", nc_varname = "mean_vapor_pressure_deficit")
  ),
  output_regexp = "svpdi_.*\\.tif$",
  input_mode    = "single",
  agg_fn        = .safe_window_mean,
  metric_specs  = list(
    list(prefix = "svpdi",      compute_fn = .svpdi_from_gamma, band_name = "svpdi"),
    list(prefix = "vpd-pon",    compute_fn = .pon_latest,       band_name = "pon",    vectorize_type = "pon"),
    list(prefix = "vpd-dev",    compute_fn = .dev_latest,       band_name = "dev",    vectorize_type = "dev"),
    list(prefix = "vpd-pctile", compute_fn = .pctile_latest,    band_name = "pctile", vectorize_type = "pctile")
  )
)

# ---- Run ---------------------------------------------------------------------
if (sys.nframe() == 0) run_metric_pipeline(config)
