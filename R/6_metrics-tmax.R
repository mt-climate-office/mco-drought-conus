##############################################################
# File: R/6_metrics-tmax.R
# Title: Tmax metrics (CONUS, tiled, parallel) from local GridMET raws.
#        Percentile and deviation from normal, multi-scale calendar reference.
#        Per-tile GeoTIFF/COG; VRT->COG mosaics to conus_drought.
# Author: Dr. Zachary H. Hoylman
# Date: 3-4-2026
##############################################################

source(file.path(Sys.getenv("PROJECT_DIR", unset = "~/mco-drought-conus"), "R", "pipeline-common.R"))

# ---- Tmax-specific metric wrapper -------------------------------------------
.pctile_latest_tmax = function(x_vec, clim_len) {
  val = try(compute_percentile(x_vec, clim_len), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) NA_real_ else as.numeric(val)
}

# ---- Configuration -----------------------------------------------------------
config = list(
  metric_label  = "tmax",
  tiles_subdir  = "tmax_metrics",
  raw_vars      = list(
    list(var_prefix = "tmmx", raw_subdir = "tmmx", nc_varname = "air_temperature")
  ),
  output_regexp = "tmax_.*\\.tif$",
  input_mode    = "single",
  agg_fn        = .safe_window_mean,
  metric_specs  = list(
    list(prefix = "tmax-pctile", compute_fn = .pctile_latest_tmax, band_name = "pctile", vectorize_type = "pctile"),
    list(prefix = "tmax-dev",    compute_fn = .dev_latest,         band_name = "dev",    vectorize_type = "dev")
  )
)

# ---- Run ---------------------------------------------------------------------
if (sys.nframe() == 0) run_metric_pipeline(config)
