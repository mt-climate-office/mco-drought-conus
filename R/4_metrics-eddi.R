##############################################################
# File: R/4_metrics-eddi.R
# Title: EDDI (CONUS, tiled, parallel) from local GridMET PET raws.
#        Evaporative Demand Drought Index (nonparametric fit).
#        Multi-scale calendar reference; per-tile GeoTIFF/COG;
#        VRT->COG mosaics to conus_drought.
# Author: Dr. Zachary H. Hoylman
# Date: 3-4-2026
##############################################################

source(file.path(Sys.getenv("PROJECT_DIR", unset = "~/mco-drought-conus"), "R", "pipeline-common.R"))

# ---- EDDI-specific metric wrapper -------------------------------------------
.eddi_from_nonparam = function(x_vec, clim_len) {
  x = as.numeric(x_vec)
  x = x[is.finite(x)]
  if (length(x) < 3 || stats::sd(x) == 0) return(NA_real_)
  fn  = .require_fun("nonparam_fit_eddi")
  val = try(fn(x, climatology_length = clim_len), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) return(NA_real_)
  as.numeric(val)
}

# ---- Configuration -----------------------------------------------------------
config = list(
  metric_label  = "EDDI",
  tiles_subdir  = "eddi_metrics",
  raw_vars      = list(
    list(var_prefix = "pet", raw_subdir = "pet", nc_varname = "potential_evapotranspiration")
  ),
  output_regexp = "eddi_.*\\.tif$",
  input_mode    = "single",
  agg_fn        = .safe_window_sum,
  metric_specs  = list(
    list(prefix = "eddi", compute_fn = .eddi_from_nonparam, band_name = "eddi")
  )
)

# ---- Run ---------------------------------------------------------------------
if (sys.nframe() == 0) run_metric_pipeline(config)
