##############################################################
# File: R/3_metrics-spei.R
# Title: SPEI (CONUS, tiled, parallel) from local GridMET raws.
#        Standardized Precipitation Evapotranspiration Index (GLO fit).
#        Water balance = PR - PET; dual-variable input.
#        Multi-scale calendar reference; per-tile GeoTIFF/COG;
#        VRT->COG mosaics to conus_drought.
# Author: Dr. Zachary H. Hoylman
# Date: 3-4-2026
##############################################################

source(file.path(Sys.getenv("PROJECT_DIR", unset = "~/mco-drought-conus"), "R", "pipeline-common.R"))

# ---- SPEI-specific metric wrapper -------------------------------------------
.spei_from_glo = function(x_vec, clim_len) {
  x = as.numeric(x_vec)
  x = x[is.finite(x)]
  if (length(x) < 3 || stats::sd(x) == 0) return(NA_real_)
  fn  = .require_fun("glo_fit_spei")
  val = try(fn(x, export_opts = "SPEI", return_latest = TRUE,
               climatology_length = clim_len), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) return(NA_real_)
  as.numeric(val)
}

# ---- Configuration -----------------------------------------------------------
config = list(
  metric_label  = "SPEI",
  tiles_subdir  = "spei_metrics",
  raw_vars      = list(
    list(var_prefix = "pr",  raw_subdir = "pr",  nc_varname = "precipitation_amount"),
    list(var_prefix = "pet", raw_subdir = "pet", nc_varname = "potential_evapotranspiration")
  ),
  output_regexp = "spei_.*\\.tif$",
  input_mode    = "dual_subtract",
  combine_fn    = function(a, b) a - b,   # water balance = PR - PET
  agg_fn        = .safe_window_sum,
  metric_specs  = list(
    list(prefix = "spei", compute_fn = .spei_from_glo, band_name = "spei")
  )
)

# ---- Run ---------------------------------------------------------------------
if (sys.nframe() == 0) run_metric_pipeline(config)
