##############################################################
# File: R/2_metrics-precip.R
# Title: Precip metrics (CONUS, tiled, parallel) from local GridMET raws.
#        SPI (gamma), % of normal, deviation from normal, percentile,
#        and raw precipitation totals (mm).
#        Multi-scale calendar reference; per-tile GeoTIFF/COG;
#        VRT->COG mosaics to conus_drought.
# Author: Dr. Zachary H. Hoylman
# Date: 3-4-2026
##############################################################

source(file.path(Sys.getenv("PROJECT_DIR", unset = "~/mco-drought-conus"), "R", "pipeline-common.R"))

# ---- Precip-specific metric wrappers ----------------------------------------
.spi_from_gamma = function(x_vec, clim_len) {
  x = as.numeric(x_vec)
  x = x[is.finite(x)]
  if (length(x) < 3 || all(x == 0) || stats::sd(x) == 0) return(NA_real_)
  fn  = .require_fun("gamma_fit_spi")
  val = try(fn(x, export_opts = "SPI", return_latest = TRUE,
               climatology_length = clim_len), silent = TRUE)
  if (inherits(val, "try-error") || !is.finite(val)) return(NA_real_)
  as.numeric(val)
}

# ---- Configuration -----------------------------------------------------------
config = list(
  metric_label  = "precipitation",
  tiles_subdir  = "precip_metrics",
  raw_vars      = list(
    list(var_prefix = "pr", raw_subdir = "pr", nc_varname = "precipitation_amount")
  ),
  output_regexp = "spi_.*\\.tif$",
  input_mode    = "single",
  agg_fn        = .safe_window_sum,
  metric_specs  = list(
    list(prefix = "spi",           compute_fn = .spi_from_gamma, band_name = "spi"),
    list(prefix = "precip-pon",    compute_fn = .pon_latest,     band_name = "pon",    vectorize_type = "pon"),
    list(prefix = "precip-dev",    compute_fn = .dev_latest,     band_name = "dev",    vectorize_type = "dev"),
    list(prefix = "precip-pctile", compute_fn = .pctile_latest,  band_name = "pctile", vectorize_type = "pctile"),
    # precip-mm: raw window sum in mm; only needs 1 valid year
    list(prefix = "precip-mm", band_name = "precip_mm",
         raw_latest = TRUE, min_years = 1L)
  )
)

# ---- Run ---------------------------------------------------------------------
if (sys.nframe() == 0) run_metric_pipeline(config)
