##############################################################
# File: R/7_metrics-cdd.R
# Title: Consecutive Dry Days (CONUS, tiled, parallel) from local GridMET raws.
#        Raw dry-streak length (days) and Standardized CDD (z-score) at three
#        precipitation thresholds. Multi-scale calendar reference;
#        per-tile GeoTIFF/COG; VRT->COG mosaics to conus_drought.
# Author: Dr. Zachary H. Hoylman
# Date: 9-23-2026
#
# Method: after Stagge et al. (2015). For each pixel, CDD is the number of
# consecutive days, counting backward from the anchor date, with daily
# precipitation below the threshold. The reference distribution is the CDD value
# on the same calendar day in each climatology year. SCDD standardizes that value
# through a mixed discrete-continuous distribution: a Weibull plotting position
# for the zero (wet-anchor) mass and a gamma fit to the positive streaks, mapped
# to a z-score. That is exactly what gamma_fit_spi() already implements for SPI,
# so this metric reuses it rather than introducing a second gamma estimator.
#
# Dry streaks are right-censored at CDD_WINDOW_DAYS. See that constant for how
# the value was chosen; it is set above the longest streak GridMET actually
# contains, so censoring should never engage in practice.
##############################################################

source(file.path(Sys.getenv("PROJECT_DIR", unset = "~/mco-drought-conus"), "R", "pipeline-common.R"))

# ---- Configuration -----------------------------------------------------------
# GridMET precipitation_amount is in mm; the literature and the source study
# state these thresholds in inches.
CDD_THRESHOLDS_IN = c("005" = 0.05, "010" = 0.10, "025" = 0.25)
CDD_THRESHOLDS_MM = CDD_THRESHOLDS_IN * 25.4

# Lookback window, and therefore the right-censoring point for a streak.
#
# Chosen from measurement, not convenience. Scanning GridMET 1989-2020 over the
# arid Southwest (lon -120..-105, lat 31..38) at the 0.25 in threshold, the
# longest dry streak found anywhere is 1893 days, in the Sonoran near 32.6 N. A
# 730-day window would have censored 4.1% of arid land pixels. 2557 days (7 yr)
# clears the observed maximum with 664 days of headroom.
#
# Cost is linear in this value and small: the aggregation is one max.col over a
# pixels x window logical matrix, about 0.9 s per tile per threshold.
#
# Note build_slice_groups() drops any reference year whose window would start
# before the record begins, so with GridMET starting 1979-01-01 the earliest
# usable anchor year is 1986. That only affects CLIM_PERIODS=full; rolling:30
# and fixed:1991:2020 are unaffected.
CDD_WINDOW_DAYS = 2557L

# Probability clamp applied before the normal-quantile transform, matching the
# source study. Bounds |SCDD| at qnorm(1e-6) = 4.75.
CDD_P_EPS = 1e-6

# ---- CDD-specific metric wrappers --------------------------------------------
#' Standardize a dry-streak length against its reference distribution.
#'
#' Deliberately does NOT carry the `sd(ref) == 0` guard used by .spi_from_gamma.
#' Identical reference streaks are a legitimate state for CDD (e.g. a pixel that
#' is wet on this calendar day in every climatology year has a reference of all
#' zeros), and gamma_fit_spi() already handles the degenerate cases internally.
.scdd_from_gamma = function(ref_dist, current_val, clim_len) {
  ref = as.numeric(ref_dist)
  ref = ref[is.finite(ref)]
  if (length(ref) < 3) return(NA_real_)
  if (!is.finite(current_val)) return(NA_real_)
  fn  = .require_fun("gamma_fit_spi")

  # Ask for the CDF rather than the z-score so the tails can be bounded before
  # the normal-quantile transform, exactly as the source study does. Dry-streak
  # distributions are far tighter than precipitation ones: a desert pixel whose
  # 30 reference streaks are all long produces a gamma with almost no left tail,
  # and an unbounded qnorm() on that returns values past -7, which are an
  # artifact of a 30-member sample rather than a real anomaly. Clamping the
  # probability to [CDD_P_EPS, 1 - CDD_P_EPS] caps |SCDD| at about 4.75.
  cdf = try(fn(ref, current_val, export_opts = "CDF",
               climatology_length = clim_len, zero_threshold = 0), silent = TRUE)
  if (inherits(cdf, "try-error") || !is.finite(cdf)) return(NA_real_)
  cdf = min(max(as.numeric(cdf), CDD_P_EPS), 1 - CDD_P_EPS)

  val = stats::qnorm(cdf)
  if (!is.finite(val)) return(NA_real_)
  as.numeric(val)
}

# ---- Metric specs ------------------------------------------------------------
# Two bands per threshold. Both bands of a threshold share one agg_key, so the
# dry-streak matrix for that threshold is built once per tile and reused.
.cdd_specs = function() {
  specs = list()
  for (nm in names(CDD_THRESHOLDS_MM)) {
    key = paste0("thr", nm)
    agg = .cdd_trailing_run(unname(CDD_THRESHOLDS_MM[[nm]]))

    # Raw streak length in days.
    specs[[length(specs) + 1L]] = list(
      prefix = paste0("cdd-", nm), band_name = "cdd",
      agg_fn = agg, agg_key = key,
      raw_latest = TRUE
    )
    # Standardized streak length (z-score).
    specs[[length(specs) + 1L]] = list(
      prefix = paste0("scdd-", nm), band_name = "scdd",
      agg_fn = agg, agg_key = key,
      compute_fn = .scdd_from_gamma
    )
  }
  specs
}

config = list(
  metric_label   = "consecutive dry days",
  tiles_subdir   = "cdd_metrics",
  raw_vars       = list(
    list(var_prefix = "pr", raw_subdir = "pr", nc_varname = "precipitation_amount")
  ),
  output_regexp  = "scdd-.*\\.tif$",
  input_mode     = "single",
  # Config-level default. Every spec overrides it with its own thresholded
  # closure, so this is only a safety net.
  agg_fn         = .cdd_trailing_run(unname(CDD_THRESHOLDS_MM[["010"]])),
  timescale_info = list(lengths = CDD_WINDOW_DAYS,
                        names   = paste0("cap", CDD_WINDOW_DAYS)),
  metric_specs   = .cdd_specs()
)

# ---- Run ---------------------------------------------------------------------
if (sys.nframe() == 0) run_metric_pipeline(config)
