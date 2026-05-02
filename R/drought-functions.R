#function to fit a gamma distrobuion to a vector of data
#export options (export_opts) allows the user to return 
#SPI valules if export_opts = 'SPI', CDF values if export_opts = 'CDF'
#or the gamma distrobution paramters if export_opts = 'params'.
#the function also allows the user to return either the latest
#CDF or SPI values when return_latest = T. when return_latest = F
#the entire SPI or CDF vector is returned. Default is to return latest. 
# ---- Gamma SPI following Stagge et al. (2015) --------------------------------
# L-moment gamma SPI with proper zero handling via center-of-probability-mass
# (Weibull plotting position) following Stagge et al. (2015).
# Reference: https://rmets.onlinelibrary.wiley.com/doi/10.1002/joc.4267
#
# Zero precipitation methodology (Stagge et al. 2015, Eq. 2-4):
#   p0      = n_zero / (n + 1)            — Weibull probability of zero
#   p_bar_0 = (n_zero + 1) / (2*(n + 1))  — center of mass for zeros
#   For x > 0: p = p0 + (1 - p0) * F(x, gamma_params)
#   For x = 0: p = p_bar_0
#   SPI = Phi^-1(p)

gamma_fit_spi = function(reference_distribution, current_val,
                         export_opts = 'SPI',
                         climatology_length = 30, zero_threshold = 0) {
  library(lmomco)
  tryCatch({
    ref = as.numeric(reference_distribution)
    ref = tail(ref, climatology_length)
    ref = ref[is.finite(ref)]
    n   = length(ref)
    if (n < 3) return(NA)
    if (!is.finite(current_val)) return(NA)

    # Identify zeros in reference (threshold per Stagge et al.)
    is_zero_ref = (ref <= zero_threshold)
    n_zero      = sum(is_zero_ref)

    # Weibull plotting positions (Eq. 2-3)
    p0      = n_zero / (n + 1)
    p_bar_0 = (n_zero + 1) / (2 * (n + 1))

    if (n_zero == n) {
      # Reference is all zeros: only meaningful answer is for a zero current
      if (current_val <= zero_threshold) {
        cdf_current = p_bar_0
        fit.gam = NULL
      } else {
        return(NA)
      }
    } else {
      ref_pos = ref[!is_zero_ref]
      if (length(ref_pos) < 3 || stats::sd(ref_pos) == 0) return(NA)

      # L-moment gamma fit to non-zero reference values
      pwm     = pwm.ub(ref_pos)
      lmom    = pwm2lmom(pwm)
      fit.gam = pargam(lmom)

      # CDF for current_val (Eq. 4)
      cdf_current = if (current_val <= zero_threshold) {
        p_bar_0
      } else {
        p0 + (1 - p0) * cdfgam(current_val, fit.gam)
      }
    }

    if (export_opts == 'CDF')    return(cdf_current)
    if (export_opts == 'params') return(list(fit = fit.gam, p0 = p0))
    if (export_opts == 'SPI')    return(qnorm(cdf_current))
    NA
  }, error = function(cond) return(NA))
}

# ---- Legacy gamma SPI (L-moments only, zeros -> 0.01mm) ---------------------

gamma_fit_spi_legacy = function(x, export_opts = 'SPI', return_latest = T, climatology_length = 30) {
  #load the package needed for these computations
  library(lmomco)
  #first try gamma
  tryCatch(
    {
      x = as.numeric(x)
      #if precip is 0, replace it with 0.01mm Really Dry
      if(any(x == 0, na.rm = T)){
        index = which(x == 0)
        x[index] = 0.01
      }
      #extract the "climatology length from the dataset (assumes x is ordered in time, 1991, 1992, 1993... 2020 etc)
      x = tail(x, climatology_length)
      #Unbiased Sample Probability-Weighted Moments (following Beguer ́ıa et al 2014)
      pwm = pwm.ub(x)
      #Probability-Weighted Moments to L-moments
      lmoments_x = pwm2lmom(pwm)
      #fit gamma
      fit.gam = pargam(lmoments_x)
      #compute probabilistic cdf 
      fit.cdf = cdfgam(x, fit.gam)
      #compute spi
      spi = qnorm(fit.cdf, mean = 0, sd = 1)
      if(return_latest == T){
        if(export_opts == 'CDF'){
          return(fit.cdf[length(fit.cdf)]) 
        }
        if(export_opts == 'params'){
          return(fit.gam) 
        }
        if(export_opts == 'SPI'){
          return(spi[length(spi)]) 
        }
      }
      if(return_latest == F){
        if(export_opts == 'CDF'){
          return(fit.cdf) 
        }
        if(export_opts == 'params'){
          return(fit.gam) 
        }
        if(export_opts == 'SPI'){
          return(spi) 
        }
      }
      
    },
    #else return NA
    error=function(cond) {
      return(NA)
    })
}


# ---- Gamma SVPDI following Stagge et al. (2015) zero handling ----------------
# L-moment gamma SVPDI with mixed-distribution zero handling identical to SPI.
# VPD is non-negative and occasionally zero; the Stagge et al. center-of-
# probability-mass approach correctly represents the discrete mass at zero.
# Positive SVPDI = drought/high VPD (matches EDDI sign convention).
# Reference: https://rmets.onlinelibrary.wiley.com/doi/10.1002/joc.4267

gamma_fit_vpdi = function(reference_distribution, current_val,
                          export_opts = 'SVPDI',
                          climatology_length = 30, zero_threshold = 0) {
  library(lmomco)
  tryCatch({
    ref = as.numeric(reference_distribution)
    ref = tail(ref, climatology_length)
    ref = ref[is.finite(ref)]
    n   = length(ref)
    if (n < 3) return(NA)
    if (!is.finite(current_val)) return(NA)

    is_zero_ref = (ref <= zero_threshold)
    n_zero      = sum(is_zero_ref)

    p0      = n_zero / (n + 1)
    p_bar_0 = (n_zero + 1) / (2 * (n + 1))

    if (n_zero == n) {
      if (current_val <= zero_threshold) {
        cdf_current = p_bar_0
        fit.gam = NULL
      } else {
        return(NA)
      }
    } else {
      ref_pos = ref[!is_zero_ref]
      if (length(ref_pos) < 3 || stats::sd(ref_pos) == 0) return(NA)

      pwm     = pwm.ub(ref_pos)
      lmom    = pwm2lmom(pwm)
      fit.gam = pargam(lmom)

      cdf_current = if (current_val <= zero_threshold) {
        p_bar_0
      } else {
        p0 + (1 - p0) * cdfgam(current_val, fit.gam)
      }
    }

    if (export_opts == 'CDF')    return(cdf_current)
    if (export_opts == 'params') return(list(fit = fit.gam, p0 = p0))
    if (export_opts == 'SVPDI')  return(qnorm(cdf_current))
    NA
  }, error = function(cond) return(NA))
}


glo_fit_spei = function(reference_distribution, current_val,
                        export_opts = 'SPEI', climatology_length = 30) {
  library(lmomco)
  tryCatch({
    ref = as.numeric(reference_distribution)
    ref = tail(ref, climatology_length)
    ref = ref[is.finite(ref)]
    if (length(ref) < 3) return(NA)
    if (!is.finite(current_val)) return(NA)

    # L-moment GLO fit to reference distribution
    pwm        = pwm.ub(ref)
    lmoments_x = pwm2lmom(pwm)
    fit.parglo = parglo(lmoments_x)

    # CDF for current_val using the fitted GLO
    cdf_current = cdfglo(current_val, fit.parglo)

    if (export_opts == 'CDF')    return(cdf_current)
    if (export_opts == 'params') return(fit.parglo)
    if (export_opts == 'SPEI')   return(qnorm(cdf_current, mean = 0, sd = 1))
    NA
  }, error = function(cond) return(NA))
}


nonparam_fit_eddi = function(reference_distribution, current_val, climatology_length = 30) {
  # following Hobbins et al., 2016
  C0 = 2.515517
  C1 = 0.802853
  C2 = 0.010328
  d1 = 1.432788
  d2 = 0.189269
  d3 = 0.001308

  ref = as.numeric(reference_distribution)
  ref = tail(ref, climatology_length)
  ref = ref[is.finite(ref)]
  if (length(ref) < 3 || !is.finite(current_val)) return(NA)

  # If current_val is already the last element of ref (rolling/full mode),
  # rank within the n-sample directly. Otherwise (fixed-outside-range mode),
  # append current_val and rank within the (n+1)-sample.
  current_in_ref = isTRUE(all.equal(ref[length(ref)], current_val))
  sample = if (current_in_ref) ref else c(ref, current_val)
  n = length(sample)

  rank_c = rank(-sample)[n]
  prob_c = (rank_c - 0.33) / (n + 0.33)

  W = if (prob_c <= 0.5) sqrt(-2 * log(prob_c)) else sqrt(-2 * log(1 - prob_c))
  eddi = W - ((C0 + C1 * W + C2 * W^2) / (1 + d1 * W + d2 * W^2 + d3 * W^3))
  if (prob_c > 0.5) eddi = -eddi
  eddi
}

#fit the beta distrobution (2 parameter) - Useful for soil moisture etc
beta_fit_smi = function(x, export_opts = 'SMI', return_latest = T, climatology_length = 30) {
  #load the package needed for these computations
  library(MASS)
  #first try beta
  tryCatch(
    {
      x = as.numeric(x)
      #if soil moisture is 0, replace it with 0.01 Really Dry
      if(any(x == 0, na.rm = T)){
        index = which(x == 0)
        x[index] = 0.01
      }
      #extract the "climatology length from the dataset (assumes x is ordered in time, 1991, 1992, 1993... 2020 etc)
      x = tail(x, climatology_length)
      #fit the beta distribution
      fit.beta = fitdistr(x, densfun = "beta", start = list(shape1 = 1, shape2 = 1))
      #store parameters
      params = fit.beta$estimate
      #compute probabilistic cdf 
      fit.cdf = pbeta(x, shape1 = params[1], shape2 = params[2])
      #compute smi (soil moisture index)
      smi = qnorm(fit.cdf, mean = 0, sd = 1)
      if(return_latest == T){
        if(export_opts == 'CDF'){
          return(fit.cdf[length(fit.cdf)]) 
        }
        if(export_opts == 'params'){
          return(params) 
        }
        if(export_opts == 'SMI'){
          return(smi[length(smi)]) 
        }
      }
      if(return_latest == F){
        if(export_opts == 'CDF'){
          return(fit.cdf) 
        }
        if(export_opts == 'params'){
          return(params) 
        }
        if(export_opts == 'SMI'){
          return(smi) 
        }
      }
      
    },
    #else return NA
    error=function(cond) {
      return(NA)
    })
}

#percent of normal: current_val / mean(reference_distribution) * 100
percent_of_normal = function(reference_distribution, current_val, climatology_length = 30){
  ref = tail(reference_distribution, climatology_length)
  ref_mean = mean(ref, na.rm = TRUE)
  (current_val / ref_mean) * 100
}

#deviation from normal: current_val - mean(reference_distribution)
deviation_from_normal = function(reference_distribution, current_val, climatology_length = 30){
  ref = tail(reference_distribution, climatology_length)
  ref_mean = mean(ref, na.rm = TRUE)
  current_val - ref_mean
}

#empirical percentile of current_val within reference_distribution
compute_percentile = function(reference_distribution, current_val, climatology_length = 30){
  tryCatch({
    ref = tail(reference_distribution, climatology_length)
    ecdf_ = ecdf(ref)
    ecdf_(current_val)
  }, error = function(e) NA)
}
