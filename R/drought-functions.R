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

gamma_fit_spi = function(x, export_opts = 'SPI', return_latest = TRUE,
                         climatology_length = 30, zero_threshold = 0) {
  library(lmomco)
  tryCatch({
    x = as.numeric(x)
    x = tail(x, climatology_length)
    n = length(x)
    if (n < 3) return(NA)

    # Identify zeros (threshold per Stagge et al.)
    is_zero = (x <= zero_threshold)
    n_zero  = sum(is_zero)

    if (n_zero == n) {
      # All zeros: center of mass -> SPI = 0 for all
      spi = rep(0, n)
      fit_cdf = rep(0.5, n)
    } else {
      x_pos = x[!is_zero]
      if (length(x_pos) < 3 || stats::sd(x_pos) == 0) return(NA)

      # L-moment gamma fit to non-zero values
      pwm      = pwm.ub(x_pos)
      lmom     = pwm2lmom(pwm)
      fit.gam  = pargam(lmom)

      # Weibull plotting positions (Eq. 2-3)
      p0      = n_zero / (n + 1)
      p_bar_0 = (n_zero + 1) / (2 * (n + 1))

      # Build CDF (Eq. 4)
      fit_cdf = numeric(n)
      fit_cdf[is_zero]  = p_bar_0
      fit_cdf[!is_zero] = p0 + (1 - p0) * cdfgam(x_pos, fit.gam)

      # Transform to standard normal and clamp to [-3, 3]
      spi = qnorm(fit_cdf)
    }

    if (return_latest) {
      if (export_opts == 'CDF')    return(fit_cdf[n])
      if (export_opts == 'params') return(list(fit = fit.gam,
                                                p0 = n_zero / (n + 1)))
      if (export_opts == 'SPI')    return(spi[n])
    } else {
      if (export_opts == 'CDF')    return(fit_cdf)
      if (export_opts == 'params') return(list(fit = fit.gam,
                                                p0 = n_zero / (n + 1)))
      if (export_opts == 'SPI')    return(spi)
    }
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

gamma_fit_vpdi = function(x, export_opts = 'SVPDI', return_latest = TRUE,
                          climatology_length = 30, zero_threshold = 0) {
  library(lmomco)
  tryCatch({
    x = as.numeric(x)
    x = tail(x, climatology_length)
    n = length(x)
    if (n < 3) return(NA)

    # Identify zeros (threshold per Stagge et al.)
    is_zero = (x <= zero_threshold)
    n_zero  = sum(is_zero)

    if (n_zero == n) {
      # All zeros: center of mass -> SVPDI = 0 for all
      svpdi   = rep(0, n)
      fit_cdf = rep(0.5, n)
    } else {
      x_pos = x[!is_zero]
      if (length(x_pos) < 3 || stats::sd(x_pos) == 0) return(NA)

      # L-moment gamma fit to non-zero values
      pwm      = pwm.ub(x_pos)
      lmom     = pwm2lmom(pwm)
      fit.gam  = pargam(lmom)

      # Weibull plotting positions (Stagge et al. Eq. 2-3)
      p0      = n_zero / (n + 1)
      p_bar_0 = (n_zero + 1) / (2 * (n + 1))

      # Build CDF (Stagge et al. Eq. 4)
      fit_cdf = numeric(n)
      fit_cdf[is_zero]  = p_bar_0
      fit_cdf[!is_zero] = p0 + (1 - p0) * cdfgam(x_pos, fit.gam)

      # Transform to standard normal (positive = high VPD = drought)
      svpdi = qnorm(fit_cdf)
    }

    if (return_latest) {
      if (export_opts == 'CDF')    return(fit_cdf[n])
      if (export_opts == 'params') return(list(fit = fit.gam,
                                                p0 = n_zero / (n + 1)))
      if (export_opts == 'SVPDI')  return(svpdi[n])
    } else {
      if (export_opts == 'CDF')    return(fit_cdf)
      if (export_opts == 'params') return(list(fit = fit.gam,
                                                p0 = n_zero / (n + 1)))
      if (export_opts == 'SVPDI')  return(svpdi)
    }
  }, error = function(cond) return(NA))
}


glo_fit_spei = function(x, export_opts = 'SPEI', return_latest = T, climatology_length = 30) {
  #load the package needed for these computations
  library(lmomco)
  #first try gamma
  tryCatch(
    {
      x = as.numeric(x)
      #extract the "climatology length from the dataset (assumes x is ordered in time, 1991, 1992, 1993... 2020 etc)
      x = tail(x, climatology_length)
      #Unbiased Sample Probability-Weighted Moments (following Beguer ́ıa et al 2014)
      pwm = pwm.ub(x)
      #Probability-Weighted Moments to L-moments
      lmoments_x = pwm2lmom(pwm)
      #fit generalized logistic
      fit.parglo = parglo(lmoments_x)
      #compute probabilistic cdf 
      fit.cdf = cdfglo(x, fit.parglo)
      #compute spi
      spei = qnorm(fit.cdf, mean = 0, sd = 1)
      if(return_latest == T){
        if(export_opts == 'CDF'){
          return(fit.cdf[length(fit.cdf)]) 
        }
        if(export_opts == 'params'){
          return(fit.parglo) 
        }
        if(export_opts == 'SPEI'){
          return(spei[length(spei)]) 
        }
      }
      if(return_latest == F){
        if(export_opts == 'CDF'){
          return(fit.cdf) 
        }
        if(export_opts == 'params'){
          return(fit.parglo) 
        }
        if(export_opts == 'SPEI'){
          return(spei) 
        }
      }
      
    },
    #else return NA
    error=function(cond) {
      return(NA)
    })
}


nonparam_fit_eddi = function(x, climatology_length = 30) {
  #define coeffitients
  C0 = 2.515517
  C1 = 0.802853
  C2 = 0.010328
  d1 = 1.432788
  d2 = 0.189269
  d3 = 0.001308
  
  # following Hobbins et al., 2016
  x = as.numeric(x)
  
  #extract the "climatology length from the dataset (assumes x is ordered in time, 1991, 1992, 1993... 2020 etc)
  x = tail(x, climatology_length)
  
  if(all(is.na(x))){
    return(NA)
  } else {
    
    #Rank PET (1 = max)
    rank_1 = rank(-x)
    
    #Calcualte emperical probabilities
    prob = ((rank_1 - 0.33)/(length(rank_1) + 0.33))
    
    #compute W (whaterver that is)
    W = numeric(length(prob))
    for(i in 1: length(prob)){
      if(prob[i] <= 0.5){
        W[i] = sqrt(-2*log(prob[i]))
      } else {
        W[i] = sqrt(-2*log(1 - prob[i]))
      }
    }
    
    #Find indexes which need inverse EDDI sign
    reverse_index = which(prob > 0.5)
    
    #Compute EDDI
    EDDI = W - ((C0 + C1*W + C2*W^2)/(1 + d1*W + d2*W^2 + d3*W^3))
    
    #Reverse sign of EDDI values where prob > 0.5
    EDDI[reverse_index] = -EDDI[reverse_index]
    
    #Return Current Value
    return(EDDI[length(EDDI)])
  }
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

#percent of normal
percent_of_normal = function(x, climatology_length = 30){
  #extract the "climatology length from the dataset (assumes x is ordered in time, 1991, 1992, 1993... 2020 etc)
  x = tail(x, climatology_length)
  
  x_mean = mean(x, na.rm = T)
  percent_of_normal = ((x[length(x)])/x_mean)*100
  return(percent_of_normal)
}

#deviation from normal
deviation_from_normal = function(x, climatology_length = 30){
  #extract the "climatology length from the dataset (assumes x is ordered in time, 1991, 1992, 1993... 2020 etc)
  x = tail(x, climatology_length)
  
  x_mean = mean(x, na.rm = T)
  deviation_from_normal = ((x[length(x)]) - x_mean)
  return(deviation_from_normal)
}

compute_percentile = function(x, climatology_length = 30){
  tryCatch({
    x = tail(x, climatology_length)
    ecdf_ = ecdf(x)
    return(ecdf_(x[length(x)]))
  }, error = function(e) {
    return(NA)
  })
}
