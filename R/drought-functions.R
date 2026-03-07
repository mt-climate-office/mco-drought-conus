#function to fit a gamma distrobuion to a vector of data
#export options (export_opts) allows the user to return 
#SPI valules if export_opts = 'SPI', CDF values if export_opts = 'CDF'
#or the gamma distrobution paramters if export_opts = 'params'.
#the function also allows the user to return either the latest
#CDF or SPI values when return_latest = T. when return_latest = F
#the entire SPI or CDF vector is returned. Default is to return latest. 
# ---- Strict gamma-based SPI (no fallback) ------------------------------------

gamma_fit_spi = function(x, export_opts = 'SPI', return_latest = T, climatology_length = 30) {
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


gamma_fit_vpdi = function(x, export_opts = 'SVPDI', return_latest = T, climatology_length = 30) {
  library(lmomco)
  tryCatch({
    x = as.numeric(x)
    # Guard against zero VPD (rare but possible)
    if (any(x == 0, na.rm = T)) x[which(x == 0)] = 0.001
    x = tail(x, climatology_length)
    pwm = pwm.ub(x)
    lmoments_x = pwm2lmom(pwm)
    fit.gam = pargam(lmoments_x)
    fit.cdf = cdfgam(x, fit.gam)
    # Positive = drought/high VPD (matches EDDI convention in this repo)
    svpdi = qnorm(fit.cdf, mean = 0, sd = 1)
    if (return_latest == T) {
      if (export_opts == 'CDF')    return(fit.cdf[length(fit.cdf)])
      if (export_opts == 'params') return(fit.gam)
      if (export_opts == 'SVPDI') return(svpdi[length(svpdi)])
    }
    if (return_latest == F) {
      if (export_opts == 'CDF')    return(fit.cdf)
      if (export_opts == 'params') return(fit.gam)
      if (export_opts == 'SVPDI') return(svpdi)
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
