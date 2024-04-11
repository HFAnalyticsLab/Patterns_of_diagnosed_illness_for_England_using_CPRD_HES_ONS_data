# ---------------------------------------------------------------------------------
# Diagnosis files: Group together spells that are within one year of each other.
# ---------------------------------------------------------------------------------

  print('Processing spell grouping for diagnoses')

  # CLEANING RULE 1: Group together spells that are within one year of each other. 
  # Flag if remission date of previous spell is within a year of next diagnosis date
  res_conds_dt <- res_conds_dt[order(patid, list_disease_real, diag_date), 
                                   temp_lag_rem := shift(.SD, n = 1, fill = 0, 'lag'), 
                                   by = c('patid', 'list_disease_real'), .SDcols = 'remission_date'] # Lag column by 1
  
  # NA date values are created as '1970-01-01' by R. Change it back to NA to avoid confusion.
  #res_conds_dt <- res_conds_dt[order(patid, list_disease_real, diag_date)][
  #  , n := fifelse(row.names(.SD) == 1, 1, 0), 
  #  by = .(patid, list_disease_real)][
  #    , temp_lag_rem := ifelse( n == 1 & temp_lag_rem == as.Date("1970-01-01"), NA, temp_lag_rem)][
  #      , temp_lag_rem := as.Date(temp_lag_rem)
  #    ]
  
  # Flag if new episode is >365 days after the last remission. 0 if not.
  res_conds_dt[, flag_date := fifelse((diag_date - temp_lag_rem) >= 365 |
                                row.names(.SD) == 1, 1, 0), 
                 by = .(patid, list_disease_real)]
  # Note, NA dates are automatically replaced by '1970-01-01' which means time is <0 
  # for diagnosis dates before 1970 and thus would be coded to 0. 
  
  # Construct 'spells' of diseases.
  res_conds_dt[, spell := cumsum(flag_date), by = .(patid, list_disease_real)]
  
  
  # Choose earliest diagnosis dates and latest remission dates within spell 
  res_conds_dt[, `:=` (diag_date = min(diag_date),
                         remission_date = max(remission_date)),
                 by = .(patid, list_disease_real, spell)]
  
  # Remove duplicate rows and remove unecessary variables
  res_conds_dt <- unique(res_conds_dt[ , patid:source])
  
  # If more than one source, recode to both sources and then drop duplicates
  res_conds_dt <- res_conds_dt[ order(patid, list_disease_real, diag_date, source)][ 
    , dum := 1, by = c('patid', 'list_disease_real', 'diag_date', 'source')]
  
  res_conds_dt[, temp := sum(dum), by = .(patid, list_disease_real, diag_date)]
  res_conds_dt[ , source := fifelse(temp == 1, source,
                              fifelse(temp == 2, "Both HES and CPRD", as.character(NA)))]
  res_conds_dt <- unique(res_conds_dt[ , patid:source])

  
gc()
