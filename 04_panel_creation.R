#-------------------------------------------------------------------------------
# This script creates an annual panel that gives each patient a weighting 
# for how much of the year they are active. The diagnosis of conditions also
# gives them the date as a % of the year that is passed. 
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# Set up panel for patients and years of study 
#------------------------------------------------------------------------------
for (i in 1:nb_cohorts){
  
# Use clean patient file 
  cohort <- opendt(paste0(cleandata.filepath, 'cohorts/', i, '/data.parquet'),
                   cols_in = c('patid')) 

  patient <- opendt(paste0(cleandata.filepath, 'interim/patient/patient_clean.parquet'),
                    cols_in = c('patid', 'regstartdate', 'death_date', 'censor_date', 'yob'),
                    patient_list = cohort)

  # Cross join with years, creating a n.Patient x n.year long DT
  patient_long <- setkey(patient[, c(k = 1, .SD)], k)[
    years[, c(k = 1, .SD)], allow.cartesian = TRUE][
      , k := NULL]
  # to join 2 dt: x[y]
  # to setkey(dt, keycolumn)
  # create a dummy column (k) and set as first column with c(k = 1, .SD)
  # Merge on dummy columns and then drop: [ , k := NULL]

  # Keep all rows corresponding to years where patient is active
  patient_long <- patient_long[regstartdate <= year_end, ]

  
  # Flag deaths that occur during the year
  patient_long[, death := fifelse(!is.na(death_date) & 
                                    death_date <= year_end & 
                                    death_date >= year_start, 1, 0)]
  
  # Cleaning rule ID#32 - Keep a row for deaths even if happens after end of registration
  if (keep_death){
    
    patient_long <- patient_long[is.na(censor_date) | (censor_date >= year_start & death == 0) | (death == 1), ]
    
  } else {

    patient_long <- patient_long[is.na(censor_date) | (censor_date >= year_start), ]
  }
  
  #-----------------------------------------------------------------------------
  # Identify active patients: Create variable (weight) for proportion of year 
  # during which patient is active
  #-----------------------------------------------------------------------------
  
  # Create a weight variable that tells us how much of each panel year our patient is
  # active for
  patient_long[, weight :=  ifelse(
    (!is.na(death_date) & (death_date == censor_date)) | 
      (!is.na(death_date) & (death_date > censor_date) & death == 0) |
      (!is.na(death_date) & (death_date > censor_date) & death == 1 & censor_date >= year_start & censor_date <= year_end) |
      is.na(death_date), 
    (as.numeric(pmin(year_end, censor_date) - pmax(year_start, regstartdate))+1) /  (as.numeric(year_end - year_start)+1),
    ifelse(!is.na(death_date) & (death_date > censor_date) & death == 1 & censor_date < year_start, 
           0, NA))]
  
  # Note, we've kept a row for the year when the patient died but the weight will be negative
  # as the patient exited the panel before. Recode to have no negative weights.
  
  print(paste0("Written panel template for cohort ", i))
  
  #------------------------------------------------------------------------------
  # Construct condition panel for diseases with no remission (one date of diagnosis)
  #------------------------------------------------------------------------------
  diag_nonres <- nonres_conds_dt[patid %in% cohort$patid, ][
    , diag_date := as.character(diag_date)][
      , c('patid', 'list_disease_real', 'diag_date')
    ] 

  
  # Reshape the data to wide form to join on to the panel
  diag_nonres <- dcast(diag_nonres, patid ~ list_disease_real,
                       value.var = "diag_date",
                       fun.aggregate = min) %>%
    clean_names()
  
  
  # Note, the reshape operation makes these vars character, we need date
  diag_nonres[, (non_res_disease) := lapply(.SD, function(x) 
  {x = as.Date(x,"%Y-%m-%d")}), .SDcols = non_res_disease]
  
  # Join patient panel with diagnosis dates for each condition
  panel_nonres <- left_join(patient_long, diag_nonres)
  
  ## get diagnoses weights as proportion of each year, we call the variables prop_ as it is [0,1]
  #non_res_prop2 = c('prop_alcohol_problems', 'prop_chronic_liver_disease', 'prop_heart_failure', 'prop_hypertension', 'prop_ibs')
  #non_res_disease2 = c('alcohol_problems', 'chronic_liver_disease', 'heart_failure', 'hypertension', 'ibs')
  panel_nonres[, (non_res_prop) := 
                 lapply(.SD, function(x) {
                   calc_diag_weight(x, regstartdate, year_start, year_end, death_date, censor_date, death)})
               , .SDcols = non_res_disease]
  # Note, correct for rows where deaths occur after end of registration. Weight is negative currently but should be 0.
 
  # Create columns for incidence
  panel_nonres[, (non_res_inc) := 
              lapply(.SD, function(x) {
                
                fifelse(!is.na(x) & x >= year_start & x <= year_end, 1, 0 ) # If date of diagnosis is during the year
                
              })
            , .SDcols = non_res_disease]
  
  # Recode as not incidence if diagnosis is during the first year of registration 
  panel_nonres[, (non_res_inc) := 
              lapply(.SD, function(x) {
                
                fifelse(regstartdate >= (year_start %m-% years(1)), 0, x) # If date of diagnosis is during the year
                
              })
            , .SDcols = non_res_inc]
  
  # Remove the date variables
  panel_nonres <- panel_nonres[, (non_res_disease):= NULL]

  print(paste0("Written panel non res for cohort ", i))
  
  
  #------------------------------------------------------------------------------
  # Construct condition panel for diseases WITH remission 
  # ie, account for several possible dates of diagnosis and remission
  #------------------------------------------------------------------------------
  
  # Pick the earliest diagnosis date BY YEAR - merge to the panel by patid-year
  # here we also replace the diag date as character for reshape
  diag_res <- res_conds_dt[patid %in% cohort$patid & 
                           remission_date >= startdate, ][ # startdate is start of study period defined in project setup
                           , fyear := ifelse(month(diag_date)>=4, year(diag_date), year(diag_date)-1)] # this line has been amended by AR. Would have affected incidence estimates
  
  diag_res_diag <- diag_res[, diag_date := as.character(diag_date)][
    , c('patid', 'list_disease_real', 'diag_date', 'fyear')
  ]
  
  # Reshape the data wide (by year) to join on to the patient panel - for diagnosis dates
  diag_res_diag <- dcast(diag_res, patid + fyear ~ list_disease_real,
                         value.var = "diag_date" ) %>%
    clean_names()
  
  # Create separate columns for diagnosis (diag_) and remission (rem_) dates 
  diag_res_rem <- diag_res[, remission_date := as.character(remission_date)][
    , c('patid', 'list_disease_real', 'remission_date', 'fyear')
  ]
  
  diag_res_rem <- dcast(diag_res_rem, patid + fyear ~ list_disease_real,
                        value.var = "remission_date" )
  
  # Add prefix 'rem' to column names
  colnames(diag_res_rem)[-c(1, 2)] <- paste("rem", colnames(diag_res_rem[, -c(1, 2)]), sep = "_")
  
  # Merge wide datasets for diagnosis and remission dates
  diag_res_wide <- merge(diag_res_diag, diag_res_rem, by = c("patid", "fyear"), all = TRUE) %>%
    clean_names()
  
  # Fill in missing years
  diag_res_wide <- setkeyv(diag_res_wide[, fyear := as.character(fyear)], c("patid", "fyear"))
  
  panel_res <- merge(diag_res_wide,
                     patient_long[, c('patid', 'fyear')],
                     all = TRUE)
  
  panel_res <- panel_res[order(patid, fyear)] 
  
  # Roll diagnosis and remission dates forward (fill) for all conditions
  panel_res <- panel_res %>% 
    group_by(patid) %>%
    fill(-c(1, 2), .direction = "down") %>%
    setDT()
  
  # Restrict to study years (2008 - 2020)
  panel_res <- panel_res[ fyear >= 2007 & fyear < 2021, ]
  
  # The reshape makes these vars character, we need date
  colnames <- names(panel_res)[-c(1,2)]
  
  panel_res[, (colnames) := lapply(.SD, function(x) 
  {x = as.Date(x,"%Y-%m-%d")}), .SDcols = colnames]
  
  # Join with patient panel, by patid year
  panel_res <- left_join(patient_long, panel_res)
  
  # Calculate the share of year lived with disease...
  
  # First, calculate time since diagnosis.. and later substract time during remission
  temp_res_prop <- paste0("temp_prop_", res_disease)
  panel_res[, (temp_res_prop) := 
              lapply(.SD, function(x) {
                calc_diag_weight(x, regstartdate, year_start, year_end, death_date, censor_date, death)})
            , .SDcols = res_disease]
  # summary(panel_res)
  # check all values are between 0 and 1 and no NA values.
  
  # Calculate proportion of time in remission
  res_rem = paste0("rem_prop_", res_disease)
  rem_vec = paste0("rem_", res_disease)
  
  panel_res[, (res_rem) := 
              lapply(.SD, function(x) {
                calc_diag_weight(x, regstartdate, year_start, year_end, death_date, censor_date, death)})
            , .SDcols = rem_vec]
  
  # Substract time in remission to time with diagnosis
  for(vars in res_disease) {
    panel_res[ , paste0("prop_", vars) := panel_res[, get(paste0("temp_prop_", vars))] - panel_res[, get(paste0("rem_prop_", vars))]]
  }
  
  # Create columns for incidence
  panel_res[, (res_inc) := 
              lapply(.SD, function(x) {
                
                fifelse(!is.na(x) & x >= year_start & x <= year_end, 1, 0 ) # If date of diagnosis is during the year
                
              })
            , .SDcols = res_disease]
  
  # Recode as not incidence if diagnosis is during the first year of registration 
  panel_res[, (res_inc) := 
              lapply(.SD, function(x) {
                
                fifelse(regstartdate >= (year_start %m-% years(1)), 0, x) # If date of diagnosis is during the year
                
              })
            , .SDcols = res_inc]
  
  # Drop columns that are no longer needed
  panel_res <- panel_res[, c(res_disease, temp_res_prop, res_rem , rem_vec) := NULL]
  print(paste0("Written panel res for cohort ", i))
 
  # Merge panel for permanent and non permanent diseases into final panel
  panel <- merge(panel_res,
                 panel_nonres,
                 by = c('patid', 'regstartdate', 'death_date', 'fyear', 'year_start',
                        'year_end', 'death', 'censor_date', 'weight', 'yob'),
                 all = TRUE)
  
  # Add columns for number of conditions for each patient
  #panel[, nb_comorb := rowSums(.SD), .SDcols = inc_col]
  
  # Join with remaining patient information
  patient <- opendt(paste0(cleandata.filepath, 'interim/patient'),
                    cols_in = c(ident_vars, 'pracid'),
                    patient_list = cohort)
  
  panel_final <- merge(panel,
                       patient,
                       by = c("patid", "regstartdate", "censor_date", 
                              "death_date", "yob"),
                       all.x = TRUE)
  
  print(paste0("Panel all processed for cohort ", i))
  
  setcolorder(panel_final, c('patid', 'gender', 'yob', 'ethnicity', 'imd2015_decile', 
                             'region', 'fyear', 'pracid', 'regstartdate', 'censor_date', 'death_date'))

  write_parquet(panel_final,
                paste0(cleandata.filepath,
                       'panel/', i, '/data.parquet'))
  
  gc()
  
  print('written')
  
}

#-------------------------------------------------------------------------------
# Add condition weights to create the Cambridge Multimorbidity Score (CMS)
#-------------------------------------------------------------------------------


panel_final <- opendt(paste0(cleandata.filepath, 'panel'))

#Keep only conditions in line with Cambridge score
panel_final <- panel_final[, c("prop_diabetes_excl_type_2", # Note, we keep individual cancers which we use in IR2022
                               "prop_anxiety",
                               "prop_depression",
                               "prop_stroke",
                               "prop_tia",
                               "prop_connective_tissue_disorder",
                               "prop_rheumatoid_arthritis"):= NULL]

# Cam score general outcome weights
panel_final[, cam_score_general := (prop_depression_anxiety * 0.5) +
              (prop_hypertension * 0.08) +
              (prop_hearing_loss * 0.09) +
              (prop_ibs * 0.21) + 
              (prop_asthma * 0.19) +
              (prop_diabetes * 0.75) +
              (prop_coronary_heart_disease * 0.49) +
              #       (prop_chronic_kidney_disease_incl_test * 0.53) + 
              (prop_atrial_fibrillation * 1.34) +
              (prop_stroke_tia * 0.8) +
              (prop_copd * 1.46) +
              (prop_connective_tissue_cam * 0.43) +
              (prop_all_cancers * 1.53) +
              (prop_alcohol_problems * 0.65) +
              (prop_heart_failure * 1.18) +
              (prop_dementia * 2.5) +
              (prop_psychosis * 0.64) +
              (prop_epilepsy * 0.92) +
              (prop_pain * 0.92) +
              (prop_constipation * 1.12)]



