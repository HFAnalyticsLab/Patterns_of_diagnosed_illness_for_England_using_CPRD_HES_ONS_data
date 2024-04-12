# Objective: Create average prevalence and Cambridge Multimorbidity Score estimates from the panel aggregated by different patient characteristics
# ---------------------------------------------------------------------------------
# Create age categories and define necessary vectors
# ---------------------------------------------------------------------------------

  agebreaks <- c(0, 1, seq(5, 95, 5), Inf)
  df <- data.table(temp = c("0", "1", as.character(seq(5, 99, 5))),
                 temp2 = c("1", as.character(seq(4, 99, 5))))
  df[, agegrp_lbl := paste0(temp, "-", temp2)][
  , c('temp', 'temp2') := NULL ]
  agelabels <- df$agegrp_lbl # passing it to a vector
  rm(df)

  imd_deciles = c("1 most deprived", as.character(2:9), "10 least deprived")
  imd_quintiles = c("1 most deprived", as.character(2:4), "5 least deprived")
  gender <- c("F", "M")
  strata <- c('agecat', 'gender', 'imd_deciles')

  prop_vars <- grep("^prop", names(panel_final), value = TRUE)
  
  mort_den_pop_vars <- gsub("prop","mort_den_pop", prop_vars)
  mort_den_pop_cond_vars <- gsub("prop","mort_den_pop_cond", prop_vars)
  prev_vars <- gsub("prop","prev", prop_vars)

# ---------------------------------------------------------------------------------
# Creating age groups in panel
  panel_final[, age := (as.numeric(fyear) - yob)][
            , age := ifelse(age == -1, 0, age)][
            , agecat := cut(age,
                    breaks = agebreaks,
                    right = FALSE,
                    labels = agelabels)][
            , imd_deciles := case_when(imd2015_decile == 1 ~ "1 most deprived",
                    imd2015_decile >= 2 & imd2015_decile <= 9 ~ as.character(imd2015_decile),
                    imd2015_decile == 10 ~ "10 least deprived")
            ]
  gc()

  #-----------------------------------------------------------------------------
  # Population characteristics
  #-----------------------------------------------------------------------------
  
  # Constructing age weights for the age standardisation
  source('./Scripts/06.1_standardisation.R')
  
  # Join on the standardisation table (with different weights for each population groups) to the panel
  panel_final <- left_join(panel_final, ESP, by = c(strata, 'fyear'))
  gc()
  
  # age range for results:
  beg <- 30
  end <- 99 # feel free to edit - we used 30-99 and 20-99 in our recent work
  
  # and prevalence estimates used for inequalities (which is 20-99) so from here on the whole code would repeat with just the age filtering being different
  panel_cam_30to99_active <- panel_final[which(age >= beg & age <= end)][which(fyear >= 2008 & fyear < 2020)][
    which(weight !=0)][ # Note, here we remove patients who are inactive in the sample but died in the year but we keep them for mortality
    , c("prop_chronic_kidney_disease", "prop_obesity", "prop_chronic_liver_disease") := NULL][
    , `:=` (dum_cam_score_greater_0 = ifelse(cam_score_general > 0, 1, 0),  # CMS > 0 AND > 1.5
            dum_cam_score_greater_1.5 = ifelse(cam_score_general > 1.5, 1, 0))][  
        , popsize_tot := length(unique(patid)), 
                                    by = c('fyear')][
        , popsize := length(unique(patid)), 
                                  by = c('fyear', strata) ] # calculate total population before collapsing to aggregate dataset
                                                      
  gc()
   
  #-------------------------------------------------------------------------------
  # CMS standardised and non-standardised by demographics 
  # NOTE, we use weighted.mean function which normalises all weights to 1.
  # We therefore do not need to recalibrate for the different dimensions.
  #-------------------------------------------------------------------------------
  
  panel_cam_30to99_cms <- panel_cam_30to99_active[, lapply(.SD, mean),
                                           by = c('fyear', 'imd_deciles', 'imd2015_decile', 'agecat',
                                                  'gender', 'popsize_tot', 'popsize', 'wt_esp'),
                                           .SDcols = 'cam_score_general'][
                                            order(fyear, agecat, gender, imd_deciles)]
  
  panel_cam_30to99_cms[, `:=` (tot_wt_esp = sum(wt_esp)), by = 'fyear'][
    , `:=` (actual_pop_wt = (popsize/popsize_tot),
            standard_pop_wt = (wt_esp/tot_wt_esp))]

  
  # NON-STANDARDISED Cambridge score 
  # By year
  cms_nonstan_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                     .SDcols = 'cam_score_general',
                                     by = fyear ][
                                     order(fyear)]
  
  
  # By year and age group
  cms_nonstan_age_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                            .SDcols = 'cam_score_general',
                                            by = c('fyear', 'agecat') ][
                                            order(fyear, agecat)]
                                          
  
  # By year, sex and age group
  cms_nonstan_age_sex_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                               .SDcols = 'cam_score_general',
                                               by = c('fyear', 'agecat', 'gender') ][
                                                 order(fyear, gender, agecat)]
  
 
  # By year, sex, age group and imd
  cms_nonstan_age_sex_imd_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                                   .SDcols = 'cam_score_general',
                                                   by = c('fyear', 'agecat', 'gender', 'imd_deciles', 'imd2015_decile') ][
                                                     order(fyear, imd_deciles, gender, agecat)]
  
  # By year and sex
  cms_nonstan_sex_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                            .SDcols = 'cam_score_general',
                                            by = c('fyear', 'gender')][
                                              order(fyear, gender)]
  

  # By year and IMD
  cms_nonstan_imd_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                                .SDcols = 'cam_score_general',
                                                by = c('fyear', 'imd_deciles', 'imd2015_decile') ][
                                                order(fyear, imd2015_decile)]
                                                
  
  #-----------------------------------------------------------------------------
  # Age-sex-deprivation standardised CMS scores
  #-----------------------------------------------------------------------------
  
  # By year
  cms_stan_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                          .SDcols = 'cam_score_general',
                          by = fyear ][
                          order(fyear)]
                          
  
  # With age breakdown
  cms_stan_age_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                                .SDcols = 'cam_score_general',
                                by = c('fyear', 'agecat')][
                                order(fyear, agecat)]
                                
  
  # By year, and sex
  cms_stan_sex_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                                            .SDcols = 'cam_score_general',
                                            by = c('fyear', 'gender')][
                                            order(fyear, gender)]
  
  # By year, and sex
  cms_stan_age_sex_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                                            .SDcols = 'cam_score_general',
                                            by = c('fyear', 'gender', 'agecat')][
                                            order(fyear, agecat, gender )]
  
  
  # By year and IMD
  cms_stan_imd_year <- panel_cam_30to99_cms[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                                .SDcols = 'cam_score_general',
                                by = c('fyear', 'imd_deciles', 'imd2015_decile')][
                                order(fyear, imd2015_decile)]
                    
  
  
  # Exporting to output
  list_files <- list('cms_nonstan_year' = cms_nonstan_year,
                     'cms_nonstan_sex_year' = cms_nonstan_sex_year,
                     'cms_nonstan_age_year' = cms_nonstan_age_year,
                     'cms_nonstan_imd_year' = cms_nonstan_imd_year,
                     'cms_nonstan_age_sex_year' = cms_nonstan_age_sex_year,
                     'cms_nonstan_age_sex_imd_year' = cms_nonstan_age_sex_imd_year,
                     'cms_stan_year' = cms_stan_year, 
                     'cms_stan_sex_year' = cms_stan_sex_year,
                     'cms_stan_age_sex_year' = cms_stan_age_sex_year,
                     'cms_stan_age_year' = cms_stan_age_year,
                     'cms_stan_imd_year' = cms_stan_imd_year)
  
  s3write_using(FUN = write.xlsx,
                x = list_files,
                object = paste0(output.filepath, 'REAL_CMS_estimates.xlsx'),
                col_names = TRUE)
  
  # Clearing the workspace
  rm(list = ls(pattern = "^cms"), panel_cam_30to99_cms)
  
  #-------------------------------------------------------------------------------
  # Disease prevalence - standardised and non-standardised by demographics 
  #-------------------------------------------------------------------------------
  
  # Prevalence for Cambridge conditions
  ## for conditions in panel
  prop_vars <- intersect(prop_vars, names(panel_cam_30to99_active))
  
  # Calculate prevalence rates for each population subgroup
  panel_cam_30to99_prev <- panel_cam_30to99_active[, (prev_vars) := lapply(.SD, function(x) { 
                                            (sum(fifelse(x> 0, 1, 0)) / sum(fifelse(weight>0, 1, 0))) 
                                            }),  # Number of existing or new cases, divided by active pop in sample
                                            by = c('fyear', 'imd_deciles', 'imd2015_decile', 'agecat',
                                                   'gender', 'popsize_tot', 'popsize', 'wt_esp'),
                                            .SDcols = prop_vars]
  
  panel_cam_30to99_prev <- panel_cam_30to99_prev %>% 
    select('fyear', 'imd_deciles', 'imd2015_decile','agecat',
             'gender', 'popsize_tot', 'popsize', 'wt_esp', c(starts_with('prev_'))) %>% 
    unique() %>% 
    setDT()
  
  # Calculating the weights (note, technically the weighted.mean function would calibrate them to 1 automatically as well)
  panel_cam_30to99_prev[, `:=` (tot_wt_esp = sum(wt_esp)), by = 'fyear'][
                        , `:=` (actual_pop_wt = (popsize/popsize_tot),
                        standard_pop_wt = (wt_esp/tot_wt_esp))]
  
  #-----------------------------------------------------------------------------
  # Prevalence = 1 if have the disease during the year
  # Not standardised
  #-----------------------------------------------------------------------------
  prev_cond_nonstan_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                                    .SDcols = prev_vars,
                                                    by = fyear ][
                                                      order(fyear)]
    
  # By year
  prev_cond_nonstan_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                            .SDcols = prev_vars,
                                            by = fyear ][
                                            order(fyear)]
                                            
  
  # By year and age
  prev_cond_nonstan_age_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                                  .SDcols = prev_vars,
                                                  by = c('fyear', 'agecat') ][
                                                    order(fyear, agecat)]
                                                  
  # By year and sex
  prev_cond_nonstan_sex_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                                      .SDcols = prev_vars,
                                                      by = c('fyear', 'gender') ][
                                                        order(fyear, gender)]
  
  
  # By year, age group and sex
  prev_cond_nonstan_age_sex_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                                      .SDcols = prev_vars,
                                                      by = c('fyear', 'gender', 'agecat') ][
                                                        order(fyear, gender)]
  
  # By year, age group, sex and IMD
  prev_cond_nonstan_age_sex_imd_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                                          .SDcols = prev_vars,
                                                          by = c('fyear', 'gender', 'agecat', 'imd_deciles', 'imd2015_decile') ][
                                                            order(fyear, gender, agecat, imd2015_decile)]
  
  # By year and IMD
  prev_cond_nonstan_imd_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, actual_pop_wt, na.rm = TRUE),
                                                      .SDcols = prev_vars,
                                                      by = c('fyear', 'imd_deciles', 'imd2015_decile') ][
                                                        order(fyear, imd2015_decile)]
                                                      
  #-----------------------------------------------------------------------------
  # STANDARDISING BY AGE GROUP, SEX OR IMD
  #-----------------------------------------------------------------------------
  
  # By year
  prev_cond_stan_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                            .SDcols = prev_vars,
                            by = fyear ][
                            order(fyear)]
  
                            
  # By year and sex
  prev_cond_stan_sex_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                                                   .SDcols = prev_vars,
                                                   by = c('fyear', 'gender') ][
                                                     order(fyear, gender)]
  
  # By year and age group
  prev_cond_stan_age_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                                            .SDcols = prev_vars,
                                            by = c('fyear', 'agecat') ][
                                              order(fyear, agecat)]
                                            
  # By year and age group
  prev_cond_stan_age_sex_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                                                   .SDcols = prev_vars,
                                                   by = c('fyear', 'agecat', 'gender') ][
                                                     order(fyear, agecat, gender)]
  
  
  # By year and IMD
  prev_cond_stan_imd_year <- panel_cam_30to99_prev[, lapply(.SD, weighted.mean, standard_pop_wt, na.rm = TRUE),
                                              .SDcols = prev_vars,
                                              by = c('fyear', 'imd_deciles', 'imd2015_decile') ][
                                                order(fyear, imd2015_decile)]
                                              
  
  rm(panel_cam_30to99_prev)
  
  
  # Exporting to output
  list_files <- list('prev_cond_nonstan_year' = prev_cond_nonstan_year,
                     'prev_cond_nonstan_sex_year' = prev_cond_nonstan_sex_year,
                     'prev_cond_nonstan_age_year' = prev_cond_nonstan_age_year,
                     'prev_cond_nonstan_imd_year' = prev_cond_nonstan_imd_year,
                     'prev_cond_nonstan_age_sex_year' = prev_cond_nonstan_age_sex_year,
                     'prev_cond_age_sex_imd_year' = prev_cond_nonstan_age_sex_imd_year,
                     'prev_cond_stan_year' = prev_cond_stan_year, 
                     'prev_cond_stan_age_sex_year' = prev_cond_stan_age_sex_year,
                     'prev_cond_stan_sex_year' = prev_cond_stan_sex_year,
                     'prev_cond_stan_age_year' = prev_cond_stan_age_year,
                     'prev_cond_stan_imd_year' = prev_cond_stan_imd_year)
  
  s3write_using(FUN = write.xlsx,
                x = list_files,
                object = paste0(output.filepath, 'REAL_prev_cond_estimates.xlsx'),
                col_names = TRUE)
  
  rm(list = ls(pattern = "^prev_cond"))
  