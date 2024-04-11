# ---------------------------------------------------------------------------------
# Project: Liverpool demand modelling/Insight Report 2022
# Objective: Implement cleaning rules 
# Date: 26/01/2022
# ---------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------
# Observation files: obsdates vs enterdates
# ---------------------------------------------------------------------------------
if (clean_obs){

   for (i in 1:nb_cohorts){

    cohort <- opendt(paste0(cleandata.filepath, 'cohorts/', i, '/data.parquet'),
                     cols_in = c('patid', 'yob')) # year of birth
    
    observation <- opendt(paste0(cleandata.filepath, 'cprd/Data/observation'), 
                          cols_in = c('patid',
                                      'obsid',
                                      'parentobsid',
                                      'medcodeid',
                                      'obsdate',
                                      'enterdate',
                                      'obstypeid',
                                      'value'),
                          patient_list = cohort) 
    
    observation$patid <- as.integer64(observation$patid)
    observation <- merge(observation, cohort, by = 'patid') # adding patient's year of birth
    
    # use enterdate if obsdate is missing
    observation[, obsdate := ifelse(is.na(obsdate), enterdate, obsdate)] # currently no missing
    observation$obsdate <- as.Date(observation$obsdate, origin = '1970-01-01')
    
    # replace by enterdate if implausible dates or drop
    observation <- observation[, flag_obsdate := fifelse(obsdate < '1900-01-01' | obsdate > '2023-01-01', 1, 0)]
    observation <- observation[, flag_enterdate := ifelse(enterdate < '1900-01-01' | enterdate > '2023-01-01', 1, 0)]
    observation <- observation[, obsdate := ifelse(flag_obsdate == 1 & flag_enterdate == 0, enterdate, obsdate)]
    observation <- observation[, todrop := ifelse(flag_obsdate == 1 & flag_enterdate == 1, 1, 0)]
    observation <- observation[ which(todrop == 0), ] # Drop observations
    observation <- observation[ , c('flag_obsdate', 'flag_enterdate', 'todrop') := NULL]
 
    observation$obsdate <- as.Date(observation$obsdate, origin = '1970-01-01')
    
    # use enterdate if obsdate if after year of birth; otherwise drop
    observation <- observation[, flag_obsdate := ifelse(year(obsdate) < yob, 1, 0)]
    observation <- observation[, flag_enterdate := ifelse(year(as.Date(enterdate)) < yob, 1, 0)]
    observation <- observation[, obsdate := ifelse(flag_obsdate == 1 & flag_enterdate == 0, enterdate, obsdate)]
    observation <- observation[, todrop := ifelse(flag_obsdate == 1 & flag_enterdate == 1, 1, 0)]
    observation <- observation[ which(todrop == 0), ] # Drop observations
    observation <- observation[ , c('flag_obsdate', 'flag_enterdate', 'todrop') := NULL]
    
    # Save file
    write_parquet(observation, 
                  paste0(cleandata.filepath
                         , 'obs/' , i, '/data.parquet'))
  
    print(paste0('Observation cohort ', i, ' processed'))
    
  }

  rm(observation, cohort)
  gc()
  
}


# ---------------------------------------------------------------------------------
# Drug issue files - setup for faster panel creation
# ---------------------------------------------------------------------------------
if (clean_drugs){
  
  for (i in 1:nb_cohorts){
    
    cohort <- opendt(paste0(cleandata.filepath, 'cohorts/', i, '/data.parquet'),
                     cols_in = 'patid') 
    
    drug <- opendt(paste0(cleandata.filepath, 'cprd/Data/drugissue'), 
                          cols_in = c('patid',
                                      'prodcodeid',
                                      'issuedate'),
                          patient_list = cohort) 

    # Save file
    ## Should really be saving these in a separate folder
    write_parquet(drug, paste0(cleandata.filepath, 'drug/', i, '/data.parquet'))
    
    print(paste0('Drug issue cohort ', i, ' processed'))
    
  }

  rm(drug, cohort)
  gc()
  
}


  # ---------------------------------------------------------------------------------
  # HES diagnosis files - clean epistart.. like for the observation files in CPRD
  # ---------------------------------------------------------------------------------
if (clean_hes_epi){ #RULE TO DEFINE
    
    for (i in 1:nb_cohorts){
      
      cohort <- opendt(paste0(cleandata.filepath, 'cohorts/', i, '/data.parquet'),
                       cols_in = 'patid') # year of birth
  
      apc_diag <- opendt(paste0(cleandata.filepath, 'hes/', grep('hes_diagnosis_epi', hes_filenames$name, value = TRUE)),
                         patient_list = cohort)
   
      apc_diag <- apc_diag[, epistart := fifelse(is.na(epistart) & !is.na(epiend), epiend, epistart)]
      
      # Save file -  for now in a separate folder
      write_parquet(apc_diag, 
                    paste0(cleandata.filepath,
                           'hes_cohorts/hes_diagnosis_epi/',
                           i, '/data.parquet'))
      
      print(paste0('HES epi cohort ', i, ' processed'))
      
    }
  
}

rm(apc_diag) 

## Create HES outpatient files by cohort for faster panel creation
if (clean_hes_op) {
  for (i in 1:nb_cohorts){
    
    cohort <- opendt(paste0(cleandata.filepath, 'cohorts/', i, '/data.parquet'),
                     cols_in = 'patid') # year of birth
    
    HESop_diag <- opendt(paste0(cleandata.filepath, 'hes/', grep('hesop_clinical', hes_filenames$name, value = TRUE)),
                       patient_list = cohort)
    
    HESop_date <- opendt(paste0(cleandata.filepath, 'hes/', grep('hesop_appointment', hes_filenames$name, value = TRUE)),
                         patient_list = cohort,
                         cols_in = c('patid', 'attendkey', 'apptdate'))
    
    op_diag <- HESop_diag[HESop_date, on = c('patid', 'attendkey'), nomatch = 0]
    # Note, recording of diagnosis with ICD10 codes is not systematic for outpatient data.
    
    # Save file -  for now in a separate folder
    write_parquet(op_diag, 
                  paste0(cleandata.filepath,
                         'hes_cohorts/hes_op/',
                         i, '/data.parquet'))
    
    print(paste0('HES op cohort ', i, ' processed'))
    
  }
}

rm(HESop_date, HESop_diag, op_diag)

## Cleaning rule #28: replace missing evdate by epistart (HES procedure)
if (clean_hes_proc){ #RULE TO DEFINE
    
    for (i in 1:nb_cohorts){
      
      cohort <- opendt(paste0(cleandata.filepath, 'cohorts/', i, '/data.parquet'),
                       cols_in = 'patid') # year of birth
      
      apc_procedure <- opendt(paste0(cleandata.filepath, 'hes/', grep('hes_procedures_epi', hes_filenames$name, value = TRUE)),
                         patient_list = cohort)
    
      apc_procedure[, evdate := fifelse(is.na(evdate) & !is.na(epistart), epistart, evdate)]
      
      # Save file -  for now in a separate folder
      write_parquet(apc_procedure, 
                    paste0(cleandata.filepath,
                           'hes_cohorts/hes_procedure/',
                           i, '/data.parquet'))
      
      print(paste0('HES procedure cohort ', i, ' processed'))
      
    }

}
rm(apc_procedure)
  
  