# ---------------------------------------------------------------------------------
# 03. Create a long panel with patient IDs, conditions and date of diagnosis
# from CPRD and HES. Different rules depending on whether the disease is permanent.
# ---------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
#  Create dates of diagnosis and remission from HES 
# ------------------------------------------------------------------------------

# Open HES dataset with diagnosis Admitted patient care (APC) # Modified to clean epistart date when missing
apc_diag <- opendt(paste0(cleandata.filepath,
              'hes_cohorts/hes_diagnosis_epi'),
              cols_in = c("patid", "epistart", "icd"))[
              , obsdate := dmy(epistart)][
              , epistart := NULL] %>%
              setnames(., "icd", "ICD10code")

apc_diag_tomerge <- unique(apc_diag[icd_lookup, on = "ICD10code", nomatch = 0][ # inner join, keep only matched diagnoses
                     , source := "HES"][
                     , ICD10code := NULL][
                     , remission_date := as.Date(ifelse(disease_duration < 999999, obsdate + disease_duration, enddate), origin = '1970-01-01')
    ]) 
rm(apc_diag)

# Adding diagnosis made in outpatient data
## use hesop_clinical and hesop_appointment
op_diag <- opendt(paste0(cleandata.filepath,
                         "hes_cohorts/hes_op/"),
                  cols_in = c('patid', 'diag_01', 'diag_02', 'diag_03', 'apptdate'))

# Reshaping to long format with diag01 to diag03
op_diag_long <- melt(op_diag, id.vars = c('patid', 'apptdate'), 
                     variable.name = "diag", 
                     value.name = "icd")[
                      !(icd == ""),][
                      , diag := NULL]

rm(op_diag)
gc()

# ICD10 codes are formatted differently in the outpatient data - so correct for that
op_diag_long[, icd := str_replace_all(icd, "[^[:alnum:]]", "")]
op_diag_long[, ICD10code := paste0(substr(icd, 1, 3), ".", substr(icd, 4, 5))] # Note, here we get rid of the last digit
op_diag_long[, ICD10code := str_replace_all(ICD10code, ".X", ".0")]

setnames(op_diag_long, "apptdate", "obsdate")
op_diag_long$obsdate <- dmy(op_diag_long$obsdate)

op_diag_tomerge <- unique(op_diag_long[icd_lookup, on = "ICD10code", nomatch = 0][ # inner join, keep only matched diagnoses
                          , source := "HES"][
                          , c('ICD10code', 'icd') := NULL][
                          , remission_date := as.Date(ifelse(disease_duration < 999999, 
                                                             obsdate + disease_duration, 
                                                             enddate), origin = '1970-01-01')]) 
rm(op_diag, op_diag_long)
gc()


# Merging both sources - APC and OP
diag_hes_all <- rbind(apc_diag_tomerge, op_diag_tomerge) %>% unique()
rm(apc_diag_tomerge, op_diag_tomerge)

# Codelist for resolving conditions
res_codelist <- codelist_CPRD[list_disease_real %in% res_conds, ]

# Identify conditions that resolve (eg. depression, cancer) and those that don't.
nonres_codelist <- codelist_CPRD[list_disease_real %in% nonres_conds, ]

res_list <- list() ## use lists in place of rbind
nonres_list <- list()

# create diagnoses tables for patients
for (i in 1:nb_cohorts){
    
  cohort <- opendt(paste0(cleandata.filepath,
                          'cohorts/', i, '/data.parquet'),
                   cols_in = c('patid')) 

  diag_hes <- merge(diag_hes_all, cohort, by = 'patid')[
      , obsdate := as.Date(obsdate)][
        obsdate <= as.Date(enddate), 
    ]
  # ------------------------------------------------------------------------------
  #  Create dates of diagnosis and remission from CPRD 
  # ------------------------------------------------------------------------------

  # Get diagnoses from observation file 
  # NOTE we don't need to filter to cohort as we know obs clean is already arranged by cohort
  temp_diag_obs <- opendt(paste0(cleandata.filepath,
                                 'obs/',
                                 i,
                                 '/data.parquet'),
                      cols_in = c('patid', 'medcodeid', 'obsdate', 'obstypeid'))[ 
                      medcodeid %in% codelist_CPRD$medcodeid & 
                      obstypeid != 4 & 
                      obsdate <= as.Date(enddate), ] 
  # Only those in the condition list (medcodes) and not family history and not after the required date
  # Note: obstypeid = 4 is family history.

  temp_diag_obs[, obsdate := as.Date(obsdate, origin = '1970-01-01')]
  
  # Select just the date, medcode and patid variables
  data.table::setkey(temp_diag_obs, patid, obsdate) # sort the data
  temp_diag_obs <- temp_diag_obs[, .(patid, medcodeid, obsdate)] %>% 
        unique()
  
  #------------------------------------------------------------------------------
  # Non-resolving/permanent conditions: find earliest date of diagnosis 
  #------------------------------------------------------------------------------
  
  # Condition per patient, first, last diagnosis and number of observations with diagnosis (mcount)
  diag_nonres_conds <- temp_diag_obs[nonres_codelist, on = .(medcodeid), allow.cartesian = TRUE][ #restrict to relevant medcodes using a join
    obsdate <= enddate & obsdate >= (enddate - read)][ 
      # Here, we filter to have observations that are before the end of the study period (obsdate < enddate)
      # AND only 'new' diagnosis observations (that are after the disease duration).
      , .(oldest_date = min(obsdate), source = 'CPRD', remission_date = as.Date(enddate))
      , by = .(patid, disease_duration = read, list_disease_real)] #count by patient/condition
  # Note, added a variable with number of diagnosis and indicating source is CPRD (for later comparison with HES diagnosis)
  
  # Add HES diagnoses 
  diag_hes_nonres_conds <- diag_hes[list_disease_real %in% nonres_conds, ][ # Subset of resolving conditions
    , .(patid,  list_disease_real,  disease_duration, oldest_date = obsdate, source, remission_date)]
  
  diag_nonres_conds <- rbind(diag_nonres_conds, diag_hes_nonres_conds)
  
  # Create diag date as the earliest of dates in both sources (HES and CPRD)
  diag_nonres_conds <- diag_nonres_conds[order(patid, list_disease_real, oldest_date)][, 
                               .(diag_date = min(oldest_date), source = head(source, 1), remission_date = head(remission_date, 1)), 
                               by = c('patid', 'list_disease_real')]
  
  # Duplicates - use either a diagnosis or a test, whichever is first
  diag_nonres_conds <- diag_nonres_conds[order(patid, list_disease_real, diag_date), 
                    first_epi := fifelse(row.names(.SD) == 1, 1, 0), 
                    by = .(patid, list_disease_real)]
  diag_nonres_conds <- diag_nonres_conds[which(first_epi == 1)][, first_epi := NULL]
  
  nonres_list[[i]] <- diag_nonres_conds
  print(paste0("completed ", i, " of ", nb_cohorts, " non-resolving diagnoses"))

  #-----------------------------------------------------------------------------
  # Conditions that can resolve: estimate date(s) of diagnoses and remission
  #-----------------------------------------------------------------------------
  
  # Obtain all dates of diagnosis for conditions that resolve from CPRD
  diag_res_conds <- temp_diag_obs[res_codelist, on = .(medcodeid), allow.cartesian = TRUE][ 
                            obsdate <= enddate][
                            !is.na(patid)][
                            , source := 'CPRD'][
                            ,.(patid,  list_disease_real,  disease_duration = read + 1, obsdate, source)] %>%
                            unique() # Adding 1 day to account for leap years
  
  # Add HES diagnoses 
  diag_hes_res_conds <- diag_hes[list_disease_real %in% res_conds, ][ # Subset of resolving conditions
                            , remission_date := NULL] %>% unique()
  diag_res_conds <- rbind(diag_res_conds, diag_hes_res_conds)
  
  # Assess whether diagnosis is new disease spell using remission date of previous obs
  setkey(diag_res_conds, patid, list_disease_real, obsdate)
  
  diag_res_conds <- diag_res_conds[order(patid, list_disease_real, obsdate)][, 
                         temp_lag_diag := shift(.SD, n = 1, fill = 0, "lag"), 
                         by = c('patid', 'list_disease_real'), .SDcols = 'obsdate'] # Lag column by 1
  # Note, R creates '1970-01-01' for first episode of patient (when no existing lag value).
  # BUT changing this to NA affects the calculation of spells later so leave as it.

  # Note, R creates '1970-01-01' for first episode of patient (when no existing lag value). Flag the first episode for later
  diag_res_conds <- diag_res_conds[order(patid, list_disease_real, obsdate)][
        , first_epi := fifelse(row.names(.SD) == 1, 1, 0), 
        by = .(patid, list_disease_real)]
  
  # Here, distinguish between non-cancers and cancer (slightly different rules for resolving)  
  diag_res_conds_noncancer <- diag_res_conds[list_disease_real %in% list_resolve_non_cancer, ]
  
  # Identify 'spells' of diseases - diagnosis dates that are 'X days' apart from each other are not new diagnoses.
  diag_res_conds_noncancer[ , flag_date := fcase(first_epi == 1 | (obsdate - temp_lag_diag) > disease_duration, 1, # Always count as 1 first episode
                                                 first_epi == 0 & (obsdate - temp_lag_diag) <= disease_duration, 0)][
        , spell := cumsum(flag_date),
        by = c("patid", "list_disease_real")][ 
      # Derive date of diagnosis and remission date by 'spell'
      , `:=` (diag_date = min(obsdate),
              remission_date = max(obsdate) + disease_duration ),
      by = c("patid", "list_disease_real", "spell")]
     
  # Restrict dataset to 'spells'   
  diag_res_conds_noncancer <- diag_res_conds_noncancer[ which(diag_res_conds_noncancer$flag_date == 1), ][ # first episode of spell
                       ,.(patid, list_disease_real, diag_date,
                      remission_date, disease_duration, source)]
  
  # For individual cancers
  # Change disease duration to 10 years (3652.5 days)
  diag_res_conds_ind_cancer <- diag_res_conds[list_disease_real %in% list_ind_cancers, ]
  
  # Identify 'spells' of diseases - diagnosis dates that are 'X days' apart from each other are not new diagnoses.
  diag_res_conds_ind_cancer <- diag_res_conds_ind_cancer[ , flag_date := fcase(first_epi == 1 | (obsdate - temp_lag_diag) > 3653, 1, 
                                                  first_epi == 0 & (obsdate - temp_lag_diag) <= 3653, 0)][
                            , spell := cumsum(flag_date),
                            by = c("patid", "list_disease_real")][
  # For cancers, only allow one type of cancer per patients (ie. not possible to have lung cancer twice). We drop the second spell 
    which(spell == 1), ][ 
  # Derive date of diagnosis and remission date by 'spell' - here, use the first diagnosis
    , `:=` (diag_date = min(obsdate),
          remission_date = min(obsdate) + 3653),
    by = c("patid", "list_disease_real", "spell")]

  # Restrict dataset to 'spells'   
  diag_res_conds_ind_cancer <- diag_res_conds_ind_cancer[ which(diag_res_conds_ind_cancer$flag_date == 1), ][ # first episode of spell
     ,.(patid, list_disease_real, diag_date,
     remission_date, disease_duration, source)]
  
  # Now calculate diagnosis and remission for all cancers, starting by individual cancers and then all cancers. 
  # Note, diagnosis is the first diagnosis of cancer (any type) and remission is calculated
  # from the first diagnosis of the last type of cancer.
  
  diag_res_conds_all_cancer <- copy(diag_res_conds_ind_cancer) # Create a copy to avoid overwriting the other file
  diag_res_conds_all_cancer <- diag_res_conds_all_cancer[, list_disease_real := "All cancers"][
          , remission_date := NULL]
  
  setnames(diag_res_conds_all_cancer, "diag_date", "obsdate")
  diag_res_conds_all_cancer[ , `:=` (diag_date = min(obsdate),
          remission_date = max(obsdate) + 3653),
          by = c("patid", "list_disease_real")] # not more than one spell for all cancers
  
  # Restrict database to one observation per patient for all cancers
  diag_res_conds_all_cancer <- diag_res_conds_all_cancer[
          , dummy := ifelse(row.names(.SD) == 1, 1, 0),
          by = c("patid", "list_disease_real")][ # Identify the first observation by patient
          which(diag_res_conds_all_cancer$dummy == 1), ][ # Keep one episode, source may differ, keep the first one.
          ,.(patid, list_disease_real, diag_date,
          remission_date, disease_duration, source)]
  
  # Merge dataset back to the individual cancers
  diag_res_conds_cancer <- rbind(diag_res_conds_all_cancer, diag_res_conds_ind_cancer)[
    order(patid, list_disease_real, diag_date)
  ]
  
  # Merge all resolving conditions together
  diag_res_conds <- rbind(diag_res_conds_noncancer, diag_res_conds_cancer)[
    order(patid, list_disease_real, diag_date)
  ]
  
  res_list[[i]] <- diag_res_conds
  
  print(paste0("completed ", i, " of ", nb_cohorts, " resolving diagnoses"))
 
  }

res_conds_dt <- rbindlist(res_list)
nonres_conds_dt <- rbindlist(nonres_list)


# Clear workspace
rm(list = ls(pattern = "^temp")) 
rm(cohort, diag_hes, diag_hes_all, diag_hes_nonres_conds, diag_hes_res_conds,  
   diag_nonres_conds, diag_res_conds,res_list, nonres_list, hist_list, diag_hist_conds)

gc()

#-------------------------------------------------------------------------------
# Adding conditions based on drug prescriptions (prodcodes)
#-------------------------------------------------------------------------------


if(use_prodcodes){
  
  source('03.1_diagnoses_using_prodcodes.R')
  
}

#-------------------------------------------------------------------------------
# Cleaning remission dates
#-------------------------------------------------------------------------------

if(spell_grouping){
  
  source('03.2_spell_grouping.R')
  
}

# save interim data
write_parquet(nonres_conds_dt, paste0(cleandata.filepath, 'interim/nonres_conds/data.parquet'))
write_parquet(res_conds_dt, paste0(cleandata.filepath, 'interim/res_conds/data.parquet'))

