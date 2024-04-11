#------------------------------------------------------------------------------------------------------------------------------------
# Project: Liverpool Demand Modelling
# Objective: Create list of diagnoses for pain and constipation using drugs data and Cambridge rules
# Date: 10/03/2022
#------------------------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------
# Read in obs and drugs data and lookup files
# ----------------------------------------------

#Read in lookup tables
emis_prodcodes <- s3read_using(FUN = vroom,
                               object = paste0(emis_codelist, '202205_EMISProductDictionary.txt'))

emis_prodcodes$dmdid <- as.integer64(emis_prodcodes$dmdid) # for merge later on
emis_prodcodes$ProdCodeId <- as.integer64(emis_prodcodes$ProdCodeId) # for merge later on

gemscript_dmd <- s3read_using(FUN = vroom,
                              object = paste0(codelist_path, 'gemscript_dmd_map_May20.txt'), 
                       delim = '|', col_types = 'IcI') #Vision gemscript code list

#Read in medcodes to flag epilepsy to later make the right diagnoses for PNC167
codelist_ep <- codelist_CPRD[disease == "Epilepsy", ]

#Read in the lookup files and merge to map GOLD to Aurum 
prodcodes <- readcam(file_pattern = '_PC_V1-1_', coltypes = 'IccI') %>% #use function to read in Cambridge drug lists
  merge(gemscript_dmd, by.x = 'gemscriptcode', by.y = 'gemscript_drug_code') %>% #join on gemscript lookup
  merge(emis_prodcodes, by.x = 'dmd_code', by.y = 'dmdid') %>% #join on product dictionary to get Aurum prodcodes
  .[, .(cond, dmd = dmd_code, prodcodeid = ProdCodeId)] %>% #reduce to required columns
  .[which(cond == "CON150" | cond == "PNC166" | cond == "PNC167"),] %>%
  unique() #ensure unique rows are returned (there are some repeated values in the original code lists)

drug_list <- list() ## use list as quicker than rbind within loop

#Run whole script in loops for cohorts
for (i in 1:nb_cohorts) {
  
  cohort <- opendt(paste0(cleandata.filepath, "cohorts/", i, "/data.parquet"),
                   cols_in = 'patid')
  

  #Open obs files and filter to epilepsy medcodes
  # NOTE we don't need to filter to cohort as we know obs clean is already arranged by cohort
  temp_diag_obs <- opendt(paste0(cleandata.filepath,
                                 "obs/",
                                 i,
                                 "/data.parquet"),
                          cols_in = c('patid', 'medcodeid', 'obsdate', 'obstypeid'))[ 
                            medcodeid %in% codelist_ep$medcodeid & 
                              obstypeid != 4 & 
                              obsdate <= as.Date(enddate), ] 
  # Only those in the condition list (medcodes) and not family history and not after the required date
  # Note: obstypeid = 4 is family history.

  temp_diag_obs <- temp_diag_obs[,.(patid, obsdate)][
    , .(epi_diag_date = min(obsdate)), by = .(patid)][
      , epi_flag := 1]  %>% unique()
  # Note, to do with date of diagnosis for epilepsy
  
  #Read in drug files 
  drugs <- opendt(paste0(cleandata.filepath,
                          'drug/',
                          i,
                          '/data.parquet'))
                   
  drugs <- drugs[prodcodes, on = .(prodcodeid)][ #restrict to relevant prodcodes using a join
           issuedate <= enddate][
           ,.(patid, issuedate, prodcodeid, cond)][
             order(patid, cond, issuedate)]
  
  #Cambridge rules say individual diagnosed with PNC or CON if 4 or more prescriptions in a year
  #Creating a count variable and dropping people with less than 4 prescriptions
  drugs_count <- drugs[,.(count = .N), by = .(patid, cond)][
                 which(count >= 4)]
  
  #Filtering drugs data to keep only those with 4 or more prescriptions of our drugs list of interest
  drugs <- drugs[drugs_count, on = .(patid, cond)]
  
  # ----------------------------------------------
  # Flag diagnoses and spells
  # ----------------------------------------------
  
  #Dropping multiple instances of the same prodcode on the same date for the same patid
  drugs <- drugs %>% distinct(patid, cond, issuedate, prodcodeid, .keep_all = TRUE)
  
  #Repeating what was done earlier to check the difference in the number of diagnoses after dropping duplicates
  drugs <- drugs[, lead3_issue_date := shift(.SD, n = 3, fill = 0, "lead"),
                   by = c("patid", "cond"), .SDcols = "issuedate"][,
                   flag := ifelse((lead3_issue_date - issuedate <= 365) & 
                   lead3_issue_date != "1970-01-01", 1, 0)]
  drugs$patid <- as.integer64(drugs$patid)
  #Merge epilepsy information
  drugs <- merge(drugs, temp_diag_obs, by = "patid", all.x = TRUE) 
  
  #Keep patients with diagnoses based on CAM rules (count only PNC167 diagnoses which happen before an epilepsy diagnosis to ensure that the prescription is not associated with epilepsy)
  drugs <- drugs[!which(cond == "PNC167" & epi_flag == 1 & issuedate >= epi_diag_date)][
           , .(patid, issuedate, cond, flag)]
  
  #Save pain separately to combine diagnoses across PNC166 and PNC167 to make a single spell of chronic pain diagnosis
  pain <- drugs[cond != "CON150", ]
  
  #Calculate variables to distinguish between different spells using Laurie's logic from script 04a
  drugs <- drugs[cond == "CON150", ][
                 ,lag_flag := shift(.SD, n = 1, fill = 0, "lag"),
                 by = "patid", .SDcols = "flag"][
                 ,last_flag := ifelse(flag != lag_flag,1,0)][
                 ,spell := cumsum(last_flag), by = "patid"][
                 ,diag_date := min(issuedate), by = c("patid", "spell")][
                 ,remission_date := max(issuedate) + 365, by = c("patid", "spell")]
  # consider replacing by:  ,last_flag := flag != lag_flag]  
                
  #Same as above but for the pain diagnoses               
  pain <- pain[order(patid, issuedate)][
                ,lag_flag := shift(.SD, n = 1, fill = 0, "lag"),
                 by = "patid", .SDcols = "flag"][
                ,last_flag := ifelse(flag != lag_flag,1,0)][
                ,spell := cumsum(last_flag), by = "patid"][
                ,diag_date := min(issuedate), by = c("patid", "spell")][
                ,remission_date := max(issuedate) + 365, by = c("patid", "spell")]
                
  drugs <- rbind(drugs, pain)
  
  #Remove spells where patient does not have a diagnosis and compress spells to one obs per spell (where diag_date=issuedate)
  drugs <- drugs[which(flag == 1 & last_flag == 1 & diag_date == issuedate)][
                 ,list_disease_real := ifelse(cond == "CON150", "Constipation", "Pain")][
                 ,.(patid, list_disease_real, diag_date, remission_date, source = "CPRD", disease_duration = 365)]
  
  drug_list[[i]] <- drugs
  print(paste0("cohort ", i, " prodcodes processed"))

}

res_conds_dt <- rbind(rbindlist(drug_list), res_conds_dt)

rm(codelist_ep, cohort, drugs, drugs_count, emis_prodcodes, gemscript_dmd, pain, prodcodes,
   temp_diag_obs, drug_list)

res_conds_dt <- unique(res_conds_dt)
gc()
