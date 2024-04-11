# ---------------------------------------------------------------------------------
# 01. Patient file
# Check ethnicity in HES and CPRD
# ---------------------------------------------------------------------------------


# -----------------------------------------------------------
# Open the ethnicity look-up files
# -----------------------------------------------------------

# Lookup file for CPRD
ethnicity_lookup <- s3read_using(FUN = read.csv,
                                 object = paste0(lookup.filepath,
                                    'Ethnicity/Ethnicity_snomed_grouping_TW_30032021_.csv')) %>%
  setDT() %>%
  .[, medcodeid := as.integer64(medcodeid)]

# Lookup file for HES ethnicity
hes_lookup <- s3read_using(FUN = read.csv,
                           object = paste0(lookup.filepath, "Ethnicity/HES_ethnicity_lookup.csv"))

# -----------------------------------------------------------
# Import all HES ethnicity files
# -----------------------------------------------------------
print('Processing HES ethnicity files')
# Note, here we use episodes file to have ethnicity recorded with a date
# CPRD provided 'gen_ethnicity' which corresponds to the most commonly reported 
# ethnicity across HES APC, OP and AE datasets. But it does not include the date.

# Load the CPRD patient cohort - to filter on
#patient_list = opendt(paste0(data.filepath, 'patient'),
#                      cols_in = 'patid')
#setnames(patient_list, 'patid', 'e_patid') # Correct to have all HES dataset column names without the 'e_'

# To filter by patient cohort 
# HES Admitted patient care
hes_epi = opendt(paste0(cleandata.filepath, 'hes/e_aurum_hes_episodes'),
                     cols_in = c('patid', 'admidate', 'ethnos'))
setnames(hes_epi, 'admidate', 'obsdate')

# HES AE attendances
hes_att = opendt(paste0(cleandata.filepath, 'hes/e_aurum_hesae_attendance'),
                    cols_in = c('patid', 'arrivaldate', 'ethnos'))  
setnames(hes_att, 'arrivaldate', 'obsdate')

# HES outpatient appointments
hes_op = opendt(paste0(cleandata.filepath, 'hes/e_aurum_hesop_appointment'),
                    cols_in = c('patid', 'apptdate', 'ethnos')) 
setnames(hes_op, 'apptdate', 'obsdate')

# Merge all HES files and retain most recent ethnicity
all_hes_ethnicity <- rbind(hes_epi, hes_att, hes_op)

# Merge with HES lookup
setnames(all_hes_ethnicity, 'ethnos', 'HES_ethnic_group')
all_hes_ethnicity <- merge(all_hes_ethnicity, hes_lookup, by = "HES_ethnic_group")
#all_hes_ethnicity <- all_hes_ethnicity[ order(patid, obsdate)]
all_hes_ethnicity <- all_hes_ethnicity[, source := "HES"]

# Remove if 'Unknown' as doesn't contribute any information
all_hes_ethnicity <- all_hes_ethnicity[HES_ethnic_group != "Unknown", ]
all_hes_ethnicity$obsdate <- as.Date(all_hes_ethnicity$obsdate)

# Remove duplicates
all_hes_ethnicity <- unique(all_hes_ethnicity)
rm(hes_epi, hes_att, hes_op)
gc()

# ------------------------------------------------------------------------------
# Import all CPRD observation files for ethnicity
# ------------------------------------------------------------------------------
raw_obs <- list() # set up list

for(i in 1:nb_cohorts){
  
  raw_obs[[i]] <- opendt(paste0(cleandata.filepath,
                                'obs/', i, '/data.parquet'),
                  cols_in = c('patid', 'medcodeid', 'obsdate', 'obstypeid'))[
                  #obstypeid != 4 & obsdate < as.Date(enddate), ][
                  obstypeid != 4, .(patid, obsdate, medcodeid)] %>%
                  merge(ethnicity_lookup, by = 'medcodeid') %>%
                  .[, .(patid, obsdate, Term, Group1, Group2, Group3)]
  
  print(paste0('Processed cohort ', i, ' of CPRD ethnicity observations'))

}

cprd_ethnicity <- rbindlist(raw_obs)

rm(raw_obs)

#Remove duplicate instances of patid with same Group1 ethnicities even though multiple medcodeids since Group1 is the lowest level of ethnicity breakdown we'll go down to
#For patids with multiple Group1 ethnicity entries, keep all entries so as to make decision on final ethnicity later
cprd_ethnicity <- cprd_ethnicity[order(patid, obsdate)]
cprd_ethnicity[, source := "CPRD"]


if(!is.Date(cprd_ethnicity$obsdate)){
  
  cprd_ethnicity[, obsdate := as.Date(obsdate, origin = '1970-01-01')]
  
}

# Save dataset
write_parquet(cprd_ethnicity, paste0(cleandata.filepath, 'interim/ethnicity/CPRD_patient_ethnicity/data.parquet'))

#------------------------------------------------------------------------------------------------------------------
# Combine HES and CPRD ethnicity, and keep most common ethnicity recorded across both sources
#------------------------------------------------------------------------------------------------------------------

cprd_ethnicity_tomerge <- cprd_ethnicity[, .(patid, obsdate, Group2, source)] %>% unique()
setnames(cprd_ethnicity_tomerge, 'Group2', 'HES_category')
cprd_ethnicity_tomerge <- merge(cprd_ethnicity_tomerge, hes_lookup, by = 'HES_category')

# Merge CPRD and HES ethnicity files 
# Note, we merge with HES ethnicity, ie. the most aggregate
ethnicity_cprd_hes <- rbind(cprd_ethnicity_tomerge, all_hes_ethnicity)


rm(all_hes_ethnicity, cprd_ethnicity, cprd_ethnicity_tomerge, #ethnicity_cprd_hes_temp, 
   ethnicity_lookup,
   hes_lookup)
gc()

# Save dataset
write_parquet(ethnicity_cprd_hes, paste0(cleandata.filepath, 'interim/ethnicity/CPRD_HES_ethnicity/data.parquet'))

# ---------------------------------------------------------------------------------
# Patient ethnicity: HES and CPRD 
# ---------------------------------------------------------------------------------
fieldnames <- c('patid', 'HES_ethnic_group')

a <- ethnicity_cprd_hes[, .N, by = fieldnames][ #count codings by patient and ethnicity
      , max := max(N), by = .(patid)][ #identify max count by patid
      max == N][ #restrict to those ethnicities that are equal to their max count
        , count := .N, by = .(patid)][ #count number of remaining records per patient (to identify ties)
        count == 1, fieldnames, with = F] #keep only patients with no ties, and keep only patid and ethnic5

fieldnamesdate <- c(fieldnames, 'obsdate')

## this gets all ethnicity observations that have the same number of different ethnicities recorded
b <- ethnicity_cprd_hes[, .(obsdate = max(obsdate), N = .N), by = fieldnames][ #count codings by patient and ethnicity
      , max := max(N), by = .(patid)][ #identify max count by patid
      max == N][ #restrict to those ethnicities that are equal to their max count
        , count := .N, by = .(patid)][ #count number of remaining records per patient (to identify ties)
        count > 1, fieldnamesdate, with = F][ #keep only patients with ties, keep observation date as well
          , .SD[which.max(obsdate)], by = patid][ ## keep only the most recent obs
            ## NOTE there may be times there are ties with the observation date
            ## will need to address this if it happens - although will be a very small number
            , fieldnames, with = F] ## remove date col

eth <- rbind(a, b)

rm(a, b, ethnicity_cprd_hes)
gc()
