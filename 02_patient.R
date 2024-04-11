# ---------------------------------------------------------------------------------
# 02. Patient file
# Create dataset with patient IDs and socio-demographic characteristics
# ---------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# Load patient CPRD data
# ------------------------------------------------------------------------------
patient <- opendt(paste0(cleandata.filepath, 'cohorts'))
# ------------------------------------------------------------------------------
# Adding patient IMD
# ------------------------------------------------------------------------------

# Loading patient and IMD file
imd <- read_delim_arrow(
          paste0(
            rawdata.filepath.hes, 
            '/e_aurum_patient_imd2015', 
            gsub('_dm', '', hes_filecode)), delim = '\t') %>%
            rename(patid = e_patid,
                   imd2015_decile = imd2015_10)

patient <- merge(patient, 
                 imd[, c('patid', 'imd2015_decile')], 
                 by = 'patid')
rm(imd)

# ------------------------------------------------------------------------------
# Add patient ethnicity from CPRD and HES
# ------------------------------------------------------------------------------
source('02.1_patient_ethnicity.R')

# Adding ethnicity to the final patient files
patient <- merge(patient, eth,
                 by = 'patid',
                 all.x = TRUE) %>%## note was getting duplicates here so changed to all.x
                 setnames(., "HES_ethnic_group", "ethnicity")


# Remove patients who are active for less than one year if over 1 year old ## 
# Cleaning rule ID#33
#if (registr_rule){
#  
#  patient_clean[ , flag := fifelse((censor_date - regstartdate <= 365), 1, 0) ] # Flagging those with <1year registration
#  patient_clean[ , flag := fifelse((!is.na(death_date) & (year(death_date) == yob | (year(death_date) - yob == 1))), 0, flag) ] 
  # Keep patients who are under 1 and died for the calculation of mortality rates.
  # Note, we don't have the exact date of birth. So we use yob = year(death) or yob is the year before year of death
 
#  patient_clean <- patient_clean[flag == 0, ][, flag := NULL]
  
#}

# Save patient file
write_parquet(patient, paste0(cleandata.filepath, 'interim/patient/patient_clean.parquet'))

rm(patient, patient_ethnicity_final)
gc()
