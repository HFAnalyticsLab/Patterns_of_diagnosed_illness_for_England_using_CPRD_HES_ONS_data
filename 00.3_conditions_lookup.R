# ---------------------------------------------------------------------------------
# This script creates 
# a) a codelist for CPRD and (codelist_CPRD)
# b) a look-up file for ICD-10 codes for list of selected conditions (icd_lookup)
# ---------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------
# Create codelist for CPRD (medcodes) 
# ---------------------------------------------------------------------------------

# Get list of conditions modeled by Liverpool/REAL
codelist <- read_multimorb_codelists_s3(codeloc = 'real_centre_cprd_project_codelists/Aurum_codelists/codelists/',
                                        bucket = buck)

code_cats <- s3read_using(
                        FUN = readxl::read_excel,
                        sheet = 'REAL Assign',
                        skip = 2,
                        object = paste0(lookup.filepath,
                        'Codelist groups/REAL_Centre_Liverpool_Disease.xlsx')) %>%
                        setDT() %>%
                        .[, 1:8] %>%
                        rename(list_disease_real = 'Disease (REAL)') %>%
                        mutate(Disease = gsub(' ', '_', Disease),
                               list_disease_real = ifelse(list_disease_real == 'Atrial fibrilation', 
                                                          'Atrial fibrillation', list_disease_real),
                               main_cond = ifelse(list_disease_real %in% main_cond, 1, 0))
             
## remove for tidy code
# Append rows corresponding to different CKD definitions (with only diagnosis, with test values and with procedure codes)
#temp_medcode_ckd <- code_cats[ list_disease_real == "Chronic kidney disease", ][
#  , list_disease_real := "Chronic kidney disease incl test" ]
#code_cats <- rbind(code_cats, temp_medcode_ckd)

list_main_conditions <- code_cats[which(code_cats$main_cond == 1), ] 

# Create list for aggregate conditions used for Cambridge Score and add them to the list
all_cancers = c("Breast cancer", "Bowel cancer", "Lung cancer", "Other cancer", "Prostate cancer")
cam_mental_health = c("Anxiety", "Depression")
diabetes = c("Diabetes excl Type 2", "Diabetes Type 2")
stroke_tia = c("Stroke", "TIA")
connective_tissue = c("Connective tissue disorder", "Rheumatoid arthritis")
cam_cond_group = c(all_cancers, cam_mental_health, diabetes, stroke_tia, connective_tissue)


list_cambridge <- list_main_conditions[ which(list_disease_real %in% cam_cond_group), ]
list_cambridge <- list_cambridge[ , list_disease_real := fifelse(list_disease_real %in% all_cancers, "All cancers", 
                                         fifelse(list_disease_real %in% cam_mental_health, "Depression_anxiety",
                                         fifelse(list_disease_real %in% diabetes, "Diabetes", 
                                         fifelse(list_disease_real %in% stroke_tia, "Stroke_TIA",
                                         fifelse(list_disease_real %in% connective_tissue, "Connective tissue Cam", list_disease_real)))))]

list_main_conditions <- rbind(list_main_conditions, list_cambridge) # Merge back in to have a row for condition and combination of condition

# Preparing list of main condition to merge with ICD10 files (disease_tomerge)
list_main_conditions < list_main_conditions[, disease_tomerge := Disease]
list_main_conditions$disease_tomerge = tolower(list_main_conditions$disease_tomerge) 
list_main_conditions$disease_tomerge <- gsub(' ', '_', list_main_conditions$disease_tomerge)
list_main_conditions$disease_tomerge <- gsub('-', '_', list_main_conditions$disease_tomerge)
list_main_conditions$disease_tomerge <- gsub('nos', 'not_otherwise_specified', list_main_conditions$disease_tomerge)
list_main_conditions$disease_tomerge <- gsub('other_or_not_specified', 'not_otherwise_specified', list_main_conditions$disease_tomerge)
list_main_conditions$disease_tomerge <- gsub('alcohol_misuse', 'alcohol_problems', list_main_conditions$disease_tomerge)
unique(list_main_conditions$list_disease_real) # Check all conditions are in

codelist <- merge(codelist, 
                  list_main_conditions, 
                  by.x = 'disease', 
                  by.y = 'Disease') %>%
  .[, .(disease, medcodeid, read, list_disease_real, descr)]

# Some medcodes descriptions for Diabetes excl Type 2 include Diabetes type 2.
codelist[, flag := ifelse(list_disease_real == "Diabetes excl Type 2" & 
                            str_detect(descr, "Type 2|type 2|type ii|Type ii|Type II|type II|non-insul|Non-insul|non insul|Non insul"), 1, 0)]
codelist[ , list_disease_real := ifelse(flag == 1, "Diabetes Type 2", list_disease_real)]
codelist[ , disease := ifelse(flag == 1, "Type_2_Diabetes_Mellitus", disease)][, flag := NULL]
# Note, non-insulin dependent diabetes is type 2.

# Remove 'alcoholic liver disease' from alcohol misuse (in line with Cambridge definition)
codelist <- codelist[, flag := ifelse(disease == 'Alcohol_Misuse' & 
                      str_detect(descr, "liver "), 1, 0)][, 
                      list_disease_real := case_when(flag == 1 & disease == 'Alcohol_Misuse' ~ 'Other alcohol problems',
                                            flag == 0 & (disease == 'Alcohol_Misuse' | disease != 'Alcohol_Misuse') ~ list_disease_real)][,
                                            flag := NULL]
# 3 medcodes are concerned
codelist <- codelist[, read := case_when(list_disease_real == 'Alcohol problems' ~ 365,
                                         list_disease_real !=  'Alcohol problems' ~ read)]
# Some duplicates by medcodeid-disease, because of different descriptions. Drop duplicates
codelist <- codelist %>%
  distinct(medcodeid, list_disease_real, .keep_all = TRUE)

## removing for new code
# Add diagnosis tests for CKD - test values
#codelist_ckd <- s3read_using(FUN = read.csv,
#                            object = paste0(lookup.filepath, 'ckd.csv')) %>%
#                setDT()
#codelist_ckd <- codelist_ckd[grep('GFR', term),]


#codelist_ckd <- codelist_ckd[ , medcodeid := as.integer64(medcodeid)] # Transform medcodeid format
#codelist_ckd <- codelist_ckd[ , disease := "Chronic_Kidney_Disease_incl_test"][
#  , list_disease_real := "Chronic kidney disease incl test"][
#  , read := 9999999][
#  , descr := ""
#  ][ , c("disease", "medcodeid", "read", "list_disease_real", "descr")]

# Treat ckd tests separately as need to identify parentobsids



# Modify dates of remission for asthma and recode all permanent diseases to be '99999'
codelist <- codelist[, read := ifelse(list_disease_real == "Asthma", 365, read)] 
codelist_CPRD <- codelist[, read := ifelse(list_disease_real %in% nonres_conds, 9999999, read)] 
#Going back to the old cancer diagnosis definition to get results out in time for the White Paper
  # Correct cancer remissions to be after 10 years (3652.5) without new diagnosis
  # Saved as codelist_CPRD

# Clear workspace
# rm(code_cats, codelist, list_cambridge, list_main_conditions)

# ---------------------------------------------------------------------------------
# Create a lookup file for ICD10 codes (used in HES and for cause of death)
# ---------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------
# STEP 1. Clean ICD10 files 
# ---------------------------------------------------------------------------------
# Combine all ICD-10 files
icd_file_list <- get_bucket(bucket = buck,
                             prefix = 'ICD10/Jan_22/ICD/') %>% rbindlist()

icd_file_list <- icd_file_list[grepl('.csv', icd_file_list$Key), ] ## keep only with .csv

icd_files <- purrr::map(icd_file_list$Key,
                        ~ aws.s3::s3read_using(FUN = vroom::vroom, 
                                               object = .x, 
                                               bucket = buck)) %>%
              bind_rows() %>%
              setDT()

# Add a column to merge on
icd_files[, disease_tomerge := Disease]

# Replace all lower case and add underscore between words
icd_files$disease_tomerge = tolower(icd_files$disease_tomerge) 
icd_files$disease_tomerge <- gsub(' ', '_', icd_files$disease_tomerge)
icd_files$disease_tomerge <- gsub('-', '_', icd_files$disease_tomerge)

# Correct some ICD-10 terms to allow merge
icd_files$disease_tomerge <- gsub('nos', 'not_otherwise_specified', icd_files$disease_tomerge)
icd_files$disease_tomerge <- gsub('other_or_not_specified', 'not_otherwise_specified', icd_files$disease_tomerge)
icd_files$disease_tomerge[icd_files$disease_tomerge == 'end_stage_renal_disease'] <- 'chronic_kidney_disease'

icd_files <- icd_files[ , disease_tomerge := ifelse(disease_tomerge == "chronic_kidney_disease" & !is.na(OPCS4code), 
                                                    "chronic_kidney_disease_incl_test_and_proc", disease_tomerge)]

# Recode procedure codes to be for CKD with procedures LOOK HERE
# Get list of OPCS code for chronic kidney disease
codelist_ckd_opcs <- s3read_using(FUN = read.csv,
                                  object = paste0(lookup.filepath, 'Medical/allOPCScodesTMP.csv')) %>%
  setDT()
codelist_ckd_opcs <- codelist_ckd_opcs[ which(Disease == "Chronic Kidney Disease")][
  , opcscodesmod := str_remove(OPCS4code, "\\.") # removing the .
]


# Add the ICD10 codes for alternative definitions of CKD (CKD with test)
temp_icd_ckd <- icd_files[ which(disease_tomerge == 'chronic_kidney_disease'),][
  , disease_tomerge := ifelse(disease_tomerge == 'chronic_kidney_disease', 
                              'chronic_kidney_disease_incl_test', disease_tomerge)]

# Append list of ICD10 for definition of CKD
icd_files <- rbind(icd_files, temp_icd_ckd)
rm(temp_icd_ckd)

# Add the ICD10 codes for alternative definitions of CKD (this time, CKD with test and procedures)
temp_icd_ckd <- icd_files[ which(disease_tomerge == 'chronic_kidney_disease'),][
        , disease_tomerge := ifelse(disease_tomerge == 'chronic_kidney_disease', 
                                    'chronic_kidney_disease_incl_test_and_proc', disease_tomerge)]

# Remove alcoholic liver disease from alcohol misuse definition (in line with Cambridge)
icd_files <- icd_files[, disease_tomerge := ifelse(Category == 'Diagnosis of Alcohol Problems' & 
                         str_detect(ICD10codeDescr, "liver "), 'alcohol_problems_other', disease_tomerge)]

# Append list of ICD10 for definition of CKD
icd_files <- rbind(icd_files, temp_icd_ckd)



# ------------------------------------------------------------------------------
# STEP 2. Merge ICD10 codes and medcodes for list of selected conditions
# ------------------------------------------------------------------------------

# Correct missing diseases in list (for diabetes type 2, lung and bowel)
icd_files <- icd_files[ , disease_tomerge := ifelse(str_sub(ICD10code, 1, 3) == "E11", "type_2_diabetes_mellitus", disease_tomerge )]
#Note, diabetes type 2 is non-insulin dependent diabetes.
icd_files <- icd_files[ , disease_tomerge := ifelse(disease_tomerge == "diabetes" & str_sub(ICD10code, 1, 3) != "E11", "type_1_diabetes_mellitus", disease_tomerge )]
icd_files <- icd_files[ , disease_tomerge := ifelse(str_sub(ICD10code, 1, 2) == "M3" | str_sub(ICD10code, 1, 4) == "L94.9"
                                                      , "connective_tissue_disorder", disease_tomerge )]
# connective tissue disorders are M30-M36 (systemic disorders) and L94.9 (localised tissue disorder).
# No ICD10 codes for M37-39.
icd_files <- icd_files[ , disease_tomerge := ifelse(disease_tomerge == "primary_malignancy_lung_and_trachea", "primary_malignancy_lung", disease_tomerge )]
icd_files <- icd_files[ , disease_tomerge := ifelse(disease_tomerge == "primary_malignancy_colorectal_and_anus", "primary_malignancy_bowel", disease_tomerge )]

icd_lookup <- merge(icd_files, 
                  list_main_conditions, 
                  by = 'disease_tomerge')
unique(icd_lookup$list_disease_real)

# Code with 3 letters may also include more precise diagnosis, such as E11 (E11.0 to E11.9)
icd_lookup <- icd_lookup[ , temp := ifelse(str_length(ICD10code) == 3, 1, 0)]

dt <- data.table(code = as.character(seq(0, 9, by = 1)))
dt[ , temp := 1] # Create temporary data.table 
dt <- rbind(dt, list(NA, 0)) # Add row for temp = 0 for later merge
dt <- rbind(dt, list(NA, 1)) # Add row to keep 3 digit ICD10 codes in lookup

icd_lookup <- icd_lookup[dt, on = 'temp', allow.cartesian = TRUE][
                            order(disease_tomerge, ICD10code, code)][
                            , ICD10code := ifelse(temp == 1 & !is.na(code), 
                                                  paste(ICD10code, code, sep = "."), 
                                                  ICD10code) # Create combinations of ICD10 codes including F10, F10.0,.. - F10.9
                            ][ , c('ICD10code', 'list_disease_real')]

icd_lookup <- unique(icd_lookup)
# Note: XX.0 is a disease with hypertension (eg. I11.0 hypertensive heart failure). 

# Add disease duration
temp <- unique(codelist_CPRD[, c('list_disease_real', 'read')])
icd_lookup <- icd_lookup[temp, on = 'list_disease_real']
setnames(icd_lookup, 'read', 'disease_duration')                  
unique(icd_lookup$list_disease_real)

# No authorisation to write it here? TO check and saved
#if (write_icd_lookup){  # Just a one-off 

#  s3write_using(FUN = write.xlsx,
#               x = icd_lookup,
#               object = paste0(write_loc, 'icd10_lookup.xlsx'),
#               col_names = TRUE)

#}


rm(temp, code_cats_REAL, dt, list_main_conditions, icd_files, 
   list_cambridge, temp_icd_ckd, temp_medcode_ckd, codelist)

