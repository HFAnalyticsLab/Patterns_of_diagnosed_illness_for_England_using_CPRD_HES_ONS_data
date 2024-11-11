# ---------------------------------------------------------------------------------
# Setup Rscript - to be run at the start of each session
# This script loads all relevant packages
# Then creates variables and functions for the rest of the processing
# ---------------------------------------------------------------------------------

# Loading libraries
library(aws.s3)
library(aurumpipeline)
library(arrow)
library(data.table)
library(bit64)
library(tidyverse)
library(janitor)
library(lubridate)
library(vroom)
library(writexl)

#-------------------------------------------------------------------------------
# Project's file locations
#-------------------------------------------------------------------------------

source('00.0_file_locations.R')

#-------------------------------------------------------------------------------
# List of conditions modelled
#-------------------------------------------------------------------------------
main_cond <- c("Alcohol problems", "All cancers", "Anxiety", "Asthma", "Atrial fibrillation", "Breast cancer", 
               "Lung cancer", "Prostate cancer", "Bowel cancer", "Other cancer", "Chronic kidney disease",
               "Chronic liver disease", "Connective tissue Cam", "Connective tissue disorder", "COPD", 
               "Coronary heart disease", "Dementia", "Depression", "Depression_anxiety", "Diabetes",
               "Diabetes excl Type 2", "Diabetes Type 2", "Epilepsy", "Hearing loss", "Heart failure",
               "Hypertension", "IBS", "Psychosis", "Rheumatoid arthritis", "Stroke", "TIA", "Stroke_TIA", 
               "Obesity",  "Constipation", "Pain")

# Define list of conditions with remission period
res_conds <- c("Alcohol problems", "Anxiety", "Asthma", "Depression", "Depression_anxiety",
               "All cancers",
               "Breast cancer", "Lung cancer",
               "Prostate cancer", "Bowel cancer", 
               "Other cancer", 
               "Constipation", "Pain") 
#Note, pain and constipation are also included

list_resolve_non_cancer <- c("Alcohol problems", "Anxiety", "Asthma", "Depression", "Depression_anxiety")
list_cancers <- c("Breast cancer", "Lung cancer", "Prostate cancer", "Bowel cancer", "Other cancer", "All cancers") 
#treating all cancers the same as individual cancers for now - to change based on Chris's suggestion
list_ind_cancers <- c("Breast cancer", "Lung cancer", "Prostate cancer", "Bowel cancer", "Other cancer") 

# Define list of conditions which we code as permanent for validation with liverpool
hist_conds <- c("Depression_anxiety", "Asthma", "Hypertension", "Psychosis")

# Defining set of permanent conditions
nonres_conds <- main_cond[!main_cond %in% res_conds] 


#-------------------------------------------------------------------------------
# List of duplicate practice IDs
#-------------------------------------------------------------------------------
prac_dup <- c(20023, 20084, 20094, 20095, 20107, 20123, 20155, 20175, 
              20182, 20296, 20303, 20331, 20336, 20428, 20447, 20472, 
              20545, 20775, 20776, 20785, 20903, 20988, 21097, 21119, 
              21236, 21326, 21339, 21349, 21406)

#-------------------------------------------------------------------------------
# Variables and vectors used in panel creation 
#-------------------------------------------------------------------------------

# Study dates
startdate <- as.Date('2007-04-01') # start of the study period
enddate <- as.Date('2021-03-31') # end of study period

# Select our "year start" for panel data
start_month <- "04" # Financial year (April-March)
start_day <- "01" 

# Create a set of years as a vector, with start and end date for each year
years <- data.table(fyear = as.character(seq(2007, 2020, by = 1)))



years$year_start <- as.Date(
  paste(years$fyear, paste0("-",start_month,"-",start_day), sep = ""), 
  "%Y-%m-%d")

years$year_end <- years$year_start %m+% years(1) %m-% days(1) # %m+% function to iterate over years/month/days

# Create vectors with disease names to apply functions to
non_res_disease <- tolower(nonres_conds) %>%
                   gsub(' ', '_', .)
non_res_prop <- paste0("prop_", non_res_disease)
non_res_inc <- paste0("inc_", non_res_disease)

res_disease <- tolower(res_conds) %>%
               gsub(' ', '_', .)
res_inc <- paste0("inc_", res_disease)
temp_res_prop <- paste0("temp_prop_", res_disease)
inc_col <- append(non_res_inc, res_inc)

#-------------------------------------------------------------------------------
# Variables to create incidence table by patient socio-demographics 
#-------------------------------------------------------------------------------

# Create age and age categories
agebreaks <- c(0, 20, 30, 50, 60, 70, 80, 90, 100, Inf)
agelabels <- c("sub20", "20-29", "30-49", "50-59", "60-69","70-79", "80-89", "90-99", "100plus")

# Create categories for number of comorbidities (comorb_cat)
comorbbreaks <- c(0, 1, 2, 3, 4, Inf)
comorblabels <- c("0", "1", "2", "3", "4+")

# Set list of patient variables 
ident_vars <- c("patid", "gender", "yob", "ethnicity", "regstartdate", "censor_date", 
                "death_date", "region", "imd2015_decile")

# Here we set the variables that we want to run this by. 
by_vars <- c('gender', 'region','imd2015_decile', 
             'agecat', 'ethnicity', 'comorb_cat') 

# Create vectors of condition names for panel creation
main_cond_inc_tab <- tolower(main_cond) 
main_cond_inc_tab <- gsub(' ', '_', main_cond_inc_tab)
condition_prop <- paste0("prop_", main_cond_inc_tab)
condition_prop_res <- paste0("prop_", res_disease)
condition_prop_nonres <- paste0("prop_", non_res_disease)
panel_res <- paste0("panel_", non_res_disease)

hist_disease <- c("hist_asthma", "hist_depression_anxiety", 
                  "hist_hypertension", "hist_psychosis")
hist_prop <- paste0("prop_", hist_disease)
hist_inc <- paste0("inc_", hist_disease)

#-------------------------------------------------------------------------------
# Function definitions
#-------------------------------------------------------------------------------

## Calculating weights for diagnoses based on length of time it has been diagnosed in the year
# calculates proportion of year diagnosis exists for
calc_diag_weight <- function(diagdate, regstartdate, year_start, year_end, death_date, censor_date, death){

  x <- fifelse(is.na(diagdate) | (!is.na(diagdate) & diagdate > year_end) | (!is.na(death_date) & death_date > censor_date & death == 1), 0, 
        # 0 if diagnosis is null or after end of year, or year is after death (weight should be 0). Death is a flag for death occurring during the year.
        # otherwise calculate length of time                         
        fifelse((!is.na(diagdate) & diagdate <= year_end & diagdate <= censor_date), # diagnosis not null and date less than censor date and year end
        # Then:  
          (as.numeric(pmin(year_end, censor_date) - #earliest of year end or censor date
          pmax(diagdate, year_start, regstartdate))+1) / #minus latest of diag/year start/reg start
          (as.numeric(year_end - year_start)+1), # divided by length of year (accounts for leap years)
            # otherwise                              
            fifelse((!is.na(diagdate) & diagdate <= year_end & censor_date < diagdate), # date not null and before year end and after censor date
            0, as.numeric(NA))  # then 0 otherwise missing (need as.numeric(NA) to match type for fifelse argument)
                )
              )
  
  return(x)
  
}

#Function created by Will Parry in his script calculating the Cambridge score
#This creates a single dataframe by combining the read codes for all the diseases included in the Cambridge score
## updated for s3
readcam <- function(file_pattern, coltypes, buck = add_me) {
  
  files <- get_bucket(bucket = buck, prefix = gsub(paste0('s3://', buck, '/'), '', codelist_path)) %>% rbindlist()
  files <- files[grep(file_pattern, files$Key), ]$Key
  nms <- str_extract(files, '(?<=CPRDCAM_)......') #pull out names of codes
  codes <- purrr::map(files,
             ~ aws.s3::s3read_using(FUN = vroom::vroom,
                                    col_types = coltypes,
                                    object = .x, bucket = buck))
  
  names(codes) <- nms
  rbindlist(codes, idcol = 'cond')
  
}


#-------------------------------------------------------------------------------
# Create cohorts of patients to run codes on. Set of cleaning rules.
#-------------------------------------------------------------------------------

# Using cohorts to speed up the processing time
nb_cohorts <- 100

# Create chunk numbers for chunk-wise processing later on
if(create_cohorts){
  
  pat <- opendt(paste0(cleandata.filepath, 'cprd/Data/patient/'),
                cols_in = c('patid', 'pracid', 'yob', 'gender',
                            'regstartdate', 'cprd_ddate', 'regenddate'))
  pat$patid <- as.integer64(pat$patid)
  
  # First removing duplicate practice IDS identified by CPRD
  if(remove_dup){
  
    pat <- pat[!(pracid %in% prac_dup), ]
  
  }

  if(remove_hes_dup){
    
    hespat <- opendt(paste0(cleandata.filepath, 'hes/', grep('hes_patient', hes_filenames$name, value = TRUE)),
                     cols_in = c('patid', 'gen_hesid'))
    
    pat <- merge(pat, hespat, all.x = TRUE)
    
    pat_nolink <- pat[is.na(gen_hesid), ]
    pat <- pat[!is.na(gen_hesid), ]
    
    setkey(pat, "gen_hesid", "regstartdate", "regenddate")
    pat <- pat[pat[, max(.I), keyby = "gen_hesid"]$V1] 
    #pat <- pat[, .SD[which.max(regstartdate)], by = gen_hesid]
    pat <- rbind(pat_nolink[, !'gen_hesid'], pat[, !'gen_hesid'])
    
    rm(hespat, pat_nolink)
  }

  ons <- read_delim_arrow(
    paste0(
      rawdata.filepath.hes, 
      '/e_aurum_death_patient', 
      hes_filecode), delim = '\t')
  
  colnames(ons) <- gsub("^e_", "", colnames(ons))  # Note: need to specify start of string with "^"
  
  pat <- merge(pat, ons[, c('patid', 'dod', 'match_rank')],
               by = 'patid',
               all.x = TRUE)
  
  pat[ , death_date := as.Date(ifelse(is.na(dod) &
                                         !is.na(cprd_ddate) &
                                         cprd_ddate > "2019-10-31", 
                                       cprd_ddate, dod))]
  
  prac <- opendt(paste0(cleandata.filepath, 'cprd/Data/practice')) %>%
    .[, region := ifelse((pracid == 20538 | pracid == 21258), 2, region)]
  pat <- merge(pat, prac, by = 'pracid', all.x = TRUE)
  
  # Registration end on death date if end of registration is not recorded
  pat[, regenddate := case_when((is.na(regenddate) & !is.na(death_date)) ~ death_date, 
                                TRUE ~ regenddate)]
  
  pat[ , censor_date := pmin(regenddate, death_date, lcd, enddate, na.rm = TRUE)]
  
  pat <- pat[censor_date > regstartdate, ]
  rm(prac, ons)
  
  breaks <- seq.int(0, 5000000, 50000)
  chunk_labels <- seq.int(1, nb_cohorts)
  
  # Create a variable with chunk number
  pat[, chunk := 1:nrow(pat)][
      , chunk := cut(chunk, breaks, chunk_labels)][
      , chunk := as.numeric(chunk)]
  
  ## create cohorts from filtered patients
  for (i in 1:length(unique(pat$chunk))) {
    
    cohort = pat[chunk == i, ][, chunk := NULL]
    
    write_parquet(cohort, 
                  paste0(cleandata.filepath, 'cohorts/' , i, '/data.parquet'))
      
  }
}

# update cohort numbers after creating
nb_cohorts <- get_bucket(proj_bucket, prefix = 'Data/cohorts') %>% 
  rbindlist() %>%
  nrow()

# After removing the duplicates, 4775883 patients
  
rm(pat, cohort, breaks, chunk_labels)


#-------------------------------------------------------------------------------
# Get codelists for our list of conditions
#-------------------------------------------------------------------------------

## Adapated pipeline function for s3
read_multimorb_codelists_s3 <- function(codeloc, bucket, file_pattern = '.csv'){
  
  ## declare variables
  medcodeid <- disease <- category <- read <- NULL
  
  ## s3 method get all files in bucket and codeloc path
  files <- get_bucket(bucket,
                      prefix = codeloc,
                      max = Inf) %>% rbindlist() #look for code lists
  
  ## remove subfolders and keep only .csv or .xls or .xlsx
  ## any further '/' will indicate a subfolder so remove them
  keep_me <- stringr::str_locate_all(pattern = '/', files$Key) %>% mapply(FUN = max) <= nchar(codeloc)
  
  files <- files[keep_me, ] %>% unique()
  
  fileext <- substr(files$Key, nchar(files$Key) - 3, nchar(files$Key)) == file_pattern
  
  files_csv <- files[fileext, ]
  
  codes <- purrr::map(files_csv$Key,
                      ~ aws.s3::s3read_using(FUN = vroom::vroom, object = .x, bucket = bucket))
  
  ## get only where there is a medcodeid and format correctly
  data <- data.table::rbindlist(codes)
  data.table::setDT(data)
  
  data.table::setnames(data, tolower(names(data))) ## lower case column names
  
  if('medcodeid' %in% names(data)){
    
    data <- data[!is.na(medcodeid), ]
    data$medcodeid <- bit64::as.integer64(data$medcodeid)
    
  }
  
  if('prodcodeid' %in% names(data)){
    
    data <- data[!is.na(prodcodeid), ]
    data$prodcodeid <- bit64::as.integer64(data$prodcodeid)
    
  }
  
  ### how far to look back for diagnoses, add to this list as we develop this method
  if('system' %in% names(data)){
    
    data$read <- fifelse(data$system == 'Cancers', 5*365.25,
                         fifelse(data$system == 'Mental Health Disorders', 365, 9999999))
    
  }
  
  if('disease' %in% names(data)){
    
    data$disease <- gsub(' ', '_', data$disease)
    
  }
  
  return(data)
  
}

source('00.3_conditions_lookup.R')
