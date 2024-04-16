# Patterns of diagnosed illness for England using CPRD HES ONS data

This code has been developed to create a patient-level longitudinal dataset identifying times of onset (incidence), prevalence, remission or mortality for a list of common conditions in the population of England using patient records from primary and secondary care. 


### Project Status: [In progress]


## Project Description
### Background
Measures of the prevalence of clinical conditions often rely on survey data which can be prone to reporting biases. Using administrative data to measure multimorbidity in the population is complicated by data access and the complex nature of administrative health patient records.   

This code has been developed to create a patient-level longitudinal dataset identifying times of onset (incidence), prevalence, remission or mortality for a list of common conditions in the population of England using patient records from primary and secondary care. 

It uses commonly used secure datasets in secondary care in the English NHS (Hospital Episodes Statistics, HES) linked with patient records in primary care (CPRD Aurum version) and death register from the Office for National Statistics. These are not publicly available, but can be applied for here[Xx]. 

### Measure of multimorbidity
The list of conditions has been developed to match the 20 principal conditions used to construct the Cambridge Multimorbidity Score (CMS), a useful way of comparing multimobidity for different types and intensity of conditions.

## How does it work?

The scripts shared can be run in numerical order and act on flat text files for CPRD, HES and ONS data. The main processes are carried out in the dual numbered scripts (00, 01, etc) and the others are usually called from within these (with exception of 00.1 & 00.2). Below is a description of what each script does:

* The scripts at the start marked 00 perform project preperation by loading libraries, and variables used in the rest of the analysis. They define the clinical conditions to be identify in the panel and to set up the structure of the panel
  + `00_project_setup.R` sets the base variables and functions used throughout the project and creates patient cohorts (As the sample is too large to process at once)
  + `00.0_file_locations.R` lists locations of files and data to be set based on your system (we used s3 locations in an AWS enivronment)
  + `00.1_cprd_processing.R` processes the CPRD flat files into an arrow dataset using the aurumpipeline package (perform first and only once)
  + `00.2_linked_hes_processing.R` processes the HES flat files in the same way as above (perform first and only once - note this uses the aurumpipeline as well as the data is linked and supplied by CRPD)
  + `00.3_conditions_lookup.R` loads in our required codelists for CPRD (snomed) and HES (ICD10) and combines for later use
* Script `01_clean_files.R` processes CRPD and HES files into cohort specific blocks, and cleans them. These can be amended by a system of flags (TRUE/FALSE) for each cleaning rule
* 02 scripts are concerned with patient demographic data processing.
  + `02_patient.R` links the patient data with IMD and ethnicity results
  + `02.1_patient_ethnicity.R` uses CPRD observations and a variety of HES data to assign ethnicity to patients. This is based on the number of observations that are made and the most common is assigned. In the case of equal numbers, the most recent is assigned
* 03 scripts identifies the dates of diagnosis and remission of chosen clinical conditions for the sample of patients both in CPRD and HES (including the different datasets of HES, inpatient, outpatient and emergency admissions)
  + `03_patient_diagnosis.R`
  + `03.1_diagnosis_using_prodcodes.R`
  + `03.2_spell_grouping.R`
* Script `04_panel_creation.R` constructs a yearly panel dataset which flags whether each patient has a certain condition, and  attributes it a weight (between 0 and 1) corresponding to the proportion of the year the condition is prevalent.
* Script `05_analysis.R` The final script attaches the weight from the Cambridge Multimorbidity Score (CMS) to each condition to obtain an aggregate measure of multimorbidity.  




Note, the sample is split into cohorts of smaller size which allows both to test the code on one cohort and to speed up the analysis. The number and size of the cohort are defined in the script '00_project_setup.R' and analysts can change it to fit their sample size or system's requirements.

### Cleaning rules
XX


## Data sources

Clinical Practice Research Datalink (CPRD) Aurum is a database containing details of routinely collected primary care data from EMIS IT systems in consenting general practices across England and Northern Ireland.
Regulatory approvals to use CPRD data for this analysis were granted by the CPRD Independent Scientific Advisory Committee (ISAC protocol number 20-000096). 

Hospital Episode Statistics data have been obtained under license from the UK XXX.

More information on these data sources is available here:
* [Hospital Episode Statistics (HES)](digital.nhs.uk/data-and-information/data-tools-and-services/data-services/hospital-episode-statistics)
* [Clinical Practice Research Datalink (CPRD)]()


## Requirements
The following R packages (available on CRAN) are needed:

* arrow
* data.table
* bit64
* tidyverse
* janitor
* lubridate
* vroom
* writexl

The code relies heavily on the CPRD Aurum pipeline written by Jay Hughes, which can be downloaded [here](https://github.com/HFAnalyticsLab/aurumpipeline)

## Useful references
* On Jay Hughes' CPRD Aurum pipeline [here](https://github.com/HFAnalyticsLab/aurumpipeline)

* On Anna Head's multimorbidity codelist for CPRD [here](https://github.com/annalhead/CPRD_multimorbidity_codelists)

* On the development and validation of the Cambridge Multimorbidity Score [here](https://pubmed.ncbi.nlm.nih.gov/32015079) - Note, this has been derived using CPRD Gold and is adapted for CPRD Aurum in our code. 

* A Health Foundation report published in July 2023 uses similar analysis to understand the future patterns of illness in the population in England. This report (*Health in 2040: projected patterns of illness in England*, by T. Watt and co-authors) can be read [here](health.org.uk/publications/health-in-2040).

## Authors
* Jay Hughes - Twitter/X - [GitHub](https://github.com/Jay-ops256)
* Laurie Rachet-Jacquet - Twitter/X - GitHub
* Ann Raymond - Twitter/X - GitHub

