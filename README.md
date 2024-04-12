# Patterns_of_diagnosed_illness_for_England_using_CPRD_HES_ONS_data

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

The panel can be created by running the master script ('_master_script.R') which sources all scripts from 5 different stages: 

1. A few scripts at the start allow to define the clinical conditions to be identify in the panel and to set up the structure of the panel.
'00.0_file_locations.R' lists locations of folders to be set based on your system.
'00.1_cprd_processing.R' Xx
'00.2_linked_hes_processing.R' XX

2. Script '01_clean_files.R' saves all CRPD and HES files as parquet files and clean the raw data files based on cleaning rules defined in the master script. These can be amended by a system of flags (TRUE/FALSE) for each cleaning rule.

3. Script '02_patient.R' collates the patient socio-economic characteristics from CPRD and linked HES datasets.

4. Script '03_patient_diagnosis.R' identifies the dates of diagnosis and remission of chosen clinical conditions for the sample of patients both in CPRD and HES (including the different datasets of HES, inpatient, outpatient and emergency admissions).

5. Script '04_panel_creation.R' constructs a yearly panel dataset which flags whether each patient has a certain condition, and  attributes it a weight (between 0 and 1) corresponding to the proportion of the year the condition is prevalent.

6. Script '05_CMS xx -- The final script attaches the weight from the Cambridge Multimorbidity Score (CMS) to each condition to obtain an aggregate measure of multimorbidity.  




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

