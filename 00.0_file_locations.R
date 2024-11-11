# ---------------------------------------------------------------------------------
# Setup RScript - to be run at the start of each session
# This script lists folder names and possible folder organisation.
# ---------------------------------------------------------------------------------


# Folders for raw data files
proj_bucket <- 'XX' # Replace with name of your project folder
raw_bucket <- 'XX' # Replace with name of your raw data folder

cprd_prefix <- 'XX' # Replace with name of your sub-folder where CPRD data are saved

rawdata.filepath.cprd <- paste0(raw_bucket, '/', cprd_prefix)

# Folders for Aurum codelists and condition lookups
res_bucket <- 'XX' # Change this to direct to folder with Aurum codelists and condition lookups. 

lookup.filepath <- paste0(res_bucket, '/Aurum_codelists/')
emis_codelist <- paste0(res_bucket, '/cprd_documentation/Lookups_CPRDAurum/')
codelist_path <- paste0(res_bucket, '/Cambridgecodelists/')

# Folders for processed data
cleandata.filepath <- "XX"