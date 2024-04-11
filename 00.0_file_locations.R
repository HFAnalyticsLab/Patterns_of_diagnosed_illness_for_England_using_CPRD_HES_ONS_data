# ---------------------------------------------------------------------------------
# Setup RScript - to be run at the start of each session
# This script lists folder names and possible folder organisation.
# ---------------------------------------------------------------------------------


# Folders for raw data files
proj_bucket <- 'XX' # Replace with name of your project folder
raw_bucket <- 'XX' # Replace with name of your raw data folder

cprd_prefix <- 'XX' # Replace with name of your sub-folder where CPRD data are saved
hes_prefix <- 'XX' #  Replace with name of your sub-folder where HES data are saved
hes_filecode <- 'XX' #  Replace with name of HES txt file

rawdata.filepath.hes <- paste0(raw_bucket, '/', hes_prefix)
rawdata.filepath.cprd <- paste0(raw_bucket, '/', cprd_prefix)

hes_filenames <- get_bucket(raw_bucket, prefix = hes_prefix) %>% 
  rbindlist() %>%
  filter(grepl('.txt', Key)) %>%
  filter(grepl('hes', Key)) %>%
  select(Key)

## get name from files
hes_filenames$name <- sub(paste0(hes_prefix, '/'), '', sub(hes_filecode, '', hes_filenames$Key))

# Folders for Aurum codelists and condition lookups
res_bucket <- 'XX' # Change this to direct to folder with Aurum codelists and condition lookups. 

lookup.filepath <- paste0(res_bucket, '/Aurum_codelists/')
emis_codelist <- paste0(res_bucket, '/cprd_documentation/Lookups_CPRDAurum/')
codelist_path <- paste0(res_bucket, '/Cambridgecodelists/')

# Folders for processed data
cleandata.filepath <- "XX"