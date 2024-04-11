## CPRD data processing

for (i in tolower(tabledata$table_name)){
  
  aurum_pipeline(i,
                 saveloc = paste0(cleandata.filepath, 'cprd'),
                 dataloc = rawdata.filepath.cprd)
  
}

#-------------------------------------------------------------------------------
# Remove "e_" prefix in columns' names (eg.'e_patid' to 'patid') to use aurum pipeline functions
#-------------------------------------------------------------------------------

# Arrange loops for files with more than 1 folder
for (j in 1:length(tabledata$table_name)) {
  
  # Create the list of all files
  get_bucket(proj_bucket, prefix = paste0('Data/cprd/Data/', tolower(tabledata$table_name[j]))) %>%
    rbindlist() %>% select(Key) %>% filter(str_detect(Key, 'data.parquet')) -> file_list
  
  for (i in 1:nrow(file_list)) {
    
    temp <- opendt(paste0('s3://', proj_bucket, '/', file_list[i])) # Open one file (e.g. problem/1/) at the time
    colnames(temp) <- gsub("^e_", "", colnames(temp))  # Note: need to specify start of string with "^"
    write_parquet(temp, 
                  paste0('s3://', proj_bucket, '/', file_list[i]))
    
    print(paste0(tabledata$table_name[j], '-', i, '-done'))
    
  }
  
}
