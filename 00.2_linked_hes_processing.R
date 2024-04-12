
#### Linked data processing
## Note that all CPRD data has been processed by the aurum pipeline and filtered to our sample already using
## the full CPRD extract

files <- get_bucket(raw_bucket,
                    prefix = hes_prefix) %>% 
          rbindlist() %>%
          filter(grepl('.txt', Key)) %>%
          filter(grepl('hes', Key)) %>%
          select(Key)

## get name from files
files$name <- sub(paste0(hes_prefix, '/'), '', sub(hes_filecode, '', files$Key))

for (i in 1:nrow(files)){
  
  data <- read_delim_arrow(
              file.path('s3:/', raw_bucket, files$Key[i]), 
              delim = '\t') %>%
          setDT()
  
  ## remove the all NA columns
  data <- data[, which(unlist(lapply(data, function(x) !all(is.na(x))))), with = F]
  
  # write the parquet data file
  write_parquet(data, 
                paste0(cleandata.filepath, 'hes/', files$name[i], '/data.parquet'))

  print(i)
  
}

get_bucket(proj_bucket, prefix = paste0('Data/hes/')) %>%
  rbindlist() %>% select(Key) %>% filter(str_detect(Key, 'data.parquet')) -> file_list_hes


for (i in 1:nrow(file_list_hes)) {
  
  temp <- opendt(paste0('s3://', proj_bucket, '/', file_list_hes[i]))
  colnames(temp) <- gsub("^e_", "", colnames(temp))  # Note: need to specify start of string with "^"
  write_parquet(temp, 
                paste0(paste0('s3://', proj_bucket, '/', file_list_hes[i])))
  
  print(paste0("loop", i, "done"))
  
}
