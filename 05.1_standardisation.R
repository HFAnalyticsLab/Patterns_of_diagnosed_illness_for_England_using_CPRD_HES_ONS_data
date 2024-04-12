# ---------------------------------------------------------------------------------
# Standardisation
# Create template to do the standardisation by age group (for 30 and over), sex and deprivation decile
# ---------------------------------------------------------------------------------



  # Defining age groups and associated labels
  df <- data.table(temp = c("0", "1", as.character(seq(5, 99, 5))),
                   temp2 = c("1", as.character(seq(4, 99, 5))))

  df[, agegrp_lbl := paste0(temp, "-", temp2)][
    , c('temp', 'temp2') := NULL
  ]
  agelabels <- df$agegrp_lbl # passing it to a vector
  rm(df)

  # European standard population (ESP) in 2013 weights:
  tt <- data.table(agecat = agelabels,
                   wt_esp = c(1000, 4000, 5500, 5500, 5500, 6000, 6000, 6500,
                              7000, 7000, 7000, 7000, 6500, 6000, 5500, 5000, 
                              4000, 2500, 1500, 800, 200))
                   # weights should add to 100,000
  
  ESP <- CJ(agecat = agelabels, # cross joining (CJ()) different vectors
            gender = c(2, 1),
            imd_deciles = c("1 most deprived", as.character(2:9), "10 least deprived"),
            fyear = as.character(seq(2008, 2020, 1))
            )
  
  ESP <- ESP[tt, on = 'agecat', all = TRUE] # merge with the weight for each group
  rm(tt)
  
  # Merge these weights back to the data table
 
