
# Libraries ---------------------------------------------------------------


library(tidyverse)
library(dplyr)
library(ggplot2)
library(lubridate)
library(data.table)
library(hms)

# Segments ----------------------------------------------------------------


List <- read_csv("../Data/list.csv")
Segments <- List$seg
Segments <- Segments[Segments != 1485706689]


total_duration=NULL
aux <- read_csv("../total_metadata_april(2).csv")
aux = as.data.frame(aux)
aux <-
  aux %>% select(c('Segment ID', 'Road', 'Direction', 'Segment Length(Miles)'))
colnames(aux) <- c('id', 'road', 'direction', 'Length_m')
aux_f <- aux %>% filter(id %in% List$seg)
result_r = NULL
list_RC_ = NULL
list_NRC_hour_delay = NULL
list_hour_delay_RC = NULL
difference = NULL
speed_heat = NULL
speed_template_daily = NULL
RC_Congestions_data=NULL
NRC_Congestions_data=NULL
reginal_el_thre=NULL
tansittion=NULL
# functions ---------------------------------------------------------------




delay = function(x, len) {
  traveltime <- x[1]
  ref <- x[2]
  dif <- (traveltime / 60) - (rep(len, times = nrow(traveltime)) / ref)
  arg_dif <- apply(dif, 1, function(x) {
    max(x, 0)
  })
  arg_dif <- as.data.frame(arg_dif)
  sm <- sum(arg_dif)
  ddd <- sm / 60
  return(ddd)
}


elbow_finder <- function(x_values, y_values) {
  # Max values to create line
  max_x_x <- max(x_values)
  max_x_y <- y_values[which.max(x_values)]
  max_y_y <- max(y_values)
  max_y_x <- x_values[which.max(y_values)]
  max_df <-
    data.frame(x = c(max_y_x, max_x_x),
               y = c(max_y_y, max_x_y))
  
  # Creating straight line between the max values
  fit <- lm(max_df$y ~ max_df$x)
  
  # Distance from point to line
  distances <- c()
  for (i in 1:length(x_values)) {
    distances <-
      c(distances,
        abs(coef(fit)[2] * x_values[i] - y_values[i] + coef(fit)[1]) / sqrt(coef(fit)[2] ^
                                                                              2 + 1 ^ 2))
  }
  
  # Max distance point
  x_max_dist <- x_values[which.max(distances)]
  y_max_dist <- y_values[which.max(distances)]
  
  return(c(x_max_dist, y_max_dist))
}

# main loop ---------------------------------------------------------------


for (sts in Segments) {
 
  start_time <- Sys.time()
  ad <- sprintf("%s%s%s%s%s", "../CP/", sts, "/", sts, ".csv")
  dataset <- read_csv(ad)
  rm(ad)
  dataset <- as.data.frame(dataset[, -1])
  dataset$cp <- 0
  dataset$group <- "NA"
  
  n = 1
  
  
  while (n < (nrow(dataset) - 1)) {
    n = n + 1
    
    if ((n + 1) == (nrow(dataset) + 1)) {
      break()
    }
    
    else
      if (!dataset$Prob[n]  <  0.01) {
        for (j in (n  +  1):nrow(dataset)) {
         
          
          
          if (dataset$Prob[j]  <  0.01) {
            d  =  max(dataset$Prob[n:j  -  1])
            
            t  <-  dataset$Prob[n:j  -  1]  ==  d
            
            dataset$cp[n:j  -  1][which(t  ==  TRUE)]  <-  1
            
            
            break()
            
          
            
          }
          
        }
        
        n <- j
      }
    
  }
  
  
  
  
  
  rm(d)
  rm(j)
  rm(n)
  rm(t)
  
  
  
  counter = 1
  l = 1
  for (i in 1:nrow(dataset)) {
    if (dataset$cp[i] == 1) {
      dataset$group[l:i] <- counter
  
      counter = counter + 1
      l = i + 1
 
    }

  }
  
  
  dataset$Hour<-hour(dataset$time)
  Night<-c(21:23,0:6)
  dataset$Period<-ifelse(dataset$Hour  %in% Night,"Night","Day")
  
  dataset$group[dataset$group == "NA"] <- counter
  rm(counter, i, l)
  
  total = NULL
  group = unique(dataset$group)
  for (g in group) {
    fil_data <-
      dataset %>% filter(group == g) %>% mutate(
        Q15 = quantile(Speed, 0.15),
        Q50 = quantile(Speed, 0.50),
        Q85 = quantile(Speed, 0.85),
        Population=nrow(.)
      )
    total <- rbind(total, fil_data)
    
  }
  
  rm(g, group, fil_data)
 
  total<-total%>%subset(population>10)
  tansition_state<-total%>%subset(population<10)
  tansittion<-rbind(tansittion,tansition_state)
  
  clusters <- kmeans(total[, c('Q15', 'Q50', 'Q85')], 2)
  
  d <- cbind(cluster = clusters$cluster, total)
  

  
  m <-
    d %>% group_by(cluster) %>% mutate(avg = mean(Speed)) %>% group_by(cluster, avg) %>%
    summarize()
  m <- as.data.frame(m)
  clu <-
    m %>% filter(avg == min(avg)) %>% select(cluster)#finding the cluster
  
  difer <- abs(m[1, 2] - m[2, 2])
  
  temp <- cbind(difer, sts)
  difference <- rbind(difference, temp)
  difference <- as.data.frame(difference)
  
   

# congestion detection ----------------------------------------------------

  
  HMS<-max(m[1, 2],m[2, 2])
  
  d$visual <-
    ifelse(d$cluster == clu$cluster & difer >=16, d$Speed, 0)
  rm(clusters, m)
  

# finding outliers --------------------------------------------------------
  # finding outliers --------------------------------------------------------
  group = unique(dataset$group)
  if (difer < 16 ){
    
    
    for (g in group) {
      if(g==length(group)){
        break()
      }
      fil_data <-
        d %>% filter(group == g) %>% mutate(
          state_mean = mean(Speed)
        )
      
      
      
      fil_data_one<-d %>% filter(group == as.numeric(g)+1) %>% mutate(
        state_mean = mean(Speed)
      )
      
      
      
      delta<-abs(unique(fil_data_one$state_mean)-unique(fil_data$state_mean))
      
      if (delta >= 16){
        
        low=min(unique(fil_data_one$state_mean),unique(fil_data$state_mean))
        
        if(unique(fil_data$state_mean)==low){
          
          d$visual<-ifelse(d$group==g,d$Speed,d$visual)
          
          
        }
        
        
        
        if(unique(fil_data_one$state_mean)==low){
          above=as.character(as.numeric(g)+1)
          
          d$visual<-ifelse(d$group==above,d$Speed,d$visual)
          
          
        }
        
        
      }
      
    }
    
    
  }
  
  
  
  
  
  
  #w <- sprintf("%s%s%s", "./Visual/", sts, ".csv")
  #write.csv(d, w)
  
  dataset <- d
  rm(d, total)
  

# elbow threshold for identifying RC NRC ------------------------------------------------------------


  #######elbow cut off for normal days to get RC time instances
  dataset$char_time <- as.character(dataset$time)
  timespan = unique(as.character(dataset$time))
  
  r = NA
  for (t in timespan) {
    time_filter <- dataset %>% filter(char_time == t)
    con_filter <- dataset %>% filter(visual != 0 & char_time == t)
    percentage <-
      cbind(
        con_percentage = nrow(con_filter) / nrow(time_filter),
        time = unique(t),
        count = nrow(con_filter)
      )
    percentage <- as.data.frame(percentage)
    r <- rbind(r, percentage)
  }
  r <- as.data.frame(r)
  r <- r[-1, ]
  
  r$con_percentage <- as.numeric(as.character(r$con_percentage))
  r$time <- as_hms(as.character(r$time))
  
  sorted_percentages <- r %>% arrange(desc(con_percentage))
  ordered_data <- sorted_percentages %>% mutate(id = row_number())
  el_thre <- elbow_finder(ordered_data[, 4], ordered_data[, 1])
  
  
  col_el_thre<-cbind(theshold=el_thre[2],segment=sts)
  reginal_el_thre<-rbind(reginal_el_thre,col_el_thre)
  
  thres <- clu$cluster
  percent_cuttedoff <- r %>% filter(con_percentage >= el_thre[2])

  percent_cuttedoff$char_time <-
    (as.character(percent_cuttedoff$time))

  cutted_time <- unique(percent_cuttedoff$char_time)

# RC detection ------------------------------------------------------------

  
  RC <- dataset %>% filter(visual != 0)
  out_RC <- RC %>% subset(!(char_time %in% cutted_time))
  RC <- RC %>% filter(char_time %in% cutted_time)
  
  elbow_cut_data_RC <-
    merge(RC, percent_cuttedoff, by = c("char_time" , "time"))#4445   
  
  

  #Extracting RC at nights when percentage is not enough, and put them in NRC
  Night_NRC<-elbow_cut_data_RC%>%filter(Period =="Night" & con_percentage < 0.3 )
  
  elbow_cut_data_RC<-elbow_cut_data_RC%>%filter(!(Period =="Night" & con_percentage < 0.3 ))
  
  
  RC_Congestions_data<-rbind(RC_Congestions_data,elbow_cut_data_RC)
  
  #rc_ad <- sprintf("%s%s%s", "./RC/", sts, ".csv")
  #write.csv(elbow_cut_data_RC, rc_ad)
# NRC detection -----------------------------------------------------------

  Night_NRC<-Night_NRC%>%subset(!con_percentage)
  out_RC<-rbind(out_RC,Night_NRC)                           
  NRC_Congestions_data<-rbind(NRC_Congestions_data,out_RC)

  #nrc_ad <- sprintf("%s%s%s", "./NRC/", sts, ".csv")
  #write.csv(out_RC, nrc_ad)
  


  cat(sep = " ", sts, "identifying RC, NRC  done.")


# average speed template yearly--------------------------------------------------


  elbow_cut_data_RC$hour = format(as.POSIXct(elbow_cut_data_RC$time, format =
                                               "%H:%M:%S"),
                                  "%H")
  

  RCtimes <- unique(elbow_cut_data_RC$char_time)

  elbow_cut_data_RC <-
    elbow_cut_data_RC %>% select(
      c(
        'char_time',
        'time',
        'cluster',
        'Speed',
        'Prob',
        'time',
        'segment',
        'date',
        'raw_speed',
        'Traveltime',
        'score',
        'ref_speed',
        'cp',
        'group',
        'Q15',
        'Q50',
        'Q85',
        'visual',
        'hour'
      )
    )

  
  #######calculating average speed and % for RC congestions yearly
  
  avg_hour = NULL
  cut_h <- unique(elbow_cut_data_RC$hour)
  
  timespan_SPEED_HEAT <- unique(elbow_cut_data_RC$char_time)
  for (timespan in timespan_SPEED_HEAT) {
    h_filter <-
      elbow_cut_data_RC %>% filter(char_time == timespan) %>% select(Speed)
    avg_speed <- apply(h_filter[1], 2, mean)
    sd_speed <- apply(h_filter[1], 2, sd)
    
    avg_c <- cbind(sts, timespan, avg_speed, sd_speed)
    avg_hour <- rbind(avg_hour, avg_c)
  }
  speed_heat <- rbind(speed_heat, avg_hour)
  
  cat(sep = " ", sts, "avg RC speed, con mean/hour.", "\n")
  
  # average speed template yearly--------------------------------------------------
  avg_hour = NULL
  avg_hour_daily = NULL
  days <- unique(elbow_cut_data_RC$date)
  for (day in as.character(days)) {
    d_filter <- elbow_cut_data_RC %>% filter(date == day)
    timespan_SPEED_temp <- unique(elbow_cut_data_RC$char_time)
    for (timespan in timespan_SPEED_temp) {
      h_filter <- d_filter %>% filter(char_time == timespan) %>% select(Speed)
      avg_speed <- apply(h_filter[1], 2, mean)
      sd_speed <- apply(h_filter[1], 2, sd)
      
      avg_c <- cbind(sts, day, timespan, avg_speed, sd_speed)
      avg_hour_daily <- rbind(avg_hour_daily, avg_c)
    }
    
  }
  
  speed_template_daily <- rbind(speed_template_daily, avg_hour_daily)
  
  

# Delay RC-------------------------------------------------------------------

  
  
  
  Total_RC_hours <- nrow(elbow_cut_data_RC) / 60
  #
  RC_days <- unique(elbow_cut_data_RC$date)
  #
  hour_delay_RC = NULL
  length <- aux_f %>% filter(id == sts) %>% select(Length_m)
  if (nrow(elbow_cut_data_RC) > 0) {
    for (di in as.character(RC_days)) {
      #print(di)
      counteri <- elbow_cut_data_RC %>% filter(date == di)
      h <- unique(counteri$hour)
      for (hh in as.character(h)) {
        h_filter <- counteri %>% filter(hour == hh)
        h_filter <-
          h_filter %>% select(c(
            Traveltime,
            ref_speed,
            Speed,
            raw_speed,
            date,
            time,
            cluster
          ))
        delay_cal <- delay(h_filter, length$Length_m)
        h_d_RC <- cbind(hh, delay_cal, di, sts)
        h_d_RC <- as.data.frame(h_d_RC)
        hour_delay_RC <- rbind(hour_delay_RC, h_d_RC)
      }
    }
    #
    hour_delay_RC$delay <-
      as.numeric(as.character(hour_delay_RC$delay_cal))
    hour_delay_RC$date <- as.Date(hour_delay_RC$di)
    hour_delay_RC$hour <- as.character(hour_delay_RC$hh)
    hour_delay_RC <- hour_delay_RC %>% arrange(date, hh)
    hour_delay_RC <- hour_delay_RC %>% select(date, hour, delay, sts)
    list_hour_delay_RC <- rbind(list_hour_delay_RC, hour_delay_RC)

    dh <- unique(hour_delay_RC$hour)
    cat(sep = " ", sts, " RC delay done.")
    THD_r = NULL
    THD = NULL
    for (hhh in dh) {
      fili <- hour_delay_RC %>% filter(hour == hhh)
      Total_delay_hour <- apply(fili[3], 2, sum)
      
      THD <- cbind(Total_delay_hour, hour = hhh)
      THD_r <- rbind(THD_r, THD)
      THD = NULL
    }
   
    THD_r <- as.data.frame(THD_r)
  
    THD_r$delay <- as.numeric(as.character(THD_r$Total_delay_hour))
    Total_delay_hour_RC <- sum(THD_r$delay)
    
    sssss <- rep(sts, nrow(THD_r))
    list_RC <- cbind(segment = sssss, THD_r)
  }



# Delay NRC ---------------------------------------------------------------



  if (nrow(out_RC) > 0) {
    out_RC$hour = format(as.POSIXct(out_RC$time, format = "%H:%M:%S"), "%H")
    
    Total_NRC_hours <- nrow(out_RC) / 60

    abn_cong_date <- unique(out_RC$date)
    
    hour_delay = NULL
    for (di in as.character(abn_cong_date)) {
      counteri <- out_RC %>% filter(date == di)
      h <- unique(counteri$hour)
      for (hh in h) {
        h_filter <- counteri %>% filter(hour == hh)
        h_filter <-
          h_filter %>% select(c(
            Traveltime,
            ref_speed,
            Speed,
            raw_speed,
            date,
            time,
            cluster
          ))
        delay_cal <- delay(h_filter, length$Length_m)
        h_d <- cbind(hh, delay_cal, di, sts)
        hour_delay <- rbind(hour_delay, h_d)
      }
    }
    
    NRC_hour_delay <- as.data.frame(hour_delay)
    NRC_hour_delay$delay <-
      as.numeric(as.character(NRC_hour_delay$delay_cal))
    NRC_hour_delay$date <- as.Date(NRC_hour_delay$di)
    NRC_hour_delay$hour <- as.character(NRC_hour_delay$hh)
    NRC_hour_delay <-
      NRC_hour_delay %>% select(date, hour, delay, sts)
    list_NRC_hour_delay <-
      rbind(list_NRC_hour_delay, NRC_hour_delay)

    rm(h_d, counteri, h_filter, di, h, hh, delay_cal)
  }

  cat(sep = " ", sts, " NRC delay done.")


# Delay percentage!!! -----------------------------------------------------

  

  if (nrow(out_RC) > 0 & nrow(elbow_cut_data_RC) > 0) {
    Total_delay_hour_NRC <- sum(NRC_hour_delay$delay)
    RC_delay_percentage <-
      Total_delay_hour_RC / (Total_delay_hour_RC + Total_delay_hour_NRC)
    NRC_delay_percentage <-
      Total_delay_hour_NRC / (Total_delay_hour_RC + Total_delay_hour_NRC)
    
  }
  
  
  aux_info <- aux_f %>% filter(id == sts) %>% select(road, direction)
  
  if (nrow(elbow_cut_data_RC) == 0){
    Total_delay_hour_RC=0
    Total_RC_hours=0}
  if (nrow(out_RC) == 0){Total_delay_hour_NRC=0
  Total_NRC_hours=0}
  
  result <-
    cbind(
      segment = sts,
      Total_delay_hour_NRC,
      Total_delay_hour_RC,
      Total_NRC_hours,
      Total_RC_hours,
      length = length$Length_m,
      road = aux_info$road,
      dir = aux_info$direction
    )
  
  result_r <- rbind(result_r, result)
  
  
  list_RC_ <- rbind(list_RC_, list_RC)
  
  
  cat(sep = " ", sts, " Result done.")
  end_time <- Sys.time()
  dur<-end_time - start_time
  duration<-cbind(sts,dur)
  total_duration<-rbind(total_duration,duration)
}
Road_dir_segment_order<-read_csv("../version_3/Road_dir_segment_order.csv")
speed_heat_minutewise_order<-merge(speed_heat,Road_dir_segment_order,by.x="sts",by.y="Sts")

# writing results ---------------------------------------------------------


list_NRC_hour_delay <-
  cbind(list_NRC_hour_delay, con = rep("NRC", nrow(list_NRC_hour_delay)))
list_hour_delay_RC <-
  cbind(list_hour_delay_RC, con = rep("RC", nrow(list_hour_delay_RC)))
list_delay <- rbind(list_NRC_hour_delay, list_hour_delay_RC)

write.csv(list_delay, "./results/list_delay.csv")



write.csv(result_r, "./results/result_r_f.csv")

write.csv(list_RC_, "./results/list_RC_f.csv")


write.csv(speed_heat, "./results/speed_heat_minutewise.csv")

write.csv(speed_heat_minutewise_order, "./results/speed_heat_minutewise_order.csv")
write.csv(speed_template_daily,
          "./results/speed_template_daily_minutewise.csv")

write.csv(RC_Congestions_data,"./results/RC_Congestions_data.csv")
write.csv(NRC_Congestions_data,"./results/NRC_Congestions_data.csv")
# done go home ------------------------------------------------------------


