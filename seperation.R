library(dplyr)
library(tidyverse)
###reading 9 segments data
data=NULL
seg<-read_csv("../Data/dsm_loop.csv")

sg<-unique(seg$'segment')#128
data_s_m=NULL
for (s in sg){
  for (i in c('April','Aug','Dec','Feb','Jan','July','June','March','May','Nov','Ocb','Sep')){
    
    ad<-sprintf("%s%s%s", "../Data/",i, "_loop.csv")
    print(i)
    data1<-read.csv(ad,stringsAsFactors = FALSE)
    data_s<-data1%>%filter(segment==s)
    rm(data1)
    data_s_m<-rbind(data_s_m,data_s)
  }
  link<-sprintf("%s%s%s", "../segments/",s, ".csv")
  data_s_m$time <- as_hms(data_s_m$time)
  data_s_m<-data_s_m%>%arrange(date,time)%>%filter(score_30>0)
  write.csv(data_s_m,link)
  data_s_m=NULL
}




