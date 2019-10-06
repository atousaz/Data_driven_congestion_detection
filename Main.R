library(tidyverse)
library(dplyr)
library(ggplot2)
library(lubridate)
library(data.table)
library(hms)
List<-read_csv("../Data/list.csv")
Segments<-List$seg
Segments<-Segments[Segments!=1485706689]

# 
# List<-read_csv("../Data/list2.csv")
# Segments<-List$seg
# Segments<-Segments[Segments!=1485706689]



aux<-read_csv("../total_metadata_april(2).csv")
aux=as.data.frame(aux)
aux<-aux%>%select(c('Segment ID','Road','Direction','Segment Length(Miles)'))
colnames(aux)<-c('id','road','direction','Length_m')
aux_f<-aux%>%filter(id %in% List$seg)
result_r=NULL
list_RC_=NULL
list_NRC_hour_delay=NULL
list_hour_delay_RC=NULL
# functions ---------------------------------------------------------------


###########################Delay
#len<-0.604986
delay=function(x,len){
  
  traveltime<-x[1]
  ref<-x[2]
  dif<-(traveltime/60)-(rep(len,times=nrow(traveltime))/ref)
  arg_dif<-apply(dif,1,function(x){max(x,0)})
  arg_dif<-as.data.frame(arg_dif)
  sm<-sum(arg_dif)
  ddd<-sm/60
  return(ddd)
}



#write.csv(r,"./1485655352/1485655352_percentage_congestion.csv")





elbow_finder <- function(x_values, y_values) {
  # Max values to create line
  max_x_x <- max(x_values)
  max_x_y <- y_values[which.max(x_values)]
  max_y_y <- max(y_values)
  max_y_x <- x_values[which.max(y_values)]
  max_df <- data.frame(x = c(max_y_x, max_x_x), y = c(max_y_y, max_x_y))
  
  # Creating straight line between the max values
  fit <- lm(max_df$y ~ max_df$x)
  
  # Distance from point to line
  distances <- c()
  for(i in 1:length(x_values)) {
    distances <- c(distances, abs(coef(fit)[2]*x_values[i] - y_values[i] + coef(fit)[1]) / sqrt(coef(fit)[2]^2 + 1^2))
  }
  
  # Max distance point
  x_max_dist <- x_values[which.max(distances)]
  y_max_dist <- y_values[which.max(distances)]
  
  return(c(x_max_dist, y_max_dist))
}

# main loop ---------------------------------------------------------------


for (sts in Segments ){
  
  ad<-sprintf("%s%s%s%s%s", "../CP/",sts,"/",sts, ".csv")
  dataset<-read_csv(ad)
  rm(ad)
  dataset<-as.data.frame(dataset[,-1])
  dataset$cp<-0
  dataset$group<-"NA"
  
  n=1
  
  while (n < (nrow(dataset)-1)){
    n=n+1
    #print(n)
    if((n+1)==(nrow(dataset)+1)){
      
      break()}
    
    else if(!dataset$Prob[n]<0.01 ){
      
      for (j in (n+1):nrow(dataset)){
        
        #print(n)
        
        
        if (dataset$Prob[j]<0.01){
          
          d=max(dataset$Prob[n:j-1])
          #print(d)
          t<-dataset$Prob[n:j-1]==d
          #print(t[-1])
          dataset$cp[n:j-1][which(t==TRUE)]<-1
          
          
          break()
          
          #print(n)
          
        }
        
      }
      
      n<-j  
    }
    
  }
  
  
  
  
  
  rm(d)
  rm(j)
  rm(n)
  rm(t)
  
  
  
  counter=1
  l=1
  for (i in 1:nrow(dataset)){
    
    if (dataset$cp[i]==1){
      
      dataset$group[l:i]<-counter
      #print(counter)
      counter=counter+1
      l=i+1
      #print(l)
    }
    # check<-dataset$cp[i+1:nrow(dataset)]==0
    # if (dataset$cp[i]==1 & unique(check)=="TRUE"){
    #   
    #   dataset$group[i:nrow(dataset)]<-counter+1
    # }
    # 
  }
  
  
  dataset$group[dataset$group=="NA"]<-counter
  rm(counter,i,l)
  #write.csv(dataset,"1485655352/result/grouping_test.csv")
  total=NULL
  group=unique(dataset$group)
  for (g in group){
    fil_data<-dataset%>%filter(group==g)%>%mutate(Q15=quantile(Speed,0.15), Q50=quantile(Speed,0.50),Q85=quantile(Speed,0.85))
    total<-rbind(total,fil_data) 
    
  }
  
  rm(g,group,fil_data)
  #write.csv(total,"total.csv")
  
  clusters<-kmeans(total[,c('Q15','Q50','Q85')],2)
  
  d<-cbind(cluster=clusters$cluster,total)
  
  m<-d%>%group_by(cluster)%>%mutate(avg=mean(Speed))%>%group_by(cluster,avg)%>%summarize()
  m<-as.data.frame(m)
  clu<-m%>%filter(avg==min(avg))%>%select(cluster)#finding the cluster 
  
  d$visual <- ifelse(d$cluster==clu$cluster, d$Speed, 0)
  rm(clusters,m)
  
  dataset<-d
  rm(d,total)
  
  
  ab_q_ad<-sprintf("%s%s%s%s%s", "../Quantiles/",sts,"/","anomolous_days_list", ".csv")
  norm_q_ad<-sprintf("%s%s%s%s%s", "../Quantiles/",sts,"/","normal_days_list", ".csv")
  ab_norm<-read_csv(ab_q_ad)
  ab_norm_d<-ab_norm$date
  nomr_days<-read_csv(norm_q_ad)
  nomr_days$date<-as.Date(nomr_days$date,  "%m/%d/%Y")
  norm_d<-nomr_days$date
  
  rm(ab_norm,nomr_days,ab_q_ad,norm_q_ad)
  
  
  ab_visual<-dataset%>%filter(date %in% ab_norm_d)
  norm_visual<-dataset%>%filter(date %in% norm_d)
  
  cat(sep=" ", sts, "norm/ab separation done.")
  
  #######elbow cut off for normal days to get RC time instances
  norm_visual$char_time<-as.character(norm_visual$time)
  timespan=unique(as.character(norm_visual$time))
  
  r=NA
  for (t in timespan){
    time_filter<-norm_visual%>%filter(char_time==t)
    con_filter<-norm_visual%>%filter(cluster==clu$cluster & char_time==t)
    percentage<-cbind(con_percentage=nrow(con_filter)/nrow(time_filter),time=unique(t),count=nrow(con_filter))
    percentage<-as.data.frame(percentage)
    r<-rbind(r,percentage)}
  r<-as.data.frame(r)
  r<-r[-1,]
  
  r$con_percentage<-as.numeric(as.character(r$con_percentage))
  r$time<-as_hms(as.character(r$time))
  sorted_percentages<-r%>%arrange(desc(con_percentage))
  
  ordered_data <- sorted_percentages %>% mutate(id = row_number())
  el_thre<-elbow_finder(ordered_data[,4],ordered_data[,1])
  
  
  thres<-clu$cluster
  percent_cuttedoff<-r%>%filter(con_percentage >= el_thre[2])
  
  #RC 
  #elbow_p_time<-read_csv("elbow_percentage_timedata.csv")
  percent_cuttedoff$char_time<-(as.character(percent_cuttedoff$time))
  
  cutted_time<-unique(percent_cuttedoff$char_time)
  RC<-norm_visual%>%filter(cluster==thres)
  
  out_RC<-RC%>%subset(!(char_time %in% cutted_time))
  
  RC<-RC%>%filter(char_time %in% cutted_time)
  
  elbow_cut_data_RC<-merge(RC,percent_cuttedoff, by=c("char_time" ,"time"))#4445
  
  cat(sep=" ", sts, "Elbow RC  done.")

# calculating average speed and % for RC congestions ----------------------

  
  #######calculating average speed and % for RC congestions
  
  #write.csv(elbow_cut_data_RC,"../Main_out/elbow_cut_data_RC_sts.csv")
  
  elbow_cut_data_RC$hour = format(as.POSIXct(elbow_cut_data_RC$time,format="%H:%M:%S"),"%H")
  avg_hour=NULL
  avg_c=NULL
  cut_h<-unique(elbow_cut_data_RC$hour)
  
  for (hh in cut_h){
    h_filter<-elbow_cut_data_RC%>%filter(hour==hh)%>%select(Speed,con_percentage)
    avg_speed<-apply(h_filter[1],2,mean)
    con_mean<-apply(h_filter[2],2,mean)
    con_SD<-apply(h_filter[2],2,sd)
    avg_c<-cbind(avg_speed,hh,con_mean,con_SD)
    avg_hour<-rbind(avg_hour,avg_c)
  }
  cat(sep=" ", sts, "avg RC speed, con mean/hour.")
# normal congestion delay -------------------------------------------------

  
  ##########################normal congestion delay
  
  ##########################################################################
  ########################RC part in NRC needs to be added to RC############
  ##########################################################################
  
  congestion_ab<-ab_visual%>%filter(cluster==thres)#388 times
  
  RCtimes<-unique(elbow_cut_data_RC$char_time)#191
  
  congestion_ab$char_time<-as.character(congestion_ab$time)#898
  
  
  #RC times in abnormal days which needs to be added to RC total time for further calculations
  RC_in_NRC<-congestion_ab%>%subset(char_time %in% RCtimes)
  
  RC_in_NRC<-RC_in_NRC%>%select(c('char_time','time','cluster','Speed','Prob','time','segment','date','raw_speed','Traveltime','score','ref_speed','cp','group','Q15','Q50','Q85','visual'))
  RC_in_NRC$hour = format(as.POSIXct(RC_in_NRC$time,format="%H:%M:%S"),"%H")
  
  congestion_ab<-congestion_ab%>%subset(!(char_time %in% RCtimes))#197 times when we extract RC times
  ###################################################
  ######Adding NRC to RC#
  #####################
  elbow_cut_data_RC<-elbow_cut_data_RC%>%select(c('char_time','time','cluster','Speed','Prob','time','segment','date','raw_speed','Traveltime','score','ref_speed','cp','group','Q15','Q50','Q85','visual','hour'))
  elbow_cut_data_RC<-rbind(elbow_cut_data_RC,RC_in_NRC)
  
  Total_RC_hours<-nrow(elbow_cut_data_RC)/60
  
  RC_days<-unique(elbow_cut_data_RC$date)
  
  hour_delay_RC=NULL
  length<-aux_f%>%filter(id==sts)%>%select(Length_m)
  
  for (di in as.character(RC_days)){
    #print(di)
    counteri<-elbow_cut_data_RC%>%filter(date==di)
    h<-unique(counteri$hour)
    for (hh in as.character(h)){
      h_filter<-counteri%>%filter(hour==hh)
      h_filter<-h_filter%>%select(c(Traveltime,ref_speed,Speed,raw_speed,date,time,cluster))
      delay_cal<-delay(h_filter,length$Length_m)
      h_d_RC<-cbind(hh,delay_cal,di,sts)
      h_d_RC<-as.data.frame(h_d_RC)
      hour_delay_RC<-rbind(hour_delay_RC,h_d_RC)
    }
  }
  
  hour_delay_RC$delay<-as.numeric(as.character(hour_delay_RC$delay_cal))
  hour_delay_RC$date<-as.Date(hour_delay_RC$di)
  hour_delay_RC$hour<-as.character(hour_delay_RC$hh)
  hour_delay_RC<-hour_delay_RC%>%arrange(date,hh)
  hour_delay_RC<-hour_delay_RC%>%select(date,hour,delay,sts)
  list_hour_delay_RC<-rbind(list_hour_delay_RC,hour_delay_RC)
  
  
  dh<-unique(hour_delay_RC$hour)
  cat(sep=" ", sts, " RC delay done.")
  THD_r=NULL
  THD=NULL
  for (hhh in dh){
    fili<-hour_delay_RC%>%filter(hour==hhh)
    Total_delay_hour<-apply(fili[3],2,sum)
    #print(Total_delay_hour)
    THD<-cbind(Total_delay_hour,hour=hhh)
    THD_r<-rbind(THD_r,THD)
    THD=NULL
  }
  
  THD_r<-as.data.frame(THD_r)
  
  THD_r$delay<-as.numeric(as.character(THD_r$Total_delay_hour))
  Total_delay_hour_RC<-sum(THD_r$delay)
  
  sssss<-rep(sts,nrow(THD_r))
  list_RC<-cbind(segment=sssss,THD_r)
  
  # NRC  --------------------------------------------------------------------
  
  
  #########################Delays abnormal days

  
  congestion_ab_outRC<-rbind(congestion_ab,out_RC)#536 times when we add outliers of Normal days RC
  congestion_ab_outRC$hour = format(as.POSIXct(congestion_ab_outRC$time,format="%H:%M:%S"),"%H")
  
  Total_NRC_hours<-nrow(congestion_ab_outRC)/60
  
  write.csv(congestion_ab_outRC,"../Main_out/visual_1485655352_abnorm_RC_OUT.csv")
  abn_cong_date<-unique(congestion_ab_outRC$date)
  
  hour_delay=NULL
  for (di in as.character(abn_cong_date)){
    counteri<-congestion_ab_outRC%>%filter(date==di)
    h<-unique(counteri$hour)
    for (hh in h){
      h_filter<-counteri%>%filter(hour==hh)
      h_filter<-h_filter%>%select(c(Traveltime,ref_speed,Speed,raw_speed,date,time,cluster))
      delay_cal<-delay(h_filter,length$Length_m)
      h_d<-cbind(hh,delay_cal,di,sts)
      hour_delay<-rbind(hour_delay,h_d)
    }
  }
  
  NRC_hour_delay<-as.data.frame(hour_delay)
  NRC_hour_delay$delay<-as.numeric(as.character(NRC_hour_delay$delay_cal))
  NRC_hour_delay$date<-as.Date(NRC_hour_delay$di)
  NRC_hour_delay$hour<-as.character(NRC_hour_delay$hh)
  NRC_hour_delay<-NRC_hour_delay%>%select(date,hour,delay,sts)
  list_NRC_hour_delay<-rbind(list_NRC_hour_delay,NRC_hour_delay)
  
  
  rm(h_d,counteri,h_filter,di,h,hh,delay_cal)
  
  
  
  cat(sep=" ", sts, " NRC delay done.")
  # delay % RC and NRC ------------------------------------------------------
  
  
  ###########################################delay percentages RC and NRC
  
  Total_delay_hour_NRC<-sum(NRC_hour_delay$delay)
  RC_delay_percentage<-Total_delay_hour_RC/(Total_delay_hour_RC+Total_delay_hour_NRC)
  NRC_delay_percentage<-Total_delay_hour_NRC/(Total_delay_hour_RC+Total_delay_hour_NRC)
  
  #write.csv(hour_delay,"./1485655352/NRC_hour_delay.csv")
  
  
  
  
  # visual ------------------------------------------------------------------
  #write.csv(norm_visual,"./1485655352/RC/norm_visual.csv")
  #write.csv(ab_visual,"./1485655352/NRC/ab_visual.csv")
  
  #write.csv(elbow_cut_data_RC,"./1485655352/RC/visual_1485655352_norm_justRC.csv")
  
  
  aux_info<-aux_f%>%filter(id==sts)%>%select(road,direction)
  result<-cbind(segment=sts,Total_delay_hour_NRC,Total_delay_hour_RC,Total_NRC_hours,Total_RC_hours,length=length$Length_m,road=aux_info$road,dir=aux_info$direction)
  
  result_r<-rbind(result_r,result)
  
  
  list_RC_<-rbind(list_RC_,list_RC)
  
  
  cat(sep=" ", sts, " Result done.")
}#main for loop

list_NRC_hour_delay<-cbind(list_NRC_hour_delay,con=rep("NRC",nrow(list_NRC_hour_delay)))
list_hour_delay_RC<-cbind(list_hour_delay_RC,con=rep("RC",nrow(list_hour_delay_RC)))
list_delay<-rbind(list_NRC_hour_delay,list_hour_delay_RC)

write.csv(list_delay,"../Main/list_delay.csv")



write.csv(result_r,"../Main/result_r_f.csv")

write.csv(list_RC_,"../Main/list_RC_f.csv")



#write.csv(norm_visual,"../Main_out/visual_1485655352_norm_final.csv")
#
#write.csv(ab_visual,"../Main_out/visual_1485655352_abnorm_final.csv")






# Needed functions --------------------------------------------------------



# some ggplot  ------------------------------------------------------------



ordered_data$highlight<-ifelse(ordered_data$con_percentage == el_thre[2], "highlight", "normal")
mycolours <- c("highlight" = "red", "normal" = "grey50")

ggplot(aes(x=ordered_data[,4],ordered_data[,1]),data=ordered_data)+geom_point(size = 3,aes(colour = highlight))+scale_color_manual("Status", values = mycolours) 

g<-subset(ordered_data,con_percentage==el_thre[2])
gg<-ordered_data[c(1,nrow(ordered_data)),]


#rr<-r
#rr$time<-as.POSIXct(r$time ,format="%H:%M:%S")
ggplot(ordered_data,aes(x=ordered_data[,4],ordered_data[,1]))+geom_line(size=1)+geom_point(aes(x=el_thre[1],y=el_thre[2]), colour="red",size=2)+
  xlab("ID number") + ylab("Congestion percentage")+ theme_classic()+  theme(legend.position = "none")+
  geom_hline(yintercept=el_thre[2], linetype="dashed", color = "red")+
  geom_line(aes(x=gg[,4],gg[,1], colour="red"),data=gg)+    theme(text = element_text(size=15),
                                                                  axis.text.x = element_text(angle=90, hjust=1))

ggplot(data=r,aes(x=time, y=con_percentage))+ geom_line()+
  geom_hline(yintercept=el_thre[2], linetype="dashed", color = "red")+
  xlab("Time") + ylab("Congestion percentage")+ theme_bw()+ 
  theme(text = element_text(size=15),axis.text.x = element_text(angle=90, hjust=1))
###################
#write.csv(ordered_data,"ordered_data.csv")
#write.csv(ordered_data,"1485655352/Smoothing data/1485655352_congestionpercentage_sorted.csv")









