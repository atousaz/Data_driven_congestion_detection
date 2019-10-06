###################normal and abnormal days detection
library(tidyverse)
library(dplyr)
library(Rlof)
library(DescTools)
library(DMwR)
List<-read_csv("C:/Users/atousaz/Desktop/loop/Quantiles/segm.csv")
Segments<-List$segm
rb=NULL

for (sts in Segments){
  ad<-sprintf("%s%s%s", "../segments/",sts, ".csv")
  dataset<-read_csv(ad)
  dataset$tt<-60/dataset$speed
  date<-unique(dataset$date)
  for (d in as.character(date)){
    filter<-dataset%>%filter(date==d)
    q<-quantile(filter$tt, probs = seq(from = 0, to = 1, by = 0.05))
    df <- data.frame(names = row.names(as.data.frame(q)), q)
    a<-spread(df, names, q)
    a$Mean=apply(a, 1, mean)
    a$SD=apply(a, 1, sd)
    cb<-cbind(a,date=d,segment=sts)
    rb<-rbind(rb,cb)
  }
  }
rm(ad,sts,d)

rbaa=NULL
for (seg in (Segments)){
  avg_lof<-data.frame()
  filter_s<-rb%>%filter(segment==seg)
  for (k in seq(20,120,by=10)){
     
    fil_lof<-lofactor(filter_s[,c(22,23)], k=k)
    fil_lof_<-as.data.frame(fil_lof)
    
    if (k==20) {
      avg_lof<-fil_lof_}
    
      avg_lof<-cbind(avg_lof,fil_lof_)
    
    
  }
  
  avg_lof<-cbind(avg_lof, "lof_mean"=rowMeans(avg_lof[,c(2:ncol(avg_lof))], na.rm=TRUE))
    
  
  
  rba<-cbind(filter_s,lof_mean=avg_lof$lof_mean)
  
  filter<-rba%>%filter(lof_mean!=Inf)
  I<-rba%>%filter(lof_mean==Inf)
  
  ordered_data<-filter%>%arrange(desc(lof_mean))
  ordered_data <- ordered_data %>% mutate(id = row_number())
  thresh<-elbow_finder(ordered_data[,'id'],ordered_data[,'lof_mean'])[2]
  
  
  
  outliers<-ordered_data%>%filter(lof_mean>thresh)%>%select(segment,date)#349
  normaldays<-ordered_data%>%filter(lof_mean<thresh)%>%select(segment,date)#45170
  I<-I%>%select(segment,date)
  normaldays<-rbind(normaldays,I)
  ad<-sprintf("%s%s%s%s%s", "../Quantiles/",seg, "/","anomolous_days_list",".csv")
  add<-sprintf("%s%s%s%s%s", "../Quantiles/",seg, "/","normal_days_list",".csv")
  


  
  
  
  write.csv(outliers,ad)
  write.csv(normaldays,add)
  print(ad)
  print(add)
  }


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



# 
# total=NULL
# for (segg in Segments){
#   total=NULL
#   new=NULL
#   db=NULL
#   filter=NULL
#   dbscan::kNNdistplot(filter_data[,1:21], k =  40)
#   abline(h = 0.06, lty = 2) #optimal eps value }
#   filter_data<-rb%>%filter(segment==1485859295)
#   
# p_=NULL
# for (e in seq(0.02,1,by=0.02) ){
#     db <- fpc::dbscan(filter_data[,1:21], eps = e, MinPts = 40)
#     new<-as.data.frame(cbind(cl=db$cluster,filter_data))
#     p=nrow(new%>%filter(cl=="0"))/(nrow(new%>%filter(cl=="1"))+nrow(new%>%filter(cl=="0")))
#     c_<-cbind(percentage=p,eps=e)
#     p_<-rbind(p_,c_)
#     p_<-as.data.frame(p_)
#   }
#   
#   total<-rbind(total,new)
#   print(segg)
#   outliers_data<-total%>%filter(cl=="0")
#   normaldays_data<-total%>%filter(cl=="1")
#   
#   anomolous_days_list<-outliers_data[,c('segment','date')]
#   
#   normal_days_list<-normaldays_data[,c('segment','date')]
#   
#   ad<-sprintf("%s%s%s%s%s", "../Quantiles/",segg, "/","anomolous_days_list",".csv")
#   add<-sprintf("%s%s%s%s%s", "../Quantiles/",segg, "/","normal_days_list",".csv")
#   write.csv(anomolous_days_list,ad)
#   write.csv(normal_days_list,add)
#   
# }
# 
# 



