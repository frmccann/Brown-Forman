library(readxl)
library(rvest)
library(dplyr)
library(tidyr)
library(readr)
library (ggplot2)
library(doSNOW);library(iterators);library(httr);library(magrittr);library(zipcode);library(stringr)
library(XML)
library(rsdmx)
##downlaod bulk zip file, unzip and store
download_link2<-"http://www.imf.org/external/pubs/ft/weo/2017/01/weodata/SDMXData.zip"
temp <- tempfile()
download.file(download_link2,temp,mode='wb')
files<-unzip(temp)
data <- xmlParse(files[1])
z<-readSDMX(files[1],isURL = FALSE)
stats<-as.data.frame(z)

download.file("http://www.imf.org/external/pubs/ft/weo/2017/01/weodata/WEOAprl2017_DSD.xlsx",temp,mode='wb')
meta_data<-list()
for ( i in 2:7){
  
meta_data[[i-1]]<-read_xlsx(temp,sheet=i,skip=7)

}
##merge meta data to make dataset more understandable
stats<-merge(stats,meta_data[[3]],by.x = 'CONCEPT',by.y='Code')
stats<-merge(stats,meta_data[[4]],by.x = 'UNIT',by.y='Code')
stats<-merge(meta_data[[2]],stats,by.y = 'REF_AREA',by.x='Code')
colnames(stats)[names(stats)=="Description.x"]<-"CodeInfo"
colnames(stats)[names(stats)=="Description.y"]<-"UnitInfo"

##store final results as stats
stats<-select(stats,-c(X__1,X__2,X__3,X__4))
stats<-select(stats,c(Description,CodeInfo,UnitInfo,FREQ,TIME_PERIOD,OBS_VALUE))
##Keep frequency, date, value, unitinfo, description
stats$provider<-"IMF"
stats<-unite_(stats,"description",c("Description","CodeInfo"),sep="|",remove=TRUE)
colnames(stats)<-c("description","unit","frequency","year","value","provider")
