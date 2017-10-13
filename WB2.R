##try worldbank with data catalog excel file
library(readxl)
library(rvest)
library(dplyr)
library(tidyr)
library(readr)
library (ggplot2)
library(doSNOW);library(iterators);library(httr);library(magrittr);library(zipcode);library(stringr)

##download data catalog
dlink<-"http://api.worldbank.org/v2/datacatalog/downloadfile"
temp <- tempfile()
download.file(dlink,temp,mode='wb')
WB_catalog<-read_xls(temp)
colnames(WB_catalog)[19]<-"bulkdownload"
colnames(WB_catalog)[20]<-"bulkdownloadlink"
condensed_catalog<-select(WB_catalog,Topics,API,Name,Description,bulkdownload,bulkdownloadlink,APIAccessURL)

##filter out datasets using keywords
condensed_catalog%>%
  filter(grepl("inan|Econ|Trade|Health|labor",Topics,ignore.case=TRUE),grepl("CSV",bulkdownloadlink,ignore.case=TRUE))->API_table

##Filter out bulk download URL's 
split_new<-function(list){
  final <- c()
  for (i in 1:length(list[[1]])) {
  x<-str_extract(list[[1]][i], "(?<==)([[:alpha:]]|[[:punct:]])+")
  final<-c(final,x)
  }
  return (final)
}

##List of bulk download URL's
as.list(API_table$bulkdownloadlink)%>%
lapply(str_split,pattern=';')->links
download_links<-lapply(links,split_new)
csv_list<-list()
csv_zip<-list()
excel_list<-list()
i=1

#check for csv download, create list of CSV download links (preffered format so replacing all of links with 'used' to indicate best available link was found)
for (i in 1:length(download_links)){
  for (j in 1:length(download_links[[i]])){
    if (grepl("\\.csv",download_links[[i]][[j]],ignore.case=TRUE)){
      csv_list[length(csv_list)+1]<-download_links[[i]][[j]]
      download_links[i]<-c("used")
      break
    }
    
  }
}

#check for csv.zip download, create list of Zip download links
for (i in 1:length(download_links)){
  for (j in 1:length(download_links[[i]])){
    if (grepl("csv\\.zip",download_links[[i]][[j]],ignore.case=TRUE)){
      csv_zip[length(csv_zip)+1]<-download_links[[i]][[j]]
      download_links[i]<-c("used")
      break
    }

  }
}

# csv_data<-list()
# ##download csv's and store in list of data frames
# for (i in 1:length(csv_list)){
# csv_data[[i]]<-read.csv(csv_list[[i]])
# 
# }

##downlaod csv.zip, unzip and store in list of data frames
csv_zipdata<-data_frame()
for (i in 1:length(csv_zip)){
  temp <- tempfile()
  download.file(csv_zip[[i]],temp)
  files2<-unzip(temp)
  if (length(files2)>1)
  {
    csv_zipdata_step<-read.csv(files2[[grep("Data",files2)]],stringsAsFactors = FALSE )
    select(csv_zipdata_step,-contains("Code"))%>%
      select(-contains("MRV"))%>%
    gather('year','value',contains("X"))->csv_zipdata_step
    csv_zipdata<-rbind(csv_zipdata,csv_zipdata_step)
  }
  else{
    csv_zipdata_step<-read.csv(files2[[1]],stringsAsFactors = FALSE )
    select(csv_zipdata_step,-contains("Code"))%>%
      select(-contains("MRV"))%>%
      gather('year','value',contains("X"))->csv_zipdata_step
    csv_zipdata<-rbind(csv_zipdata,csv_zipdata_step)
  }
  unlink(temp)
  
}
csv_zipdata<-csv_zipdata[!is.na(csv_zipdata$value),]
csv_zipdata$provider<-"worldbank"
csv_zipdata$frequency<-""
csv_zipdata<-separate(csv_zipdata,Indicator.Name,c("Indicator.Name","unit"),sep="\\(")
csv_zipdata<-unite_(csv_zipdata,"description",c("ï..Country.Name","Indicator.Name"),sep="|",remove=TRUE)
