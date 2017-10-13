library (ggplot2)
library(jsonlite)
library(doSNOW);library(iterators);library(httr);library(magrittr);library(zipcode);library(stringr)
library(parallel)
library(foreach)
library(doSNOW)
##Download list of providers (sources of data), store the "slugs" used for API queries in a list
providers_list<-as.data.frame(fromJSON("https://api.db.nomics.world/api/v1/json/providers")$data)
providers_list<-filter(providers_list,!grepl("fed|bea|imf",slug))
provider_names<-as.list(providers_list$slug)

provider_data<-list()
meta_data<-list()

##construct base api links 
base_data_request<-"https://api.db.nomics.world/api/v1/json/providers/bis/datasets/keys"
final_base<-"https://api.db.nomics.world/api/v1/json/datasets/bis-pp-ls/values?"

##parallelized function
parallel_function<-function (provider_names1=provider_names,base_data_request1=base_data_request,i){
  provider_data<-list()
  name_data<-list()
  ##obtain dataset key in order to later form API requests
  data_keys<-as.list(fromJSON(gsub("bis",provider_names1[i],base_data_request1))$data)
  
  for (j in 1:length(data_keys)){ 
    ##download actual data and meta data and flatten any nested data frames
    request_url<-gsub("bis-pp-ls",data_keys[j],final_base)
    meta_link<-paste0("https://api.db.nomics.world/api/v1/json/datasets/",data_keys[j])
    try(name_data[[j]]<-fromJSON(meta_link)[[1]],silent = TRUE)
  }
  return(list(name_data))
  # return(meta_data)
}

##register cores
cl <- makeCluster(detectCores(all.tests = TRUE))
registerDoSNOW(cl)


##Thingy will be a list of meta data and actual data, odd indices are actual data even are the meta data

namedata<-foreach(A=1:length(provider_names),
                .export="parallel_function",
                .verbose=TRUE,
                .combine="c",
                .packages=c("jsonlite","foreach","dplyr","tidyr","doSNOW"))%dopar%{---
                    return(parallel_function(i=A))
                }

stopCluster(cl)