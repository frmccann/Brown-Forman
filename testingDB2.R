library(rvest)
library(dplyr)
library(tidyr)
library(readr)
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
  meta_data<-list()
  ##obtain dataset key in order to later form API requests
  data_keys<-as.list(fromJSON(gsub("bis",provider_names1[i],base_data_request1))$data)
  
  for (j in 1:length(data_keys)){ 
    ##download actual data and meta data and flatten any nested data frames
    request_url<-gsub("bis-pp-ls",data_keys[j],final_base)
    meta_link<-paste0("https://api.db.nomics.world/api/v1/json/datasets/",data_keys[j],"/dimensions")
    try(meta_data[[j]]<-fromJSON(meta_link)[[1]],silent = TRUE)
    try(unflattend<-fromJSON(request_url,simplifyDataFrame = TRUE)[[1]],silent = TRUE)
    column<-unflattend$values
    values_df<-do.call("rbind", column)
  
    ##replicate rows that cotained nested data frames and merge to unnested data frame
    repetition_list<-lapply(column,function(x) length(x[1][[1]]))
    unflattend[rep(1:nrow(unflattend),repetition_list),]%>%
      select(-values)->unflattend
    final<-cbind(unflattend,values_df)
    do.call("rbind",final$dimensions)%>%
      as.data.frame()%>%
      t()->unnested_dimensions
    colnames(unnested_dimensions)<-paste0("dimension:",colnames(unnested_dimensions))
    unnested_dimensions%>%
      cbind(final)%>%
      select(-dimensions)->final
    provider_data[[j]]<-final

  }
  return(list(provider_data,meta_data))
  # return(meta_data)
}

##register cores
cl <- makeCluster(detectCores(all.tests = TRUE))
registerDoSNOW(cl)


##Thingy will be a list of meta data and actual data, odd indices are actual data even are the meta data

thingy<-foreach(A=1:length(provider_names),
                .export="parallel_function",
                .verbose=TRUE,
                .combine="c",
                .packages=c("jsonlite","foreach","dplyr","tidyr","doSNOW"))%dopar%{---
                    return(parallel_function(i=A))
                }

##parallelized function
parallel_function2<-function (provider_names1=provider_names,base_data_request1=base_data_request,i){
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
                      return(parallel_function2(i=A))
                  }

stopCluster(cl)

##create list to store final results
test_list<-list()
##iterate through each element in the result from the data pull above (only odd numbers contain actual datasets)
for (z in 1:(length(thingy)/2)){
  test_list[[z]]<-list()
  y=2*z-1

  ##iterate through all the datasets from one provider
for (i in 1:length(thingy[[y]])){
  test_list[[z]][[i]]<-thingy[[y]][[i]]
  if (length(thingy[[y+1]][[i]])==0){
    next()
  }
  test_list[[z]][[i]]$description<-as.character(namedata[[z]][[i]]$name)
  ##iterate through the Dimensions of each data set, which are using codes and need metadata
  for (j in 1:length(thingy[[y+1]][[i]])){
  
    ##get meta data corresponding to this particular dimension of this dataset and store it as dataset 'x'
  as.data.frame(thingy[[y+1]][[i]][[j]])%>%
      t()%>%
      as.data.frame()->x
    
    ##try to merge the meta data
  try({
  if (contains(match = "-",vars=x$V1,ignore.case = FALSE)<=0 & contains(match="percent",vars=x$V1)<=0){
  x$codes<-row.names(x)
  name1<-colnames(test_list[[z]][[i]])[j]
  merge(x,test_list[[z]][[i]],by.x  ='codes',by.y =name1)%>%
    select(-contains('codes'))->test_list[[z]][[i]]
  colnames(test_list[[z]][[i]])[colnames(test_list[[z]][[i]])=='V1']<-name1

  }
    else{

    }
  },silent=TRUE)
  }
  
}
}

