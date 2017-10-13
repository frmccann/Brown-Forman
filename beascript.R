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

names_list<-list()
API<-"BA7D2E9D-125A-4BF6-863F-A2015C0F447C"
info<-fromJSON("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GETDATASETLIST&ResultFormat=JSON&"        )
data_names_list<-as.list(info$BEAAPI$Results$Dataset$DatasetName)[-c(1,2)]

##function that retrieves parameter values, will be used later on
paramater_values<-function(ds,pn){
  paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetParameterValues&DataSetName=",ds,"&ParameterName=",pn,"&ResultFormat=JSON&")%>%
           fromJSON()->info
  return(info)
}

##download NIPA tables from 1968 to present
NIPA_link<-"https://www.bea.gov//national/nipaweb/SS_Data/Section"
nipa_data<-list()
for (i in 1:7)
{
  temp<-read.csv(paste0(NIPA_link,as.character(i),"All_csv.csv"),header = FALSE)
  nipa_data[[i]]<-temp
}

meta_data<-list()
index1<-0
final_nipa<-data.frame()
for (u in 1:length(nipa_data)){
indices<-grep("Adden|Rest|midperiod",nipa_data[[u]]$V2)
meta_data[[u]]<-as.data.frame(nipa_data[[u]][1:2,1:1])
test<-nipa_data[[u]][8:indices[1],]
test<-as.data.frame(sapply(test,function(x)as.character(x)),stringsAsFactors = FALSE)
test<-as.data.frame(sapply(test,function(x) gsub(",","",x)),stringsAsFactors = FALSE)
test[,c(4:ncol(test))]<-sapply(test,function(x)as.numeric(as.character(x)))
test<-test[,colSums(is.na(test))!=nrow(test)]
colnames(test)<-test[1,]
test<-test[2:nrow(test),]
test<-gather(test,year,value,5:ncol(test))
test$Line<-sapply(test$Line,function(x)x<-as.character(meta_data[[u]][1,1]))
test$unit<-as.character(meta_data[[u]][2,1])
test<-test[,c(1,2,5,6,7)]
final_nipa<-rbind(final_nipa,test)
}
final_nipa$provider<-"NIPA tables"
colnames(final_nipa)[[1]]<-"description"
colnames(final_nipa)[[2]]<-"description2"
final_nipa<-unite_(final_nipa,"description",c("description","description2"),remove=TRUE)
final_nipa$value<-as.character(final_nipa$value)
final_nipa$frequency<-""

##broken
##NIU data pull 
paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetParameterValues&DataSetName=",data_names_list[1],"&ParameterName=TableID&ResultFormat=JSON&")%>%
  fromJSON()->info
IDList<-as.list(info$BEAAPI$Results$ParamValue$TableID)
NIU_data<-list()
for (i in 1:length(IDList)){
  URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&DataSetName=",data_names_list[1],"&TableID=",IDList[i],"&Frequency=")
  if (exists("Results",where=content(GET(paste0(URL,"M&Year=X&ResultFormat=JSON&")))$BEAAPI)){
   try(NIU_data[[i]]<-fromJSON(paste0(URL,"M&Year=X&ResultFormat=JSON&"))$BEAAPI$Results$Data,silent=TRUE)
  }
  else if(exists("Results",where=content(GET(paste0(URL,"Q&Year=X&ResultFormat=JSON&")))$BEAAPI)){
    try(NIU_data[[i]]<-fromJSON(paste0(URL,"Q&Year=X&ResultFormat=JSON&"))$BEAAPI$Results$Data,silent=TRUE)
  }
  else {
    try(NIU_data[[i]]<-fromJSON(paste0(URL,"A&Year=X&ResultFormat=JSON&"))$BEAAPI$Results$Data,silent=TRUE)
  }
}

##Multi National Enterprises data pull
DOI_values<-c("Outward","Inward","State","Parent")
MNEDI_data<-data.frame()
for (i in 1:length(DOI_values)){
  URL<-paste0("https://www.bea.gov/api/data/?&SeriesId=30&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&DataSetName=MNE&Year=ALL&Country=ALL&DirectionOfInvestment=",DOI_values[i],"&Classification=CountrybyIndustry")
  test<-fromJSON(URL)[['BEAAPI']][['Results']][['Data']]
  try({
  MNEDI_data<-rbind(MNEDI_data,select(test,c(Year,SeriesName,Row,Column,TableScale,DataValueUnformatted)))
  },silent=TRUE)
}
MNEDI_data<-unite_(MNEDI_data,"description",c("SeriesName","Row",'Column'),sep="|",remove=TRUE)
MNEDI_data$provider<-"bea|Multinational Enterprises"
colnames(MNEDI_data)<-c("year","description","unit","value","provider")

##MNE activities data pull
MNEactivities_data<-data.frame()
for (i in 1:length(DOI_values)){
  URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&DataSetName=MNE&Year=ALL&Country=ALL&Industry=all&DirectionOfInvestment=",DOI_values[i],"&Classification=CountryByIndustry&SeriesId=ALL&NonBankAffiliatesOnly=0&OwnershipLevel=1")
  print (URL)
  try(test<-fromJSON(URL)[['BEAAPI']][['Results']][['Data']],silent=TRUE)
  try(MNEactivities_data<-rbind(MNEactivities_data,select(test,c(Year,SeriesName,Row,Column,TableScale,DataValueUnformatted)),silent=TRUE))
  test<-data.frame()
}
MNEactivities_data<-unite_(MNEactivities_data,"description",c("SeriesName","Row",'Column'),sep="|",remove=TRUE)
MNEactivities_data$provider<-"bea|Multinational Enterprises Activities"
colnames(MNEactivities_data)<-c("year","description","unit","value","provider")

##Fixed Assets data pull 
fixed_assets_data<-data.frame()

names_list<-as.list(paramater_values("FixedAssets","TableID")$BEAAPI$Results$ParamValue$TableID)
for (i in 1:length(names_list)){
  URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&DataSetName=FixedAssets&TableID=",names_list[i],"&Year=X&")
  if (not(exists("Error",where=content(GET(URL))$BEAAPI$Results))){
    temp<-merge(fromJSON(URL)[['BEAAPI']][['Results']][['Notes']],as.data.frame(fromJSON(URL)[['BEAAPI']][['Results']][['Data']]),by.x='NoteRef',by.y = 'NoteRef')
    temp<-select(temp,c(NoteText,LineDescription,TimePeriod,CL_UNIT,UNIT_MULT,DataValue))
    fixed_assets_data<-rbind(fixed_assets_data,temp)
  }
}
fixed_assets_data$provider<-"bea|Fixed Assets"
fixed_assets_data<-fixed_assets_data<-unite_(fixed_assets_data,"description",c("NoteText","LineDescription"),sep="|",remove=TRUE)
fixed_assets_data<-unite_(fixed_assets_data,"unit",c("CL_UNIT","UNIT_MULT"),sep="|multiplier=",remove=TRUE)
colnames(fixed_assets_data)<-c("description","year","unit","value","provider")

##ITA data pull 

names_list<-as.list(paramater_values("ITA","Indicator")$BEAAPI$Results$ParamValue$Key)
ITA_data<-data.frame()
test<-list()
for (i in 1:length(names_list)){
URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&DataSetName=ITA&Indicator=",names_list[i],"&")
x<-fromJSON(URL)[['BEAAPI']][['Results']][['Data']]
try({test<-merge(fromJSON(URL)[['BEAAPI']][['Results']][['Notes']],x,by.x='NoteRef',by.y = 'NoteRef')
test<-select(test,c(NoteText,AreaOrCountry,Frequency,TimePeriod,TimeSeriesDescription,CL_UNIT,UNIT_MULT,DataValue))
ITA_data<-rbind(ITA_data,test)
next()},silent = TRUE)
ITA_data<-rbind(ITA_data,x)
}
ITA_data$provider<-"bea|ITA"
ITA_data<-unite_(ITA_data,"description",c("NoteText","TimeSeriesDescription","AreaOrCountry"),sep="|",remove=TRUE)
ITA_data<-unite_(ITA_data,"unit",c("CL_UNIT","UNIT_MULT"),sep="|multiplier=",remove=TRUE)
colnames(ITA_data)<-c("description","frequency","year","unit","value","provider")

##IIP data pull
test1<-0
names_list<-as.list(paramater_values("IIP","TypeOfInvestment")$BEAAPI$Results$ParamValue$Key)
IIP_data<-data.frame()
IIP_data_notes<-list()
for (i in 1:length(names_list)){
  URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&DataSetName=IIP&TypeOfInvestment=",names_list[i],"&Frequency=A")
  y<-fromJSON(URL)[['BEAAPI']][['Data']]
  x<-fromJSON(URL)[['BEAAPI']][['Notes']]
  try({temp<-merge(x,y,by.x = 'NoteRef',by.y = 'NoteRef')
  temp<-select(IIP_data[[i]],c(NoteText,TypeOfInvestment,Frequency,TimeSeriesDescription,TimePeriod,CL_UNIT,UNIT_MULT,DataValue))
  IIP_data<-rbind(IIP_data,temp)
  next()},silent = TRUE)
  test1<-test1+1
  IIP_data<-rbind(IIP_data,y)
}
IIP_data$provider<-"bea|IIP"
IIP_data<-select(IIP_data,-Year,-TypeOfInvestment,-Component,-TimeSeriesId,-NoteRef)
IIP_data<-unite_(IIP_data,"unit",c("CL_UNIT","UNIT_MULT"),sep="|multiplier=",remove=TRUE)
colnames(IIP_data)<-c("frequency","description","year","unit","value","provider")

##GDP by Industry data pull 

GDPbyIndustryData<-data.frame()
GDPbyIndustryData_notes<-list()
names_list<-as.list(paramater_values("GDPbyIndustry","TableID")$BEAAPI$Results$ParamValue$Key)
for (i in 1:length(names_list)){
  URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&DataSetName=GDPbyIndustry&TableID=",names_list[i],"&Frequency=A&Year=ALL&Industry=ALL&")
  try({y<-fromJSON(URL)[['BEAAPI']][['Results']][['Data']]
  x<-fromJSON(URL)[['BEAAPI']][['Results']][['Notes']]
  temp<-merge(x,y,by.x = 'NoteRef',by.y = 'NoteRef')
  temp<-select(temp,c(NoteText,Frequency,Quarter,IndustrYDescription,DataValue))
  GDPbyIndustryData<-rbind(GDPbyIndustryData,temp)
      next()},silent = TRUE)
  try({temp<-fromJSON(URL)[['BEAAPI']][['Results']][['Data']]
  temp<-select(temp,c(NoteText,Frequency,Quarter,IndustrYDescription,DataValue))
  GDPbyIndustryData<-rbind(GDPbyIndustryData,temp)
  }
  ,silent=TRUE)
  }
GDPbyIndustryData$provider<-"bea|GDPbyIndustry"
GDPbyIndustryData<-separate(GDPbyIndustryData,NoteText,c("NoteText","unit"),sep="\\[")
GDPbyIndustryData<-unite_(GDPbyIndustryData,"description",c("NoteText","IndustrYDescription"),sep="|",remove=TRUE)
colnames(GDPbyIndustryData)<-c("description","unit","frequency","year","value","provider")

##Regional Income data pull

names_list<-as.list(paramater_values("RegionalIncome","TableName")$BEAAPI$Results$ParamValue$Key)
fromJSON(paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetParameterValues&DataSetName=RegionalIncome&ParameterName=LineCode&TableName=",names_list[1],"&"))$BEAAPI$Results$ParamValue%>%
  as.data.frame()->linecodes
linecodes%>%
  separate(Desc,c("y","n"),sep="]")->tempo2

RegionalIncome_scrape<-function(tablename,linecodes){
  answer<-list()
  GeoFips<-c("STATE","COUNTY","MSA","MIC","DIV","CSA","PORT")
  tempo<-filter(linecodes,y==paste0("[",tablename))
  linecodes<-as.list(tempo$Key)
  for (j in 1:length(linecodes)){
    answer[[j]]<-list()
    for (k in 1:length(GeoFips)){
      URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&datasetname=RegionalIncome&TableName=",tablename,"&LineCode=",linecodes[j],"&GeoFips=",GeoFips[k],"&Year=All&")
      try({
        answer[[j]][[k]]<-merge(fromJSON(URL)[['BEAAPI']][['Results']][['Notes']],fromJSON(readLines(URL))[['BEAAPI']][['Results']][['Data']],by='NoteRef')
        answer[[j]][[k]]$Code<-sapply(answer[[j]][[k]]$Code,function(x)x<-as.character(fromJSON(URL)[['BEAAPI']][['Results']][['Statistic']]))
        next()
        },silent = TRUE)
      try(answer[[j]][[k]]<-fromJSON(readLines(URL))[['BEAAPI']][['Results']][['Data']], silent = TRUE)
    }
  }
  return (answer)
  
}

##register cores
cl <- makeCluster(detectCores(all.tests = TRUE),nnodes=20)
registerDoSNOW(cl)


RegionalIncome_data<-foreach(A=1:length(names_list),
                .export="RegionalIncome_scrape",
                .verbose=TRUE,
                # .combine="c",
                .packages=c("jsonlite","foreach","dplyr","tidyr","doSNOW"))%dopar%{---
                    return(RegionalIncome_scrape(names_list[A],tempo2))
                }

##needs more meta data


##Regional Product data pull

component_list<-as.list(paramater_values("RegionalProduct","Component")$BEAAPI$Results$ParamValue$Key)
RegionalProduct_scrape<-function(component){
  answer<-list()
  Sys.sleep(5)
  GeoFips<-c("STATE","MSA")
  ##error in line below
  IndustryIDs<-as.list(fromJSON(paste0("https://bea.gov/api/data/?UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetParameterValuesFiltered&datasetname=RegionalProduct&TargetParameter=IndustryId&Component=",component))$BEAAPI$Results$ParamValue$Key)
  for (j in 1:length(IndustryIDs)){
    ##why error in next line?
    answer[[j]]<-list()
    for (k in 1:length(GeoFips)){
      URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&datasetname=RegionalProduct&Component=",component,"&IndustryId=",IndustryIDs[j],"&GeoFips=",GeoFips[k],"&Year=All&")
      try({
        answer[[j]][[k]]<-merge(fromJSON(URL)[['BEAAPI']][['Results']][['Notes']],fromJSON(URL)[['BEAAPI']][['Results']][['Data']],by='NoteRef')
        next()
      },silent = TRUE)
      try(answer[[j]][[k]]<-fromJSON(URL)[['BEAAPI']][['Results']][['Data']], silent = TRUE)
    }
    }
  return (answer)
  
}

RegionalProduct_data<-foreach(A=1:length(component_list),
                              #.export="RegionalIncome_scrape",
                              .verbose=TRUE,
                              .combine="c",
                              .packages=c("jsonlite","foreach","dplyr","tidyr","doSNOW"))%dopar%{---
                                  return(RegionalProduct_scrape(component_list[A]))
                              }
stopCluster(cl)

##InputOutput data pull

InputOutput_data<-data.frame()
InputOutput_data_notes<-list()
names_list<-as.list(paramater_values("InputOutput","TableId")$BEAAPI$Results$ParamValue$Key)
for (i in 1:length(names_list)){
  URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&datasetname=InputOutput&TableID=",names_list[i],"&Year=All")
  try({
    y<-fromJSON(URL)[['BEAAPI']][['Results']][['Data']]
    x<-fromJSON(URL)[['BEAAPI']][['Results']][['Notes']]
    InputOutput_data<-rbind(InputOutput_data,merge(x,y,by='NoteRef'))
    next()
  }
  ,silent=TRUE)
  try(InputOutput_data<-rbind(InputOutput_data,fromJSON(URL)[['BEAAPI']][['Results']][['Data']],silent = TRUE))
}
InputOutput_data$frequency<-""
InputOutput_data$provider<-"bea|InputOutput"
InputOutput_data<-select(InputOutput_data,-NoteRef,-TableID,-RowCode,-ColCode)
InputOutput_data<-unite_(InputOutput_data,"description",c("NoteText","RowType","RowDescr","ColType","ColDescr"),sep="|",remove=TRUE)
InputOutput_data$unit<-"see descr"
colnames(InputOutput_data)<-c("description","year","value","unit","provider")

##Underlying Gross Domestic Product by Industry data pull 

UnderlyingGDPbyIndustry_data<-data.frame()
names_list<-as.list(paramater_values("underlyingGDPbyIndustry","TableId")$BEAAPI$Results$ParamValue$Key)
for (i in 1:length(names_list)){
  URL<-paste0("https://www.bea.gov/api/data/?&UserID=BA7D2E9D-125A-4BF6-863F-A2015C0F447C&method=GetData&DataSetName=UnderlyingGDPbyIndustry&TableID=",names_list[i],"&Frequency=Q&Year=ALL&Industry=ALL")
  try({
  y<-fromJSON(URL)[['BEAAPI']][['Results']][['Data']]
  x<-fromJSON(URL)[['BEAAPI']][['Results']][['Notes']]
  UnderlyingGDPbyIndustry_data<-rbind(UnderlyingGDPbyIndustry_data,merge(x,y,by='NoteRef'))
  next()
  },silent=TRUE)
  
  try(UnderlyingGDPbyIndustry_data<-rbind(UnderlyingGDPbyIndustry_data,fromJSON(URL)[['BEAAPI']][['Results']][['Data']],silent = TRUE))
}
UnderlyingGDPbyIndustry_data<-select(UnderlyingGDPbyIndustry_data,-NoteRef,-TableID,-Quarter,-Industry)
UnderlyingGDPbyIndustry_data$provider<-"bea|Underlying GDP by Industry"
UnderlyingGDPbyIndustry_data<-separate(UnderlyingGDPbyIndustry_data,NoteText,c("NoteText","unit"),sep="\\[")
UnderlyingGDPbyIndustry_data<-unite_(UnderlyingGDPbyIndustry_data,"description",c("NoteText","IndustrYDescription"),sep="|",remove=TRUE)
colnames(UnderlyingGDPbyIndustry_data)<-c("description","unit","frequency","year","value","provider")

union(final_nipa,MNEDI_data)%>%
  union(MNEactivities_data)%>%
  union(fixed_assets_data)%>%
  union(ITA_data)%>%
  union(IIP_data)%>%
  union(GDPbyIndustryData)%>%
  union(InputOutput_data)%>%
  union(UnderlyingGDPbyIndustry_data)->BEA_total_dataset
