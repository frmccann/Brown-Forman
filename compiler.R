library(dplyr)
library(tidyr)
stats$frequency<-as.character(stats$frequency)
csv_zipdata$value<-as.character(csv_zipdata$value)
union(BEA_total_dataset,stats)%>%
  union(csv_zipdata)->Macroeconomic_dataset