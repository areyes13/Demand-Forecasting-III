# 1A - LOAD PACKAGES -----------------------------------------------------------
setwd("C:/Users/SCIP2/Box Sync/Demand Forecasting (areyesm@us.ibm.com)/Archive")
con <- file("30_day_DF.log")
sink(con, append=TRUE)
sink(con, append=TRUE, type="message")

#set Java heap size to max
options(java.parameters = "-Xmx24g")
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jdk1.8.0_102')
Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe")

#set working directory (all output / input files are stored here)
setwd("C:/Users/SCIP2/Box Sync/Demand Forecasting (areyesm@us.ibm.com)/Archive")
#directory for templates / error messages
temple.Dir <- 'C:/Users/SCIP2/Documents/Demand Forecasting III/Templates/'

#load packages
library(randomForest, lib.loc = "C:/Program Files/R/Libraries")
library(rJava, lib.loc = "C:/Program Files/R/Libraries")
library(RJDBC, lib.loc = "C:/Program Files/R/Libraries")
library(dplyr, lib.loc = "C:/Program Files/R/Libraries")
library(data.table, lib.loc = "C:/Program Files/R/Libraries")
library(lubridate, lib.loc = "C:/Program Files/R/Libraries")
library(tidyr, lib.loc = "C:/Program Files/R/Libraries")
library(zoo, lib.loc = "C:/Program Files/R/Libraries")
library(stringr, lib.loc = "C:/Program Files/R/Libraries")
library(cowsay, lib.loc = "C:/Program Files/R/Libraries")

#set random seed for consistent RF results
set.seed(666)

#set current day for naming
current.run <- Sys.Date()
save(current.run, file = 'current run.saved')

# 2A - LOAD DATA ---------------------------------------------------------------
message('2A - LOAD DATA')
fresh.data <- F
save(fresh.data, file = 'fresh.data.saved')

#query new data: connection details to PMP
jcc = JDBC("com.ibm.db2.jcc.DB2Driver",
           "C:/Users/SCIP2/Documents/DB2 Driver/db2jcc4.jar")

load('credentials.saved')

conn <- tryCatch(dbConnect(jcc,
                           as.character(credentials[3]),
                           user=as.character(credentials[1]),
                           password=as.character(credentials[2])),
                 error = function(e) {
                   sink(paste0(temple.Dir,'invalid_connection_body','.txt'))
                   cat('Let the ruling classes tremble at a Communistic revolution.\nThe proletarians have nothing to lose but their chains.\nThey have a world to win.\nWorkingmen of all countries unite!', sep = '\n')
                   cat(paste('\n','\n',Sys.time(),': PROM user ID or password invalid\nScript terminated'), sep = '\n')
                   sink()
                   
                   message('Sending PRoM Invalid Login Warning!')
                   mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                    to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                    subject = "Demand Forecasting: Invalid Credentials Error ",
                                    body = paste0(temple.Dir, 'invalid_connection_body.txt'),
                                    html = F,
                                    smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                                user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                    authenticate = TRUE,
                                    send = TRUE)
                   message('Terminating script...')
                   quit(save = 'no')
                 })
rm(credentials)

#GR DETAIL TABLE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.GR_OPNSET_DTL_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.GR_OPNSET_DTL_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
GR_OPNSET_DTL_T <- fetch(rs, -1)
save(GR_OPNSET_DTL_T, file = paste0('GR_OPNSET_DTL_T_', '.saved'))

#GR POSITIONS TABLE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.GR_OPNSET_POS_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.GR_OPNSET_POS_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
GR_OPNSET_POS_T <- fetch(rs, -1)

#FULFILLMENT SOURCE MAPPING TABLE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.FULFLMNT_SRC_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.FULFLMNT_SRC_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
FULFLMNT_SRC_T <- fetch(rs, -1)

#GR Delivery Center
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.GBL_DEL_CTR_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.GBL_DEL_CTR_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
GBL_DEL_CTR_T <- fetch(rs, -1)

#GR Project table - this gets you project name
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.GR_PROJ_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.GR_PROJ_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
GR_PROJ_T <- fetch(rs, -1)

#GR Client table - this gets you client name
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.GRDS_CLNT_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.GRDS_CLNT_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
GRDS_CLNT_T <- fetch(rs, -1)

GR <- tryCatch(GR_OPNSET_POS_T %>%
                 full_join(GR_OPNSET_DTL_T) %>%
                 left_join(FULFLMNT_SRC_T %>%
                             select(FULFLMNT_SRC_ID, FULFLMNT_SRC_DESC)) %>%
                 left_join(GBL_DEL_CTR_T %>%
                             select(GBL_DEL_CTR_ID, GBL_DEL_CTR_NM)) %>%
                 left_join(GR_PROJ_T %>%
                             select(GR_PROJ_ID, PROJ_NM, GRDS_CLNT_CD)) %>%
                 left_join(GRDS_CLNT_T %>%
                             select(GRDS_CLNT_CD, CLNT_NM)) %>%
                 select(-FULFLMNT_SRC_ID, -GBL_DEL_CTR_ID, -GRDS_CLNT_CD, -GR_PROJ_ID),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_GR_body','.txt'))
                 msg <- paste0(Sys.time(),': GR tables unable to merge"')
                 cat(cowsay::say(msg, "cat", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid GR Merge Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: GR table merge error",
                                  body = paste0(temple.Dir, 'invalid_GR_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })

#OPEN SEAT TABLE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.OPNSET_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.OPNSET_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
rs <- dbSendQuery(conn,"SELECT * FROM BCSPMP.OPNSET_T")
OPNSET_T <- fetch(rs, -1)

##new line: read in mapping table for country code, dump after filter
load("~/Demand Forecasting III/Templates/ctry.saved")

#couldn't get SQL filters to work in original DB2 query without blowing it up ;__;
OPNSET_T <- tbl_df(OPNSET_T) %>%
  left_join(ctry, by = c('WRK_CNTRY_CD' = 'CTRY')) %>%
  filter(#SET_TYP_CD == 'MP', 
    RDC_CTRY_NAME %in% c('United States', 'Canada',
                         'China', 'Mexico', 'Brazil', 'United Kingdom', 'Spain', 
                         'France', 'India', 'Japan', 'Australia', #'Germany',
                         'Netherlands', 'Sweden', 'Singapore', 
                         'Korea, Republic of', 'South Africa', 'Italy')) %>%
  select(-RDC_CTRY_NAME, -URN_RDC_CTRY)

#OPEN POSITION TABLE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.OPNSET_POS_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.OPNSET_POS_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
OPNSET_POS_T <- fetch(rs, -1)

save(OPNSET_POS_T, file = paste0('OPNSET_POS_T_', '.saved'))

#OPEN DETAIL TABLE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.OPNSET_DTL_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.OPNSET_DTL_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
OPNSET_DTL_T <- fetch(rs, -1)

#STATUS CODE REF TABLE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.POS_STAT_RESN_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.POS_STAT_RESN_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
POS_STAT_RESN_T <- fetch(rs, -1)

#CONTRACT ORG REF TABLE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.CNTRCT_OWNG_ORG_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.CNTRCT_OWNG_ORG_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
CNTRCT_OWNG_ORG_T <- fetch(rs, -1)

CNTRCT_OWNG_ORG_T <- CNTRCT_OWNG_ORG_T %>%
  mutate(Unit = ifelse(grepl('GBS', CNTRCT_OWNG_ORG_NM), 'GBS', 
                       ifelse(grepl('GTS', CNTRCT_OWNG_ORG_NM), 'GTS', 
                              'OTHER')),
         Unit = as.factor(Unit))

#CONTRACT TYPE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.CNTRCT_TYP_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.CNTRCT_TYP_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
CNTRCT_TYP_T <- fetch(rs, -1)

#CONTRACT TYPE
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.INDSTR_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.INDSTR_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
INDSTR_T <- fetch(rs, -1)
INDSTR_T$DEL_FLG <- NULL

#COUNTRY SECUTIRY CLEARANCE CODE REF
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.CNTRY_SEC_CLRNCE_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.CNTRY_SEC_CLRNCE_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
CNTRY_SEC_CLRNCE_T <- fetch(rs, -1)

#COUNTRY SECUTIRY CLEARANCE CODE REF
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.SEC_CLRNCE_TYP_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.SEC_CLRNCE_TYP_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
SEC_CLRNCE_TYP_T <- fetch(rs, -1)

#OPNSET_POS_CAND_T
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.OPNSET_POS_CAND_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.OPNSET_POS_CAND_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
OPNSET_POS_CAND_T <- fetch(rs, -1)

#OPNSET_POS_CAND_STAT
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.OPNSET_POS_CAND_STAT_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.OPNSET_POS_CAND_STAT_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
OPNSET_POS_CAND_STAT_T <- fetch(rs, -1)

#CAND_STAT_NM
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.CAND_STAT_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.CAND_STAT_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
CAND_STAT_T <- fetch(rs, -1)

#CAND_T
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM BCSPMP.CAND_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM BCSPMP.CAND_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
CAND_T <- fetch(rs, -1)
setnames(CAND_T, 'CNTRY_CD', 'cand.ctry')

#APP USR TBL
rs <- tryCatch(dbSendQuery(conn,"SELECT * FROM ACLADMIN.APP_USR_T"),
               error = function(e) {
                 sink(paste0(temple.Dir,'invalid_query_body','.txt'))
                 msg <- paste0(Sys.time(),': Unable to retrieve\n"SELECT * FROM ACLADMIN.APP_USR_T"')
                 cat(cowsay::say(msg, "chicken", type = 'string'), sep = '\n')
                 cat('Terminating script...')
                 sink()
                 
                 message('Sending Invalid Query Error!')
                 mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                  to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                  subject = "Demand Forecasting: Invalid Query Error ",
                                  body = paste0(temple.Dir, 'invalid_query_body.txt'),
                                  html = F,
                                  smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                              user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                  authenticate = TRUE,
                                  send = TRUE)
                 message('Terminating script...')
                 quit(save = 'no')
               })
APP_USR_T <- fetch(rs, -1)

#MERGES
OPNSET_T <- OPNSET_T %>%
  left_join(APP_USR_T %>% 
              select(APP_USR_ID, NOTES_ID), 
            by = c('OWNR_USR_ID' = 'APP_USR_ID')) %>%
  rename(OWNER_NOTES_ID = NOTES_ID)

OPNSET_T <- OPNSET_T %>%
  left_join(APP_USR_T %>% 
              select(APP_USR_ID, NOTES_ID), 
            by = c('DELG_USR_ID' = 'APP_USR_ID')) %>%
  rename(DELG_NOTES_ID = NOTES_ID)

security <-  CNTRY_SEC_CLRNCE_T %>%
  left_join(SEC_CLRNCE_TYP_T, by = "SEC_CLRNCE_TYP_ID") %>%
  select(CNTRY_SEC_CLRNCE_ID, SEC_CLRNCE_TYP_DESC)

#split MP and GR
tables <- OPNSET_T %>%
  left_join(OPNSET_POS_T, 
            by = 'OPNSET_ID') %>%
  left_join(OPNSET_DTL_T, 
            by = "OPNSET_ID")

mp <- tables %>%
  filter(SET_TYP_CD == 'MP')

gdev <- tables %>% 
  filter(SET_TYP_CD == 'GR',
         WRK_CNTRY_CD %in% c('CN', 'IN'))

#drop duplicate columns
nm <- colnames(gdev)[!colnames(gdev) %in% colnames(GR)]

#bring in GR fields to seats from main query
gdev <- gdev %>%
  group_by(OPNSET_ID) %>%
  select_(.dots = nm) %>%
  left_join(GR, by = 'OPNSET_ID') %>%
  ungroup() %>%
  select(-FULFLMNT_ORG_CD) %>%
  rename(FULFLMNT_ORG_CD = DEL_ORG_CD)

rm(nm)


master <- tryCatch(mp %>%
                     bind_rows(gdev) %>%
                     left_join(POS_STAT_RESN_T %>%
                                 select(STAT_RESN_CD, STAT_RESN_DESC),
                               by = 'STAT_RESN_CD') %>%
                     left_join(CNTRCT_OWNG_ORG_T %>%
                                 select(CNTRCT_OWNG_ORG_ID, CNTRCT_OWNG_ORG_NM, Unit),
                               by = 'CNTRCT_OWNG_ORG_ID') %>%
                     left_join(CNTRCT_TYP_T %>%
                                 select(CNTRCT_TYP_ID, CNTRCT_TYP_DESC), 
                               by = 'CNTRCT_TYP_ID') %>%
                     left_join(INDSTR_T,
                               by = 'INDSTR_ID') %>%
                     left_join(security, 
                               by = "CNTRY_SEC_CLRNCE_ID") %>%
                     mutate(STAT_RESN_DESC.og = STAT_RESN_DESC,
                            STAT_RESN_DESC = replace(STAT_RESN_DESC, 
                                                     STAT_RESN_CD %in% c('WB', 'WE', 'WG') & 
                                                       PREF_FULFLMNT_CHNL_CD == 'SUBC',
                                                     'Staffed by contractor/other')),
                   error = function(e) {
                     sink(paste0(temple.Dir,'invalid_master_body','.txt'))
                     msg <- paste0(Sys.time(),': Unable to create master table"')
                     cat(cowsay::say(msg, "cat", type = 'string'), sep = '\n')
                     cat('Terminating script...')
                     sink()
                     
                     message('Sending Broken Master Error!')
                     mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                      to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
                                      subject = "Demand Forecasting: Unable to generate master table error",
                                      body = paste0(temple.Dir, 'invalid_mastery_body.txt'),
                                      html = F,
                                      smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                                  user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                      authenticate = TRUE,
                                      send = TRUE)
                     message('Terminating script...')
                     quit(save = 'no')
                   })

#map candidate features
candidate.type <- OPNSET_POS_CAND_STAT_T %>%
  left_join(CAND_STAT_T[1:2], 
            by = c('CAND_OPNSET_STAT_ID' = 'CAND_STAT_ID'))

candidate <- tbl_df(OPNSET_POS_CAND_T) %>%
  filter(OPNSET_ID %in% master$OPNSET_ID) %>%
  left_join(candidate.type, by = 'OPNSET_POS_CAND_ID') %>%
  left_join(CAND_T %>%
              select(CAND_ID, cand.ctry, CAND_SRC_CD), 
            by = 'CAND_ID')

save(master, file = paste0('master ', '.saved'))

# 2B - DATA REFRESH ----------------------------------------------------
message('DATA REFRESH')
#skip if first run-through
skip <- file.exists('last.run.saved')

if(skip) {
  #bring up the last date the model was run
  load('last run.saved')
  paste0('Last model run: ', current.run - last.run, ' day(s) ago')
  #load last test set
  load(paste0('30 day testing', last.run, '.saved'))
  #get position ids from last test set
  last.pos <- unique(testing$OPNSET_POS_ID)
  
  #grab fields to update in testing data from last query
  actualizer <- tbl_df(master) %>%
    filter(OPNSET_POS_ID %in% last.pos) %>%
    select(OPNSET_POS_ID, STAT_RESN_DESC, WTHDRW_CLOS_T) %>%
    mutate(STAT_RESN_DESC = ifelse(is.na(STAT_RESN_DESC), F, 
                                   STAT_RESN_DESC == 'Staffed by contractor/other'), 
           WTHDRW_CLOS_T = ymd(str_sub(WTHDRW_CLOS_T, 1, 10)),
           #set status to 'false' if position did not close
           STAT_RESN_DESC = replace(STAT_RESN_DESC, is.na(WTHDRW_CLOS_T), F))
  
  #drop vars about to be updated
  test.old <- tbl_df(testing) %>%
    select(-STAT_RESN_DESC, -WTHDRW_CLOS_T)
  
  #update the values by merging the 2 tables
  actualizer <- left_join(test.old, actualizer, by = 'OPNSET_POS_ID')
  #now there's 2 paths:
  #A) position was CLOSED / WITHDRAWN since the last run: we want to STACK with OLD TRAINING
  #B) positions is STILL OPEN since last run: we want to stack FROZEN VIEW with OLD TRAINING and feed last query data to NEW TESTING
  
  #LOAD LAST TRAINING:
  load(paste0('30 day train', last.run, '.saved'))
  #stack last run's testing set
  train <- tryCatch(rbind(train, actualizer), 
                    error = function(e) {
                      sink(paste0(temple.Dir,'invalid_update_body','.txt'))
                      msg <- paste0(Sys.time(),': unable to update training set')
                      cat(cowsay::say(msg, "cat", type = 'string'), sep = '\n')
                      cat('Terminating script...')
                      sink()
                      
                      mailR::send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
                                       to = list('areyesm@us.ibm.com'),
                                       subject = "Demand Forecasting: Training Set Update error",
                                       body = paste0(temple.Dir, 'invalid_update_body.txt'),
                                       html = F,
                                       smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                                                   user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
                                       authenticate = TRUE,
                                       send = TRUE)
                    })
  save(train, file = paste0('30 day train', current.run, '.saved'))
  
  #select POSITIONS STILL OPEN FROM LAST UPDATE
  open.test <- actualizer %>% 
    filter(is.na(WTHDRW_CLOS_T)) %>%
    select(OPNSET_POS_ID) %>%
    left_join(master, by = 'OPNSET_POS_ID')
  
  #filter query to select only new data for analysis (STUFF NOT ALREADY IN 'TRAINING')
  new.pos <- master %>%
    filter(!OPNSET_POS_ID %in% train$OPNSET_POS_ID)
  
  #stack open pos from LAST RUN and new data from CURRENT QUERY
  tbls <- list(open.test, new.pos)
  tbls <- lapply(tbls, function(x) 
    as.data.frame(lapply(x, as.character)))
  tbls <- plyr::rbind.fill(tbls)
  
  #this is the new data that needs to flow thru the next sections to generate features
  #we will need to split into testing/training AFTER all features are built and then stack where needed
  save(tbls, file = paste0('30 day new positions raw', ',saved'))
  master <- tbls
}
# 2C - DATA PREP ----------------------------------------------------------
message('2C - DATA PREP')
start <- proc.time()
save(start, file = 'start time.saved')
rm(start)

#GR variables to keep
gr.var <- c('GR_OPNSET_POS_ID', 'ONSIT_STRT_DT', 'ONSIT_END_DT', 'NEED_CLNT_STE_IND', 'ACPT_SUB_IND','GBL_DEL_CTR_NM')

#select which variables to keep
vars <- c("OPNSET_ID", "OPNSET_POS_ID", "STAT_RESN_DESC", "INDSTR_NM", "STRT_DT", "END_DT", "CRE_T", 
          "LST_UPDT_T", 'OPN_T', "WTHDRW_CLOS_T", "CNTRCT_OWNG_ORG_NM",  "Unit",
          "BND_LOW", "BND_HIGH", "PAY_TRVL_IND", "WRK_RMT_IND", "CNTRCT_TYP_DESC", 
          "FULFILL_RISK_ID", "SEC_CLRNCE_TYP_DESC", "OWNG_CNTRY_CD", "OWNER_NOTES_ID", "DELG_NOTES_ID", 
          "WRK_CNTRY_CD", "JOB_ROL_TYP_DESC", "SKLST_TYP_DESC", 
          "SET_TYP_CD", "WRK_CTY_NM", "URG_PRIRTY_IND", 
          "PREF_FULFLMNT_CHNL_CD", "NEED_SUB_IND", gr.var)

#drop variables
data <- master %>%
  select_(.dots = vars)

#this is AGE OF RECORD (how long has it been sitting in the system?)
to.today <- function(date) {
  today <- current.run
  days <- as.integer(
    round(
      difftime(today, date, units = 'days'),
      digits = 0)
  )
  return(days)
}

#create lead days var (time btwn creation to expected start date)
lead.time <- function(created, start) {
  days <- as.integer(
    round(
      difftime(start, created, units = 'days'),
      digits = 0)
  )
  return(days)
}

#needed w/in 30 days
from.today <- function(date) {
  today <- current.run
  days <- as.integer(
    round(
      difftime(date, today, units = 'days'),
      digits = 0)
  )
  return(days)
}

#length of project
day.diff <- function(start, end) {
  days <- as.integer(
    round(
      difftime(end, start, units = 'days'),
      digits = 0)
  )
  return(days)
}

#set proper data types
#get rid of the &^%!@#%$@ trailing spaces in JRSS fields....ugh
#recode relevant variables to binary
#STAT_RESN_DESC: contractor position = TRUE; this is target
data <- data %>%
  mutate_at(vars(ONSIT_STRT_DT, ONSIT_END_DT),
            ymd) %>%
  mutate_at(vars(STRT_DT, END_DT, CRE_T, LST_UPDT_T, OPN_T, WTHDRW_CLOS_T), 
            function(x) ymd(str_sub(x, 1, 10))) %>%
  mutate_if(is.character, str_trim) %>%
  mutate(URG_PRIRTY_IND = URG_PRIRTY_IND == 'Y',
         PREF_FULFLMNT_CHNL_CD = PREF_FULFLMNT_CHNL_CD == 'SUBC',
         NEED_SUB_IND = NEED_SUB_IND == 'Y',
         STAT_RESN_DESC = ifelse(is.na(STAT_RESN_DESC), F, 
                                 STAT_RESN_DESC == 'Staffed by contractor/other'),
         PAY_TRVL_IND = PAY_TRVL_IND == 'Y',
         WRK_RMT_IND = WRK_RMT_IND == 'Y',
         Created.floor = floor_date(CRE_T, 'month'),
         OG.Start.floor = floor_date(STRT_DT, 'month'),
         Close.floor = floor_date(WTHDRW_CLOS_T, 'month'),
         Close.week = floor_date(WTHDRW_CLOS_T, 'week'),
         record.age = to.today(CRE_T),
         Lead.time.days = lead.time(CRE_T, STRT_DT),
         needed30.days = from.today(STRT_DT) <= 30,
         project.duration = day.diff(STRT_DT, END_DT)) %>%
  mutate_if(is.character, as.factor)

#position requests created AFTER work started (???)
time.vortex <- filter(data, CRE_T > STRT_DT)

# 2D - DATE CLEANUP -------------------------------------------------------
message('2D - DATE CLEANUP')
#put data for prediction into separate df and save for later
filter30 <- function(start.date) {
  day30 <- current.run + days(30)
  flag <- start.date %within% interval(current.run, day30)
  return(flag)
}

filter60 <- function(start.date) {
  day30 <- current.run + days(31)
  day60 <- day30 + days(30)
  flag <- start.date %within% interval(day30, day60)
  return(flag)
}

# 2E - CANDIDATE MAPPING --------------------------------------------------
message('2E - CANDIDATE MAPPING')
#set up helper function for NA values from casting
replacer <- function(x) {
  values <- replace(x, is.na(x), 0)
  return(values)
}

candidate$OPNSET_ID <- as.character(candidate$OPNSET_ID)
#get start date into candidate table
candidate <- left_join(candidate, 
                       data %>%
                         select(OPNSET_ID, OG.Start.floor) %>%
                         mutate(OPNSET_ID = as.character(OPNSET_ID)), 
                       by = 'OPNSET_ID')

#filter to grab the last available status 1 MONTH BEFORE START DATE
candidate <- candidate %>%
  mutate(CAND_OPNSET_STAT_T = ymd_hms(CAND_OPNSET_STAT_T),
         start.hms = ymd_hms(paste(OG.Start.floor, '00:00:00'))) %>%
  group_by(OPNSET_POS_CAND_ID, OPNSET_ID) %>%
  filter(CAND_OPNSET_STAT_T < start.hms, 
         CAND_OPNSET_STAT_T == max(CAND_OPNSET_STAT_T)) %>%
  ungroup() %>%
  distinct(.keep_all = T)

#how many candidates per SEAT
candidate.count <- candidate %>%
  group_by(OPNSET_ID, OG.Start.floor) %>%
  summarise(candidate.count = length(unique(OPNSET_POS_CAND_ID)),
            csa.src = sum(CAND_SRC_CD == 'C')) %>%
  mutate(csa.src = replacer(csa.src))

#how many candidates per SEAT DESCRIPTION CODE
candidate.type <- candidate %>%
  group_by(OPNSET_ID, OG.Start.floor, CAND_STAT_NM) %>%
  summarise(candidate.type = length(unique(OPNSET_POS_CAND_ID))) %>% 
  group_by(OPNSET_ID, OG.Start.floor) %>% 
  spread(CAND_STAT_NM, candidate.type) %>%
  mutate_each(funs(replacer))

candidate.count <- left_join(candidate.count, 
                             candidate.type, 
                             by = c('OPNSET_ID', 'OG.Start.floor')) %>%
  mutate(active.cands = (candidate.count - Confirmed - Withdrawn - `<NA>`)/candidate.count) %>%
  mutate_each(funs(replacer), -OPNSET_ID, - OG.Start.floor)

#map back into the data
data <- left_join(data, 
                  candidate.count %>% 
                    ungroup() %>%
                    mutate(OPNSET_ID = as.double(OPNSET_ID)), 
                  by = c('OPNSET_ID', 'OG.Start.floor'))
candidate.vars <- setdiff(colnames(candidate.count), c('OPNSET_ID', 'OG.Start.floor'))

save(data, candidate.vars, filter60, file = paste0('data in progress ', current.run,'.saved'))
fresh.data <- T
save(fresh.data, file = 'fresh.data.saved')

# 3A - JOB ROLE FEATURES ------------------------------------------------
message('3A - JOB ROLE FEATURES')
#collapse by JR to create new features
jr.ft <- tbl_df(data) %>%
  filter(!is.na(WTHDRW_CLOS_T)) %>%
  group_by(JOB_ROL_TYP_DESC, OG.Start.floor) %>%
  summarise(jr.count = length(OPNSET_POS_ID))

month.vec <- seq.Date(ymd('2000-01-01'), floor_date(current.run, 'month'), 'month')

month.expanded <- with(jr.ft, CJ(unique(as.factor(JOB_ROL_TYP_DESC)), month.vec))
setnames(month.expanded, colnames(month.expanded), c('JOB_ROL_TYP_DESC', 'OG.Start.floor'))

month.expanded <- left_join(month.expanded, jr.ft, by = c('JOB_ROL_TYP_DESC', 'OG.Start.floor')) %>%
  group_by(JOB_ROL_TYP_DESC) %>%
  arrange(desc(OG.Start.floor)) %>%
  mutate(jr.count = lead(jr.count, 1),
         jr.count = replace(jr.count, is.na(jr.count), 0))

# 3B - JRSS ACTUAL WEEKLY DEMAND TABLE ---------------------------------------------
message('3B - JRSS ACTUAL WEEKLY DEMAND TABLE')
#how many positions were ACTUALLY CLOSED/WITHDRAWN each month

jrss.week <- tbl_df(data) %>%
  filter(!is.na(Close.floor), WTHDRW_CLOS_T >= CRE_T) %>%
  #filter(year(Close.floor) >= 2012) %>% #in case we want to limit data size
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, Close.week) %>%
  summarise(actual = length(OPNSET_POS_ID),
            sub.actual = sum(STAT_RESN_DESC)) %>%
  mutate(JRSS = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - '))

week.vec <- with(jrss.week, seq.Date(min(Close.week), floor_date(current.run, 'week'), 'week'))

week.expanded <- with(jrss.week, CJ(unique(c(JRSS)), week.vec))
setnames(week.expanded, colnames(week.expanded), c('JRSS', 'Close.week'))

#map in the separate JRs and SSssssss's to this dummy table
mapper <- unique(jrss.week[c('JRSS', 'JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC')])
week.expanded <- left_join(week.expanded, mapper, by = 'JRSS') %>%
  select(-JRSS)

#now get the data in there and set NAs to 0
week.expanded <- left_join(week.expanded, jrss.week, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'Close.week'))  %>%
  select(-JRSS) %>%
  mutate(actual.wk = replace(actual, is.na(actual), 0),
         sub.actual.wk = replace(sub.actual, is.na(sub.actual), 0)) %>%
  select(-actual, -sub.actual)

#need this bc cumsum doesn't handle NAs
cumsum.NA <- function(x) {
  value <- cumsum(ifelse(is.na(x), 0, x)) + x*0
  return(value)
}

#sliding 4 week cumulative sum function
cumul.msum <- function(x) {
  #need to split eval in 2 due to sliding window not generating values until width is matched
  first <- cumsum.NA(x[1:4])
  roller <- rollapply(data = x,
                      width = 4,
                      FUN = cumsum.NA,
                      align = 'right',
                      fill = NA)[,4]
  #the first 4 observations (width) are NA, replace with regular cumsum output
  roller[1:4] <- first
  return(roller)
} 

#big dplyr chain to get ACTUAL JRSS features
week.expanded <- tbl_df(week.expanded) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  #filter(JOB_ROL_TYP_DESC == 'Application Developer', SKLST_TYP_DESC == 'COBOL') %>%
  #we need to 'lag' the data so that we only use the info we had AT THAT TIME (no peeking into future)
  mutate(tot.lag = lag(actual.wk, n = 1),
         sub.lag = lag(sub.actual.wk, n = 1),
         #these will create a cumulative sum of actual demand (sliding 12 month window)
         tot.4wk = cumul.msum(tot.lag),
         sub.4wk = cumul.msum(sub.lag),
         tot.4lag = lag(tot.4wk, n = 4),
         sub.4lag = lag(sub.4wk, n = 4),
         tot.delta = tot.4wk - tot.4lag,
         sub.delta = sub.4wk - sub.4lag,
         actual.wk = tot.lag,
         sub.actual.wk = sub.lag) %>%
  #drop the lagged data
  select(-tot.lag, -sub.lag)

# 3C - JRSS ACTUAL MONTHLY DEMAND TABLE  ----------------------------------------
message('3C - JRSS ACTUAL MONTHLY DEMAND TABLE')
#FIRST WE GET OUR MONTHLY JRSS ACTUALS
#how many positions were ACTUALLY CLOSED/WITHDRAWN each month
jrss.counter <- tbl_df(data) %>%
  filter(!is.na(Close.floor), WTHDRW_CLOS_T >= Created.floor) %>%
  #filter(year(Close.floor) >= 2012) %>% #in case we want to limit data size
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, Close.floor) %>%
  summarise(actual = length(OPNSET_POS_ID),
            sub.actual = sum(STAT_RESN_DESC)) %>%
  mutate(JRSS = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - '))

#there are no entries for months without observations. we need to map these to get rolling avgs
#create a sequence of all months between the earliest and latest available date
date.vec <- with(jrss.counter, seq.Date(min(Close.floor), max(Close.floor), 'month'))

#create dummy table with all JRSS/month combinations
jrss.expanded <- with(jrss.counter, CJ(unique(c(JRSS)), date.vec))
setnames(jrss.expanded, colnames(jrss.expanded), c('JRSS', 'Close.floor'))

#map in the separate JRs and SSssssss's to this dummy table
mapper <- unique(jrss.counter[c('JRSS', 'JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC')])
jrss.expanded <- left_join(jrss.expanded, mapper, by = 'JRSS') %>%
  select(-JRSS)

#dump into a placeholder
expanded <- jrss.expanded

#now get the data in there and set NAs to 0
jrss.expanded <- left_join(jrss.expanded, jrss.counter, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'Close.floor'))  %>%
  select(-JRSS) %>%
  mutate(actual = replace(actual, is.na(actual), 0),
         sub.actual = replace(sub.actual, is.na(sub.actual), 0))

#sliding 12 month cumulative sum function
cumul.yrsum <- function(x) {
  #need to split eval in 2 due to sliding window not generating values until width is matched
  first <- cumsum.NA(x[1:12])
  roller <- rollapply(data = x,
                      width = 12,
                      FUN = cumsum.NA,
                      align = 'right',
                      fill = NA)[,12]
  #the first 12 observations (width) are NA, replace with regular cumsum output
  roller[1:12] <- first
  return(roller)
} 

#big dplyr chain to get ACTUAL JRSS features
jrss.expanded <- tbl_df(jrss.expanded) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  #we need to 'lag' the data so that we only use the info we had AT THAT TIME (no peeking into future)
  mutate(tot.lag = lag(actual, n = 1),
         sub.lag = lag(sub.actual, n = 1),
         #we create a rolling 2 period avg based on the lagged values
         r2tot = rollapply(data = tot.lag, width = 2,FUN = mean,
                           align = 'right', fill = NA, na.rm = F),
         r2sub = rollapply(data = sub.lag, width = 2, FUN = mean,
                           align = 'right', fill = NA, na.rm = F),
         #these will create a cumulative sum of actual demand (sliding 12 month window)
         tot.12cs = cumul.yrsum(tot.lag),
         sub.12cs = cumul.yrsum(sub.lag)) %>%
  #drop the lagged data
  select(-tot.lag, -sub.lag)

# 3D - JRSS PROJECTED MONTHLY DEMAND TABLE --------------------------------
message('3D - JRSS PROJECTED MONTHLY DEMAND TABLE')
#use the 'expanded' object we made in previous section
setnames(expanded, 'Close.floor', 'OG.Start.floor')

#use filters to get projection counts for all JRSS / MONTH combinations (ONLY using data available in previous month!)
jrss.projection <- tbl_df(data) %>%
  filter(!is.na(OG.Start.floor)) %>%
  #filter(year(Close.floor) == 2015) %>% #in case we want to limit data size
  #positions created BEFORE the start of PREVIOUS MONTH (which positions w start date in March were created BEFORE/ON FEB 1st?)
  filter(CRE_T <= OG.Start.floor %m-% months(1)) %>%
  #which positions were still OPEN at the start of the PREVIOUS MONTH? (which positions w start date in March were still open on FEB 1st?)
  mutate(open = ifelse(is.na(WTHDRW_CLOS_T), T, 
                       WTHDRW_CLOS_T > OG.Start.floor %m-% months(1))) %>%
  filter(open) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, OG.Start.floor) %>%
  #now collapse by JRSS and get counts
  summarise(prj.tot = length(OPNSET_ID))

#get table of positions by MONTH THEY ARE SUPPOSED TO START
jrss.projection <- left_join(expanded, 
                             jrss.projection, 
                             by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'OG.Start.floor'))

#create the projection features!
jrss.projection <- tbl_df(jrss.projection) %>%
  mutate(prj.tot = replace(prj.tot, is.na(prj.tot), 0)) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(r3prj = rollapply(data = prj.tot, width = 3,FUN = mean,
                           align = 'right', fill = NA, na.rm = F),
         #these will create a cumulative sum of actual demand (sliding 12 month window)
         prj.12cs = cumul.yrsum(prj.tot))

#LET'S BRING IT TOGETHER!
setnames(jrss.projection, 'OG.Start.floor', 'ref.month')
setnames(jrss.expanded, 'Close.floor', 'ref.month')
jrss.tbl <- left_join(jrss.expanded, jrss.projection, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'ref.month'))

#create a few more features
jrss.tbl <- tbl_df(jrss.tbl) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(m.tot.dmd = prj.12cs/12,
         m.tot.act = tot.12cs/12,
         m.sub.act =  sub.12cs/12,
         tot.heat12 = ifelse(m.tot.dmd == 0, 0, prj.tot/m.tot.dmd),
         tot.heat2 = ifelse(r2tot == 0, 0, prj.tot/r2tot))

#create top 50 JRSS feature
top.jrss <- tbl_df(jrss.expanded) %>%
  #filter(ref.month == max(ref.month)) %>%
  filter(ref.month == floor_date(current.run, 'month')) %>%
  select(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, tot.12cs, sub.12cs) %>%
  #group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(tot.rank = row_number(desc(tot.12cs)),
         sub.rank = row_number(desc(sub.12cs)))

#set up H/M/L tiers for subk demand based on PAST 12 MONTHS
jrss.tiers <- top.jrss %>%
  mutate(demand.tier = ifelse(sub.rank <=30, 'HIGH',
                              ifelse(sub.rank >30 & sub.rank <= 80, 'MEDIUM',
                                     'LOW'))) %>%
  select(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, demand.tier)

#split and select TOTAL  
top.tot <- top.jrss %>%
  filter(tot.rank <=50) %>%
  select(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(jrss.tot = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - '))
#split and select SUB
top.sub <- top.jrss %>%
  filter(sub.rank <=50) %>%
  select(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
  mutate(jrss.sub = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - '))

#join back to single table
top.jrss <- full_join(top.tot, top.sub, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC'))
top.jrss <- full_join(top.jrss, jrss.tiers, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC'))

#map JRSS features back into POSITION DATA
df <- data

save(df, file = '30 day df pre filter.saved')

# 3E - TEST / TRAIN FILTER ------------------------------------------------
message('3E - TEST / TRAIN FILTER')
#need to split df by test/train to map in the last available JRSS values to prediction set
df <- tbl_df(df) %>%
  #exclude dates beyond the time period we're projecting
  filter(STRT_DT <= (current.run + days(30)),
         (WTHDRW_CLOS_T >= OG.Start.floor | is.na(WTHDRW_CLOS_T)),
         CRE_T <= OG.Start.floor) %>%
  #start date may be up to 30 days in advance, but take any positions that are still open
  mutate(type = as.factor(
    ifelse(filter30(STRT_DT) & is.na(WTHDRW_CLOS_T),
           'TEST',
           'TRAIN')))

#create JRSS counts from testing
upcoming.tbl <- tbl_df(df) %>%
  group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, OG.Start.floor) %>%
  summarise(upcoming.open = sum(WTHDRW_CLOS_T >= OG.Start.floor | is.na(WTHDRW_CLOS_T)))

df <- left_join(df, upcoming.tbl, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'OG.Start.floor'))

#create pos counts from data (how many pos per seats? how many csa candidates per position?)
post.count <- df %>%
  group_by(OPNSET_ID, csa.src) %>%
  summarise(post.count = length(OPNSET_POS_ID)) %>%
  mutate(csa.posts = csa.src/post.count) %>%
  select(-csa.src)

df <- left_join(df, post.count, by = 'OPNSET_ID')

#otherwise they will get all NA vals from numeric features (since we don't have any actual data for dates that haven't happened, duh)
testing <- filter(df, type == 'TEST')
train <- filter(df, type == 'TRAIN')
train <- left_join(train, week.expanded, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'Close.week'))
train <- left_join(train, jrss.tbl, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'OG.Start.floor' = 'ref.month'))
train <- left_join(train, month.expanded, by = c('JOB_ROL_TYP_DESC', 'OG.Start.floor'))

#skip <- T
if(skip) {
  #shift these new training positions to a temp df
  train.new <- train
  #load the training set USED IN PREVIOUS RUNS (this is the running training df)
  load(paste0('30 day train', last.run, '.saved'))
  
  #stack the new positions with features into master training set
  #gotta make sure the master training set is LOCKED before we start running (ie, need same features in old & current)
  train <- bind_rows(train, train.new)
}

#select last FULL week (don't include the current week)
latest.week <- week.expanded %>%
  filter(Close.week == floor_date(current.run, 'week') - weeks(1))
testing <- testing %>%
  mutate(dummy.week = floor_date(current.run, 'week') - weeks(1))
testing <- left_join(testing, latest.week, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'dummy.week' = 'Close.week'))
testing <- select(testing, -dummy.week)

#select last MONTH from JRSS.TBL
latest.month <- jrss.tbl %>%
  filter(ref.month == floor_date(current.run, 'month'))
testing <- testing %>%
  mutate(dummy.month = floor_date(current.run, 'month'))
testing <- left_join(testing, latest.month, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', 'dummy.month' = 'ref.month'))
testing <- select(testing, -dummy.month)

#select last MONTH from MONTH.EXPANDED
latest.jr <- month.expanded %>%
  filter(OG.Start.floor == floor_date(current.run, 'month'))
testing <- testing %>%
  mutate(dummy.month = floor_date(current.run, 'month'))
testing <- left_join(testing, latest.jr, by = c('JOB_ROL_TYP_DESC', 'dummy.month' = 'OG.Start.floor'))
testing <- select(testing, -dummy.month)

#restack for data type processing (we'll split them later)
df <- train %>%
  bind_rows(testing)

#create work city var
past.12 <- interval(floor_date(current.run, 'month') - months(12), floor_date(current.run, 'month'))
city <- tbl_df(df) %>%
  filter(OG.Start.floor %within% past.12) %>%
  group_by(WRK_CTY_NM) %>%
  summarise(count = length(unique(OPNSET_POS_ID))) %>%
  ungroup() %>%
  mutate(rank = row_number(desc(count)),
         top.city = TRUE) %>%
  filter(rank <= 30) %>%
  arrange(rank) %>%
  select(-count, -rank)

#create opp owner var
owner <- tbl_df(df) %>%
  filter(OG.Start.floor %within% past.12, !is.na(OWNER_NOTES_ID)) %>%
  group_by(OWNER_NOTES_ID) %>%
  summarise(count = length(unique(OPNSET_POS_ID))) %>%
  ungroup() %>%
  mutate(rank = row_number(desc(count)),
         top.owner = TRUE) %>%
  filter(rank <= 30) %>%
  arrange(rank) %>%
  select(-count, -rank)

#create GR delivery center var
gdc <- tbl_df(df) %>%
  filter(OG.Start.floor %within% past.12, !is.na(GBL_DEL_CTR_NM)) %>%
  group_by(GBL_DEL_CTR_NM) %>%
  summarise(count = length(unique(OPNSET_POS_ID))) %>%
  ungroup() %>%
  mutate(rank = row_number(desc(count)),
         top.gdc = TRUE) %>%
  filter(rank <= 30) %>%
  arrange(rank) %>%
  select(-count, -rank)




#map the top 50 tot / sub most common JRSS in past 12 months
df <- left_join(df, top.jrss, by = c('JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC'))

#map in top 30 cities with most requests from past 12 months
df <- merge(df, city, by = 'WRK_CTY_NM', all.x = T)

#map in top 30 cities with most requests from past 12 months
df <- merge(df, owner, by = 'OWNER_NOTES_ID', all.x = T)

#map in the top 30 GDC names
df <- merge(df, gdc, by = 'GBL_DEL_CTR_NM', all.x = T)

#replace NAs with 'OTHER'
df[c('jrss.tot', 'jrss.sub', 'top.city', 'top.owner')] <- lapply(df[c('jrss.tot', 'jrss.sub', 'top.city', 'top.owner')], 
                                                                 function(x) replace(x, is.na(x), 'OTHER'))


load('start time.saved')
proc.time() - start
save(df, file = paste0('30 day observation tbl ', current.run, '.saved'))

# 4A - PREP DATA FOR RANDOM FORESTS ------------------------------------------------------
message('4A - PREP DATA FOR RANDOM FORESTS')
df$Month <- with(df, lubridate::month(STRT_DT, label = T))
df$Year <- with(df, lubridate::year(STRT_DT))
df$GR.strt.mo <- with(df, lubridate::month(ONSIT_STRT_DT), label = T)
df$GR.srtr.yr <- with(df, lubridate::year(ONSIT_END_DT), label = T)
df$GR.duration <- with(df, as.numeric(ONSIT_END_DT - ONSIT_STRT_DT))
df$GR.duration <- ifelse(df$GR.duration < 1, NA, df$GR.duration)

facts <- c("OPNSET_POS_ID", "STAT_RESN_DESC", "INDSTR_NM", "CNTRCT_OWNG_ORG_NM", "OWNER_NOTES_ID", "DELG_NOTES_ID", "Unit",
           "PAY_TRVL_IND", "WRK_RMT_IND", "CNTRCT_TYP_DESC", "FULFILL_RISK_ID", "BND_LOW", "BND_HIGH", 'Month', 'Year',
           "SEC_CLRNCE_TYP_DESC", "OWNG_CNTRY_CD", "WRK_CNTRY_CD", "JOB_ROL_TYP_DESC", "SKLST_TYP_DESC", 
           "SET_TYP_CD", "WRK_CTY_NM", 'URG_PRIRTY_IND', 'PREF_FULFLMNT_CHNL_CD', 'NEED_SUB_IND', "needed30.days",
           'jrss.tot', 'jrss.sub', 'top.city', 'top.owner', 'demand.tier', 
           'top.gdc', 'NEED_CLNT_STE_IND', 'ACPT_SUB_IND')

#set any missing factor values to 'NONE' so we can run them thru RFs
df[facts] <- lapply(df[facts], function(x) {
  val <- as.character(x)
  vals <- ifelse(is.na(val), 'NONE', val)
  vals <- as.factor(vals)
  return(vals)
})

#use continuous variables + prob output from factor forest
num.inputvars <- c('STAT_RESN_DESC', "Lead.time.days", "record.age","r2tot", "r2sub", "tot.12cs", "sub.12cs","prj.tot", 
                   "r3prj", "prj.12cs", "m.tot.dmd", "m.tot.act", "m.sub.act", "tot.heat12", 
                   "tot.heat2", "project.duration", "jr.count", "actual.wk", "sub.actual.wk",
                   "tot.4wk", "sub.4wk", "tot.4lag", "sub.4lag", "tot.delta", "sub.delta", 'upcoming.open',
                   candidate.vars, 'post.count', 'csa.posts',
                   'GR.strt.mo', 'GR.srtr.yr', 'GR.duration')
num.inputvars <- num.inputvars[!num.inputvars %in% c("<NA>", 'Not Selected')]

#recode NA as -100 (so we don't throw out any records just bc of NAs)
df[setdiff(num.inputvars, 'STAT_RESN_DESC')] <- lapply(df[setdiff(num.inputvars, 'STAT_RESN_DESC')], function(x) {
  val <- ifelse(x < 0, NA, x)
  val <- ifelse(is.na(val), -100, val)
  return(val)
})

#split into training / testing & save
testing <- df[df$type == 'TEST',]
train <- df[df$type == 'TRAIN',]

save(testing, file = paste0('30 day testing', current.run, '.saved'))

save(train, file = paste0('30 day train', current.run, '.saved'))
#save last run date for using next refresh

# 4B - TRAIN FACTOR FOREST ------------------------------------------------
message('4B - TRAIN FACTOR FOREST ')
#grab position IDs and JRSS for mapping back into RF outputs
id <- data.frame(Position.ID = train$OPNSET_POS_ID,
                 GR.Position.ID = train$GR_OPNSET_POS_ID,
                 JR = train$JOB_ROL_TYP_DESC, 
                 SS = train$SKLST_TYP_DESC, 
                 country = train$WRK_CNTRY_CD,
                 Start.dt = train$OG.Start.floor)

#put the factor variable data into input table (remove id vars, we'll map these in later with 'id')
facts.input <- train[setdiff(facts, c('OPNSET_ID', 'OPNSET_POS_ID', 'JOB_ROL_TYP_DESC', 'SKLST_TYP_DESC', "OWNER_NOTES_ID", "DELG_NOTES_ID"))]

################ UH OH, RF CAN'T HANDLE VARIABLES WITH 53+ LEVEL. NEED TO DROP WORK CITY ;__;
#code to check if any variables have more than 53 levels
#unlist(lapply(facts.input, function(x) length(levels(x))))
#probably could set up automatic check that drops any vars that violate this....
facts.input$WRK_CTY_NM <- NULL

facts.input$OWNG_CNTRY_CD <- NULL

detach("package:dplyr", unload=TRUE)

#drop time sensitive values - THESE MAY BE BIASING THE TRAINING DATA ON INFO WE DON'T HAVE 1 MONTH IN ADVANCE
facts.input$PREF_FULFLMNT_CHNL_CD <- NULL
facts.input$NEED_SUB_IND <- NULL

t <- proc.time()
fact.forest <- randomForest(STAT_RESN_DESC ~ ., 
                            data = facts.input,
                            mtry = 5,
                            importance = T,
                            #proximity = T,
                            do.trace = 100,
                            ntree = 500,
                            nodesize = 100,
                            na.action = na.omit)

proc.time()-t
print(fact.forest)

varImpPlot(fact.forest)

#grab probability predictions
fact.pred <- predict(fact.forest, type="prob")[, 2]

fact.out <- data.frame(predicted = fact.forest$predicted, 
                       actual = fact.forest$y,
                       prob = fact.pred)

fact.out <- cbind(id, fact.out)

# 4c - TRAIN CONTINUOUS FOREST --------------------------------------------
message('4c - TRAIN CONTINUOUS FOREST')
#get the numeric vars
library(dplyr)
num.input <- train[num.inputvars] %>%
  mutate(fact.out = fact.out$prob)
detach("package:dplyr", unload=TRUE)
#include the factor forest probabilities as INPUTS to numeric forest


#run that shizznit
t <- proc.time()
num.forest <- randomForest(STAT_RESN_DESC ~ ., 
                           data = num.input,
                           mtry = 5,
                           importance = T,
                           #proximity = T,
                           do.trace = 100,
                           ntree = 500,
                           nodesize = 100,
                           na.action = na.omit)
proc.time()-t
print(num.forest)
varImpPlot(num.forest)


library(cowsay)
sink(paste0('global 30 day randomForest diagnostics ', '.txt'))
print(current.run)
cat(say('Clark is still a butt!', by='chicken', type = 'string'), sep = '\n')
print(fact.forest)
print(num.forest)
sink()

#training out: NUMERIC
num.pred <- predict(num.forest, type="prob")[, 2]

num.out <- data.frame(predicted = num.forest$predicted, 
                      actual = num.forest$y,
                      prob = num.pred)
num.out <- cbind(id, num.out)

#variable importance table (plot later)
n.imp <- as.data.frame(importance(num.forest))
n.imp$type <- 'NUMERIC'
f.imp <- as.data.frame(importance(fact.forest))
f.imp$type <- 'FACTOR'
importance.tbl <- rbind(n.imp, f.imp)
importance.tbl$var <- as.factor(rownames(importance.tbl))
rownames(importance.tbl) <- NULL
importance.tbl$var <- factor(importance.tbl$var, levels = importance.tbl$var[order(importance.tbl$MeanDecreaseAccuracy)])

# 5A - PREDICT ------------------------------------------------------------
message('5A - PREDICT')
#select factor variables for PREDICTION SET
test.facts <- testing[colnames(facts.input)]
#get FACTOR predictions
factor.predictions <- data.frame(pos.id = testing$OPNSET_POS_ID,
                                 GR.Position.ID = testing$GR_OPNSET_POS_ID,
                                 pred = predict(fact.forest, newdata = test.facts, type = 'response'),
                                 prob = predict(fact.forest, newdata = test.facts, type = 'prob')[,2])

#select numeric variables for PREDICTION SET
test.num <- testing[num.inputvars]
#include factor ouput
test.num$fact.out <- factor.predictions$prob
#get NUMERIC predictions
num.predictions <- data.frame(pos.id = testing$OPNSET_POS_ID,
                              GR.Position.ID = testing$GR_OPNSET_POS_ID,
                              pred = predict(num.forest, newdata = test.num, type = 'response'),
                              prob = predict(num.forest, newdata = test.num, type = 'prob')[,2])

test.id <- data.frame(Position.ID = testing$OPNSET_POS_ID, 
                      GR.Position.ID = testing$GR_OPNSET_POS_ID,
                      JR = testing$JOB_ROL_TYP_DESC, 
                      SS = testing$SKLST_TYP_DESC, 
                      city = testing$WRK_CTY_NM, 
                      country = testing$WRK_CNTRY_CD,
                      actual = testing$STAT_RESN_DESC)
#map eval outcomes
final.pred <- cbind(test.id, num.predictions)
save(final.pred, file = paste0('30 day prediction output ', current.run, '.saved'))

#plot importance chart at the end

# CLARK ERRORS OVERALL ------------------------------------------------------------

binary.code <- function(n, probs) {
  #sample from binomial distribution N times, assuming size of 100, and base probability from random forest output
  nums <- rbinom(n, 100, probs)/100
  #count where random sample is less than or equal to base prob
  check <- nums >= runif(n, 0 , 1)
  return(check)
}

#apply to every row
simulator <- sapply(final.pred$prob, function(x) binary.code(100, x))
simulator <- as.data.frame(t(simulator))
#join back with id vars
simulator <- cbind(final.pred[c('Position.ID', 'JR', 'SS', 'country')], simulator)

library(dplyr)
library(tidyr)
#convert to vertical structure
simulator <- simulator %>%
  gather(run, value, -Position.ID, - JR, -SS, -country)
#summarise by JRSS / simulation run
simulator <- tbl_df(simulator) %>%
  group_by(JR, SS, run, country) %>%
  summarise(count = sum(value))

#go back to wide structure
simulator <- tbl_df(simulator) %>%
  spread(run, count) %>%
  mutate(JRSS = paste(JR, SS, sep = ' - ')) %>%
  select(-JR, -SS)
setnames(simulator, 
         setdiff(colnames(simulator), c('JRSS', 'country')), 
         paste0('sim', 1:100))
simulator <- simulator[c('JRSS', 'country', paste0('sim', 1:100))]

#error bounds
bounded <- apply(simulator[3:102], 1, function(x) round(quantile(x, c(.25, .75)), 0))
bounded <- as.data.frame(t(bounded))

bounded <- cbind(simulator[c('JRSS', 'country')], bounded)

setnames(bounded, setdiff(colnames(bounded), c('JRSS', 'country')), c('Q1', 'Q3'))

total.summary <- bounded %>%
  group_by(JRSS) %>%
  summarise(Q1 = sum(Q1),
            Q3 = sum(Q3)) %>%
  mutate(country = 'Overall')

#function to expand some of the bounds
limiter <- function(x) {
  x <- as.integer(x)
  value <- ifelse(x <= 4, x + round(runif(1, 1, 4),0), x)
  return(value)
}
#only gets applied to the OVERALL estimates
#it would be a nightmare to do this for the countries separately and then have them align to the overall nums
#so just explain that country level estimates are more unstable and numbers may not align perfectly
#ugh
total.summary$Q3 <- apply(total.summary, 1, function(x) limiter(x[3]))

bounded <- rbind(bounded, total.summary)

run.this <- F
if(run.this) {
  #only for eval
  outcome <- tbl_df(testing) %>%
    group_by(JOB_ROL_TYP_DESC, SKLST_TYP_DESC) %>%
    summarise(count = sum(as.logical(STAT_RESN_DESC))) %>%
    mutate(JRSS = paste(JOB_ROL_TYP_DESC, SKLST_TYP_DESC, sep = ' - ')) %>%
    ungroup() %>%
    select(JRSS, count, -JOB_ROL_TYP_DESC, -SKLST_TYP_DESC)
  
  outcome <- left_join(outcome, bounded, by = 'JRSS')
  
  outcome$inbound <- with(outcome, count >= Q1 & count <= Q3)
  
  model.guess <- final.pred %>%
    group_by(JR, SS) %>%
    summarise(roundsum = round(sum(prob), 0)) %>%
    mutate(JRSS = paste(JR, SS, sep = ' - ')) %>%
    ungroup() %>%
    select(-JR, -SS)
  
  outcome <- left_join(outcome, model.guess, by = c('JRSS'))
  
  outcome.plot <- ggplot(outcome, aes(x = count, color = inbound)) +
    geom_abline(
      slope = 1,
      intercept = 0,
      linetype = 'dashed',
      color = 'gray30'
    ) +
    geom_errorbar(aes(ymax = high95, ymin = low5),
                  alpha = 1 / 4,
                  size = 1.5) +
    geom_point(
      aes(y = roundsum),
      shape = 1,
      alpha = 1 / 3,
      size = 4,
      stroke = 2,
      fill = 'white'
    ) +
    scale_x_continuous(
      limits = c(0, 18),
      breaks = seq(0, 20, by = 2),
      name = 'Actual Subk Demand',
      expand = c(0, 0)
    ) +
    scale_y_continuous(
      limits = c(0, 18),
      breaks = seq(0, 20, by = 2),
      name = 'Predicted Subk Range',
      expand = c(0, 0)
    ) +
    scale_color_discrete(name = "Within Predicted Range") +
    coord_flip() +
    theme_bw() +
    guides(colour = guide_legend(override.aes = list(alpha = 1))) +
    theme(
      panel.grid.major.x = element_blank(),
      panel.grid.major.y = element_blank(),
      strip.text.x = element_text(size = 10.5, face = "bold"),
      strip.text.y = element_text(size = 10.5, face = "bold"),
      axis.title.x = element_text(face = "bold", size = 14),
      axis.title.y = element_text(face = "bold", size = 14),
      axis.text.x = element_text(size = 10),
      axis.text.y = element_text(size = 10),
      plot.title = element_text(face = "bold", size = 24),
      legend.position = "bottom"
    )
  
  ggsave(
    plot = outcome.plot,
    paste0("Eval - JRSS prediction bounds July", current.run, '.png'),
    h = 200,
    w = 200,
    unit = "mm",
    type = "cairo-png",
    dpi = 300
  )
  
  outcome.plot
}

# EXCEL OUTPUT ------------------------------------------------------------
message('EXCEL OUTPUT')
#library(XLConnect)

#grab the vars we need for the DUMB REPORT
library(dplyr)

reporter <- data %>%
  mutate_at(vars(OPNSET_POS_ID, GR_OPNSET_POS_ID,
                 ONSIT_STRT_DT, STRT_DT,
                 ONSIT_END_DT, END_DT), 
            as.character) %>%
  filter(OPNSET_POS_ID %in% as.character(na.omit(unique(testing$OPNSET_POS_ID))) | 
           GR_OPNSET_POS_ID %in% as.character(unique(na.omit(testing$GR_OPNSET_POS_ID)))) %>%
  mutate(OPNSET_POS_ID = ifelse(is.na(OPNSET_POS_ID), GR_OPNSET_POS_ID, OPNSET_POS_ID),
         STRT_DT = ifelse(SET_TYP_CD == 'GR', ONSIT_STRT_DT, STRT_DT),
         END_DT = ifelse(SET_TYP_CD == 'GR', ONSIT_END_DT, END_DT)) %>%
  select(OPNSET_ID, OPNSET_POS_ID, Unit, CNTRCT_OWNG_ORG_NM, JOB_ROL_TYP_DESC, 
         SKLST_TYP_DESC, CRE_T, WRK_CTY_NM, WRK_CNTRY_CD,
         WTHDRW_CLOS_T, INDSTR_NM, OWNER_NOTES_ID, DELG_NOTES_ID, 
         WRK_RMT_IND, BND_LOW, BND_HIGH, STRT_DT, END_DT, project.duration, PAY_TRVL_IND, Unit,
         SET_TYP_CD, GBL_DEL_CTR_NM) %>%
  mutate(Open.Dummy = 'OPEN',
         Low.Band = BND_LOW, 
         High.Band = BND_HIGH, 
         Opp.Industry = INDSTR_NM, 
         Business.Unit = Unit,
         Pay.Travel.Expenses = PAY_TRVL_IND)

#merge with DUMB PREDICTION OUTPUTS
reporter <- left_join(num.predictions %>%
                        mutate(SET_TYP_CD = ifelse(pos.id == 'NONE', 'GR', 'MP'),
                               pos.id = ifelse(pos.id == 'NONE', 
                                               as.character(GR.Position.ID), 
                                               as.character(pos.id))) %>%
                        select(-GR.Position.ID), 
                      reporter, 
                      by = c('pos.id' = "OPNSET_POS_ID", 'SET_TYP_CD'))

#fix DUMB VARIABLE NAMES
setnames(reporter, 
         setdiff(colnames(reporter), c('Low.Band', 'High.Band', 
                                       'Opp.Industry', 'Business.Unit', 'Pay.Travel.Expenses',
                                       'SET_TYP_CD', 'GBL_DEL_CTR_NM')),
         c('Position.ID', 'Prediction', 'Probability', "Seat.ID", 'Unit', 'Sub.LOB','Job.Role', 
           'Skillset', 'Create.Date', 'Work.City', 'Work.Country', 'Close.Date',
           'Industry', 'Opp.Owner.ID', 'Delegate.Notes.ID',
           'Work.Remotely', 'Band-low', 'Band-high', 'Start.Date', 
           'End.Date', 'Engagement.Duration', 'Pay.Travel', 'Status'))
setnames(reporter,
         c('SET_TYP_CD', 'GBL_DEL_CTR_NM'),
         c('Position.Type', 'Global.Delivery.Center.'))

reporter$Probability.percentage <- with(reporter, paste0(round(Probability, 2)*100, '%'))
reporter$Subk.prob.flag <- with(reporter, ifelse(Probability >= .6, 'High Subk Likelihood',
                                                 ifelse(Probability < .6 & Probability > .3, 'Medium Subk Likelihood',
                                                        'Low Subk Likelihood')))
reporter <- reporter %>%
  mutate(JRSS = paste0(Job.Role, ' - ', Skillset))
reporter <- left_join(reporter, 
                      bounded %>% filter(country == 'Overall'), 
                      by = 'JRSS') %>%
  select(-Job.Role, -Skillset)
setnames(reporter, c('Q1','Q3'), c('Low.Estimate', 'High.Estimate'))

reporter <- left_join(reporter, 
                      bounded %>% filter(country != 'Overall'),
                      by = c('JRSS', 'Work.Country' = 'country'))
setnames(reporter, c('Q1','Q3'), c('Low.Estimate.(Country)', 'High.Estimate.(Country)'))

names(reporter) <- gsub(x = names(reporter),
                        pattern = "\\.",
                        replacement = " ")

save(reporter, file = '30 day reporter.saved')

#load workbook & figure out sheet names
setwd("~/Demand Forecasting III/Templates")

#rep.arch <- 'C:/Users/SCIP2/Documents/Demand Forecasting III/Output Reports'

rep.arch <- "C:/Users/SCIP2/Box Sync/Demand Forecasting (areyesm@us.ibm.com)/Country Reports"

cartographer <- as.character(sort(unique(df$WRK_CNTRY_CD)))
message(c('REPORTING ON THE FOLLOWING COUNTRIES:\n', paste0(cartographer, sep = '   ')))

reports <- paste0(rep.arch, '/', cartographer, '/', 
                  c(rep('30_day_', length(cartographer)), 
                    rep('60_day_', length(cartographer))), 
                  cartographer, '_.xlsx')

sink('filenames.txt')
cat(sort(reports), sep = '\n')
sink()

setwd("C:/Users/SCIP2/Box Sync/Demand Forecasting (areyesm@us.ibm.com)/Archive")
save.image("30 day ws.RData")

#TEMPORARY WD, CHANGE ONCE LOCATION IS STABLE
main.Dir <- 'C:/Users/SCIP2/Box Sync/Demand Forecasting (areyesm@us.ibm.com)/Country Reports'
#template directory:
temple.Dir <- 'C:/Users/SCIP2/Documents/Demand Forecasting III/Templates/Country Template/'

#loop over all countries to dump csv data
for(i in 1:length(cartographer)) {
  #verify folder exists for country
  ctry.dir <- file.path(main.Dir, cartographer[i])
  if(!dir.exists(ctry.dir)) {
    dir.create(ctry.dir)
    message(paste0(cartographer[i], ' directory created!'))
  }
  
  #take full REPORTER object and filter by each country
  rep.dump <- reporter %>%
    select(-`Opp Owner ID`, -`Low Estimate`, -`High Estimate`) %>%
    filter(`Work Country` == cartographer[i])
  
  #open the 30 day template and paste REPORTER into DATA sheet
  wb <- openxlsx::loadWorkbook(file = paste0(temple.Dir, '30_day_BLANK TEMPLATE_', cartographer[i], '.xlsx'))
  openxlsx::writeData(wb, "Data", 
                      rep.dump, 
                      startCol = 1, startRow = 1, rowNames = F)
  #move to country-specific folders & delete previous run
  #*********NOT SURE IF THIS IS THE RIGHT FOLDER
  setwd(ctry.dir)
  unlink(dir(path = getwd(), pattern =  '30_day_'))
  #save country report
  openxlsx::saveWorkbook(wb = wb, 
                         file = paste0('30_day_', cartographer[i],'_', '.xlsx'))
  
  message(paste0('Dumped ', cartographer[i],'!!!'))
}
rm(rep.dump)

send.it <- F
setwd('C:/Users/SCIP2/Documents/Demand Forecasting III/Templates')
sink(paste0('low_size_body','.txt'))
print(current.run)
print('clark is a SMELLLY butt')
for(i in 1:length(cartographer)) {
  ctry.dir <- file.path(main.Dir, cartographer[i])
  dump.size <- file.info(paste0(ctry.dir, '/', '30_day_', cartographer[i],'_', '.xlsx'))$size/1000
  
  if(dump.size < 100) {
    print(paste0(cartographer[i], ' file is very small (', dump.size,' kb)'))
    send.it <- T
  }
}
sink()

if(send.it){
  message('Sending Low File Size Warning!')
  #send email if small file size
  library(mailR, lib.loc = "C:/Program Files/R/Libraries")
  
  send.mail(from = "Demand_Forecast@scip.atl.dst.ibm.com",
            to = list('areyesm@us.ibm.com', 'clark.llamzon@us.ibm.com'),
            subject = "Demand Forecasting: Low File Size Warning",
            body = paste(getwd(), 'low_size_body.txt', sep = '/'),
            html = F,
            smtp = list(host.name = "scip.atl.dst.ibm.com", port = 465, ssl=FALSE, 
                        user.name = "Demand_Forecast@scip.atl.dst.ibm.com", passwd = "df2017"),
            authenticate = TRUE,
            send = TRUE)
}



setwd("C:/Users/SCIP2/Box Sync/Demand Forecasting (areyesm@us.ibm.com)/Archive")

#make pretty importance chart
library(ggplot2)

importance.plot <- ggplot(importance.tbl, aes(x = var, y = MeanDecreaseAccuracy, color = type))+
  geom_point(size = 4)+
  geom_point(size = 4, shape = 1, color = 'black')+
  geom_vline(xintercept = 0)+
  coord_flip()+
  theme_bw()+
  theme(panel.grid.major.x = element_blank(),
        panel.grid.major.y =   element_line(colour = "grey70", size=.7, linetype = 'dashed'),
        strip.text.x = element_text(size=10.5, face="bold"), 
        strip.text.y = element_text(size=10.5, face="bold"),
        axis.title.x=element_text(face="bold",size=14),
        axis.title.y=element_text(face="bold",size=14),
        axis.text.x=element_text(size=10),
        axis.text.y=element_text(size=10),
        plot.title=element_text(face="bold",size=24))

ggsave(plot= importance.plot,
       paste0("30 day Random Forest feature importance", '.png'),
       h=250,
       w=200,
       unit="mm",
       type="cairo-png",
       dpi=300)


message('30 DAY COMPLETED')