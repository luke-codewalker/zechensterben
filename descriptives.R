# setup environment
rm(list=ls())
readRenviron(paste0(getwd(), "/.Renviron"))

# load (or install if necessary) postgreSQL package
if(!require("RPostgreSQL", quietly=TRUE)) {
  install.packages("RPostgreSQL")
  library(RPostgreSQL)
}

# connect to postgreSQL
driver <- dbDriver("PostgreSQL")
connection <- dbConnect(driver,
                       host = "localhost",
                       port = 5432,
                       dbname = Sys.getenv("DB_NAME"),
                       user = Sys.getenv("DB_USER"),
                       password = Sys.getenv("DB_PASSWORD")
                       )
