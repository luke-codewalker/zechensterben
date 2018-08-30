# setup environment
rm(list=ls())
readRenviron(paste0(getwd(), "/.env"))

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

# load wikidata table into dataframe
data <- dbGetQuery(connection, "SELECT * FROM wikidata WHERE open_year IS NOT NULL AND (close_year IS NOT NULL OR is_active = True);")

# substitute close year with current year for active mines
data$close_year[data$is_active==TRUE] <- 2018

# how many are still active?
sum(as.integer(data$is_active==TRUE))

# descriptives for closing and opening years
(openStats <- summary(data$open_year))
(closeStats <- summary(data$close_year))

# count active shafts for each year and plot
year <- openStats["Min."]:closeStats["Max."]
num <- sapply(year, function(x) {
  v <- logical(nrow(data))
  for(i in 1:nrow(data)) {
    v[i] <- x > data$open_year[i] && x < data$close_year[i]
  }
  return(sum(as.integer(v)))
})

plot(num ~ year, type="l")

# close the connection
dbDisconnect(connection)
dbUnloadDriver(driver)