# setup environment
rm(list=ls())
readRenviron(paste0(getwd(), "/.env"))

# load (or install if necessary) postgreSQL package
packages <- c("RPostgreSQL", "leaflet")
for (package in packages) {
  if(!require(package, quietly=TRUE)) {
    install.packages(package)
    library(package, character.only = TRUE)
  }
}

# connect to postgreSQL
connection <- dbDriver("PostgreSQL") %>% 
  dbConnect(.,
           host = "localhost",
           port = 5432,
           dbname = Sys.getenv("DB_NAME"),
           user = Sys.getenv("DB_USER"),
           password = Sys.getenv("DB_PASSWORD")
           )

# load wikidata table into dataframe
df <- dbGetQuery(connection, "SELECT * FROM wikidata WHERE open_year IS NOT NULL AND (close_year IS NOT NULL OR is_active = True);")

# substitute close year with current year for active mines
df$close_year[df$is_active==TRUE] <- 2018

# how many are still active?
sum(as.integer(df$is_active==TRUE))

# descriptives for closing and opening years
(openStats <- summary(df$open_year))
(closeStats <- summary(df$close_year))

# count active shafts for each year and plot
year <- openStats["Min."]:closeStats["Max."]
num <- sapply(year, function(x) {
  v <- logical(nrow(df))
  for(i in 1:nrow(df)) {
    v[i] <- x > df$open_year[i] && x < df$close_year[i]
  }
  return(sum(as.integer(v)))
})

plot(num ~ year, type="l")

# plot locations
map <- leaflet(df) %>%
  addTiles() %>%
  addCircleMarkers(lng=~lon, lat=~lat, popup=~name, clusterOptions = markerClusterOptions())
map

# close the connection
dbDisconnect(connection)
dbUnloadDriver(driver)