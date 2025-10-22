library(DBI)
library(odbc)
library(dplyr)
library(dbplyr)

# Connect using the container's network hostname
con <- dbConnect(
  odbc::odbc(),
  Driver = "DB2",
  Database = "DEVDB",
  Hostname = "db",  # This is the docker-compose service name
  Port = 50000,
  UID = "db2inst1",
  PWD = "mypassword123",
  Protocol = "TCPIP"
)

dbExecute(con, "SET SCHEMA sail")
# Reference remote tables with dbplyr
gp_event_reformatted <- tbl(con, "GP_EVENT_REFORMATTED")
gp_event_codes <- tbl(con, "GP_EVENT_CODES")

# Join the tables on EVENT_CD_ID (example of linking)
joined_data <- gp_event_reformatted %>%
  inner_join(gp_event_codes, by = "EVENT_CD_ID")

# Inspect generated SQL query (optional)
show_query(joined_data)

# To actually execute and collect results into R dataframe:
joined_data %>% collect() %>% head()

# Cleanup
# dbDisconnect(con)
