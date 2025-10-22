library(DBI)
library(odbc)

# Connect using the container's network hostname
conn <- dbConnect(
  odbc::odbc(),
  Driver = "DB2",
  Database = "DEVDB",
  Hostname = "db",  # This is the docker-compose service name
  Port = 50000,
  UID = "db2inst1",
  PWD = "mypassword123",
  Protocol = "TCPIP"
)

print("Connected to DB2!")
