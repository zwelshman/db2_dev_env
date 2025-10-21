library(DBI)
library(odbc)

# Check available drivers
print("Available ODBC drivers:")
print(odbcListDrivers())

# Connect using the container's network hostname
con <- dbConnect(
  odbc::odbc(),
  Driver = "DB2",
  Database = "DEVDB",
  Hostname = "127.0.0.1",  # This is the docker-compose service name
  Port = 50000,
  UID = "db2inst1",
  PWD = "mypassword123",
  Protocol = "TCPIP"
)

print("Connected to DB2!")

# Query data
result <- dbGetQuery(con, "SELECT * FROM MYSCHEMA.EMPLOYEES")
print(result)

# Close connection
dbDisconnect(con)