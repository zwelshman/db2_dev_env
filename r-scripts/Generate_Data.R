library(odbc)
library(DBI)
library(dplyr)
library(lubridate)
library(purrr)

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

print("Connected to DB2!")

set.seed(42)
N_PATIENTS <- 10000
BATCH_SIZE <- 1000
today <- Sys.Date()

# Helper: generate random dates in range
random_dates <- function(n, start_date = as.Date("1940-01-01"), end_date = Sys.Date()) {
  start_num <- as.numeric(start_date)
  end_num <- as.numeric(end_date)
  as.Date(sample(start_num:end_num, n, replace = TRUE), origin = "1970-01-01")
}

# Helper: batch insert into DB
batch_insert <- function(con, table_name, df, batch_size = BATCH_SIZE) {
  cols <- colnames(df)
  n <- nrow(df)
  for (i in seq(1, n, by = batch_size)) {
    batch <- df[i:min(i + batch_size - 1, n),]
    values <- batch %>%
      mutate(across(where(is.character), ~ dbQuoteString(con, .))) %>%
      mutate(across(where(is.Date), ~ dbQuoteString(con, as.character(.)))) %>%
      pmap(function(...) paste0("(", paste(c(...), collapse = ","), ")")) %>%
      paste(collapse = ",\n")
    sql <- paste0("INSERT INTO sail.", table_name, " (", paste(cols, collapse = ","), ") VALUES\n", values, ";")
    dbExecute(con, sql)
  }
}

# PATIENT_ALF_CLEANSED
patients <- tibble(
  WOB = random_dates(N_PATIENTS, as.Date("1930-01-01"), as.Date("2025-01-01")),
  PRAC_CD_PE = sample(100:999, N_PATIENTS, replace = TRUE),
  DELTA_KEY = 'N',
  LOCAL_NUM_PE = 100000:(100000 + N_PATIENTS - 1),
  ALF_STS_CD = sample(c("ACTIVE", "INACTIVE"), N_PATIENTS, replace = TRUE, prob = c(0.9, 0.1)),
  ALF_PE = 1:N_PATIENTS,
  ALF_MTCH_PCT = round(runif(N_PATIENTS, 95, 100), 2),
  LSOA_CD = paste0("LSOA", sample(1000:9999, N_PATIENTS, replace = TRUE)),
  BATCH_NUM = 1,
  PROCESS_DT = today,
  CREATE_DT = today,
  AVAIL_FROM_DT = today,
  OPT_OUT_FLG = sample(c('Y','N'), N_PATIENTS, replace = TRUE, prob = c(0.02, 0.98)),
  SOURCE_EXTRACT = 1,
  GNDR_CD = sample(c('M','F'), N_PATIENTS, replace = TRUE),
  REG_CAT_CD = sample(c("REG_A", "REG_B", "REG_C"), N_PATIENTS, replace = TRUE)
)

batch_insert(con, "PATIENT_ALF_CLEANSED", patients)

# GP_EVENT_CODES
gp_event_codes <- tibble(
  EVENT_CD_ID = 1:500,
  EVENT_CD = sample(100:999, 500, replace = TRUE),
  IS_READ_V2 = sample(0:1, 500, replace = TRUE),
  IS_READ_V3 = sample(0:1, 500, replace = TRUE),
  IS_VALID_CODE = sample(c(0,1), 500, replace = TRUE, prob = c(0.1,0.9)),
  DESCRIPTION = replicate(500, paste(sample(letters, 5, TRUE), collapse = "")),
  EVENT_TYPE = sample(c("CONSULT", "TEST", "DIAGNOSIS", "TREATMENT"), 500, replace = TRUE),
  HIERARCHY_LEVEL_1 = sample(c("Condition", "Observation", "Procedure"), 500, replace = TRUE),
  HIERARCHY_LEVEL_1_DESC = replicate(500, paste(sample(letters, 7, TRUE), collapse = "")),
  HIERARCHY_LEVEL_2 = paste0("L2_", 1:500),
  HIERARCHY_LEVEL_2_DESC = replicate(500, paste(sample(letters, 7, TRUE), collapse = "")),
  HIERARCHY_LEVEL_3 = paste0("L3_", 1:500),
  HIERARHCY_LEVEL_3_DESC = replicate(500, paste(sample(letters, 7, TRUE), collapse = ""))
)

batch_insert(con, "GP_EVENT_CODES", gp_event_codes)

# GP_EVENT_REFORMATTED
n_events <- N_PATIENTS * 3 # average 3 events per patient approx
gp_event_ref <- tibble(
  ALF_E = sample(patients$ALF_PE, n_events, replace = TRUE),
  ALF_STS_CD = "ACTIVE",
  ALF_MTCH_PCT = round(runif(n_events, 95, 100), 2),
  PRAC_CD_E = sample(100:999, n_events, replace = TRUE),
  EVENT_CD_ID = sample(gp_event_codes$EVENT_CD_ID, n_events, replace = TRUE),
  EVENT_VAL = round(runif(n_events, 0, 200), 2),
  EVENT_DT = random_dates(n_events, Sys.Date() - years(2), Sys.Date()),
  EVENT_YR = sample(2023:2024, n_events, replace = TRUE)
)

batch_insert(con, "GP_EVENT_REFORMATTED", gp_event_ref)

# GP_EVENT_CLEANSED
gp_event_cleansed <- tibble(
  PRAC_CD_PE = sample(100:999, n_events, replace = TRUE),
  LOCAL_NUM_PE = sample(patients$LOCAL_NUM_PE, n_events, replace = TRUE),
  EVENT_CD_VRS = sample(c("V2", "V3"), n_events, replace = TRUE),
  EVENT_CD = sample(as.character(gp_event_codes$EVENT_CD), n_events, replace = TRUE),
  EVENT_VAL = gp_event_ref$EVENT_VAL,
  EVENT_DT = gp_event_ref$EVENT_DT,
  EPISODE = paste0("EP", sample(1000:9999, n_events, replace = TRUE)),
  SEQUENCE = 1:n_events,
  DELTA_KEY = "N",
  BATCH_NUM = 1,
  EVENT_YR = gp_event_ref$EVENT_YR,
  CREATE_DT = today,
  AVAIL_FROM_DT = today,
  SOURCE_EXTRACT = 1
)
batch_insert(con, "GP_EVENT_CLEANSED", gp_event_cleansed)

# WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN & WLGP_CLEANED_GP_REG_MEDIAN
# Using same data for both tables (as per original script)
wlgp_reg <- tibble(
  ALF_PE = patients$ALF_PE,
  AVAILABLE_FROM = format(Sys.time() - runif(N_PATIENTS, 0, 1e7), "%Y-%m-%d %H:%M:%S"),
  END_DATE = Sys.Date() + sample(30:365, N_PATIENTS, replace = TRUE),
  GP_DATA_FLAG = sample(0:1, N_PATIENTS, replace = TRUE, prob = c(0.1, 0.9)),
  PRAC_CD_PE = patients$PRAC_CD_PE,
  START_DATE = Sys.Date() - sample(1000:2000, N_PATIENTS, replace = TRUE)
)

batch_insert(con, "WLGP_CLEANED_GP_REG_BY_PRACINCLUNONSAIL_MEDIAN", wlgp_reg)
batch_insert(con, "WLGP_CLEANED_GP_REG_MEDIAN", wlgp_reg)

# Disconnect
dbDisconnect(con)
