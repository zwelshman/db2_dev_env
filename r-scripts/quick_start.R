# Quick Start: Create Long Format Asset Tables
# Simple template for creating long format tables from DB2
#install.packages(c('tidyverse','glue'))

library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)

# Source the main functions 

source("read_db2_config_multi_source.R")  # If you need multi-source functions
source("create_long_format_assets.R")     # Main long format functions

# ============================================================================
# SETUP
# ============================================================================

# Set your database credentials
# Sys.setenv(DB_USER = "db2inst1")
# Sys.setenv(DB_PASSWORD = "mypassword123")

# Load configuration
config <- read_db_config("db2_config_multi_source.yaml")

# Connect to database
#conn <- create_db2_connection(config)

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

# ============================================================================
# OPTION 1: Create Single Asset Long Format Table
# ============================================================================

# Example: Create ethnicity long format table
# ethnicity_long <- create_long_format_asset(
#   conn = conn,
#   config = config,
#   asset_name = "ethnicity",
#   patient_ids = NULL  # NULL = all patients, or specify: c(1001, 1002, 1003)
# )

sex_long <- create_long_format_asset(
  conn = conn,
  config = config,
  asset_name = "sex",
  patient_ids = NULL  # NULL = all patients, or specify: c(1001, 1002, 1003)
)

# View first few rows
head(sex_long)

# Get summary statistics
summarize_long_format_table(sex_long, "sex")

# Check for conflicts
conflicts <- check_conflicts(sex_long, "sex", "sex_code")

# Export to CSV
export_asset_table(sex_long, "ethnicity", format = "csv")

# ============================================================================
# OPTION 2: Create All Asset Tables at Once
# ============================================================================

# Create long format tables for all assets
asset_tables <- create_all_asset_tables(
  conn = conn,
  config = config,
  patient_ids = NULL,  # NULL = all patients
  assets = c("date_of_birth", "sex", "ethnicity", "lsoa")
)

# Access individual tables
dob_long <- asset_tables$date_of_birth
sex_long <- asset_tables$sex
ethnicity_long <- asset_tables$ethnicity
lsoa_long <- asset_tables$lsoa

# Export all tables
export_all_asset_tables(asset_tables, format = "csv")

# ============================================================================
# OPTION 3: Full Pipeline with Analysis
# ============================================================================

# Run complete pipeline
results <- create_asset_pipeline(
  config_path = "db2_config_multi_source.yaml",
  patient_ids = NULL,  # All patients
  assets = c("date_of_birth", "sex", "ethnicity", "lsoa"),
  output_dir = "/mnt/user-data/outputs"
)

# Results contain:
# - asset_tables: List of long format tables
# - summaries: Summary statistics for each asset
# - conflicts: Conflict analysis for each asset
# - exported_files: Paths to exported CSV files

# ============================================================================
# WORKING WITH LONG FORMAT TABLES
# ============================================================================

# 1. Get one row per patient (highest priority source)
ethnicity_resolved <- get_highest_priority_per_patient(ethnicity_long)

# 2. Filter to specific sources
ethnicity_self_reported <- ethnicity_long %>%
  filter(source_table == "self_reported_ethnicity")

# 3. Compare sources
ethnicity_comparison <- ethnicity_long %>%
  select(patient_id, source_table, ethnicity_code, ethnicity_category) %>%
  pivot_wider(
    names_from = source_table,
    values_from = c(ethnicity_code, ethnicity_category)
  )

# 4. Get patients with multiple sources
multi_source_patients <- ethnicity_long %>%
  group_by(patient_id) %>%
  filter(n() > 1) %>%
  ungroup()

# 5. Find specific conflicts
conflicting_patients <- ethnicity_long %>%
  group_by(patient_id) %>%
  filter(n_distinct(ethnicity_code) > 1) %>%
  arrange(patient_id, source_priority)

# ============================================================================
# EXAMPLE: Custom Analysis
# ============================================================================

# Example: Which source has highest coverage for ethnicity?
coverage_analysis <- ethnicity_long %>%
  group_by(source_table, source_priority, source_coverage) %>%
  summarise(
    n_patients = n_distinct(patient_id),
    .groups = "drop"
  ) %>%
  arrange(source_priority)

print(coverage_analysis)

# Example: How many patients have self-reported ethnicity?
self_reported_count <- ethnicity_long %>%
  filter(source_table == "self_reported_ethnicity") %>%
  n_distinct(patient_id)

cat(glue("Patients with self-reported ethnicity: {self_reported_count}\n"))

# Example: Distribution of ethnicity by source quality
quality_distribution <- ethnicity_long %>%
  group_by(source_quality, ethnicity_category) %>%
  summarise(n = n(), .groups = "drop") %>%
  arrange(source_quality, desc(n))

print(quality_distribution)

# ============================================================================
# JOINING ASSETS
# ============================================================================

# Create resolved (one row per patient) versions of each asset
dob_resolved <- get_highest_priority_per_patient(dob_long)
sex_resolved <- get_highest_priority_per_patient(sex_long)
ethnicity_resolved <- get_highest_priority_per_patient(ethnicity_long)
lsoa_resolved <- get_highest_priority_per_patient(lsoa_long)

# Join all assets together
patient_demographics <- dob_resolved %>%
  select(patient_id, date_of_birth, year_of_birth, dob_source = source_table) %>%
  left_join(
    sex_resolved %>% select(patient_id, sex_code, sex_source = source_table),
    by = "patient_id"
  ) %>%
  left_join(
    ethnicity_resolved %>% select(patient_id, ethnicity_code, ethnicity_category, 
                                  ethnicity_source = source_table),
    by = "patient_id"
  ) %>%
  left_join(
    lsoa_resolved %>% select(patient_id, lsoa_code, imd_decile, 
                             lsoa_source = source_table),
    by = "patient_id"
  )

# Export combined demographics
write.csv(patient_demographics, 
          "/mnt/user-data/outputs/patient_demographics_combined.csv",
          row.names = FALSE)

# ============================================================================
# CLEANUP
# ============================================================================

# Close database connection when done
DBI::dbDisconnect(conn)

cat("\n✓ All done!\n")
cat("Check /mnt/user-data/outputs/ for exported files.\n")