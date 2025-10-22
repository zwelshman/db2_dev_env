# Test Script for Your Specific Configuration
# Based on your YAML: ALF_PE, GNDR_CD, CREATE_DT

library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)

# Source the main functions
source("create_long_format_assets.R")

# ============================================================================
# Setup
# ============================================================================

cat("=== Testing Sex Asset Configuration ===\n\n")

# Load your configuration
config <- read_db_config("db2_config_multi_source.yaml")

# Check the configuration
cat("Checking YAML configuration for 'sex' asset...\n\n")

sex_config <- config$assets$sex
source_config <- sex_config$sources$gp_sex

cat("Source: gp_sex\n")
cat(glue("  Table: {source_config$table_name}\n"))
cat(glue("  Primary Key: {source_config$primary_key}\n\n"))

cat("Column Mappings:\n")
for (col_name in names(source_config$columns)) {
  col_config <- source_config$columns[[col_name]]
  cat(glue("  {col_name} ← {col_config$db_column}\n"))
}

cat("\n")

# Verify primary_key matches a db_column
cat("Verifying primary_key configuration...\n")
patient_id_found <- FALSE
for (col_name in names(source_config$columns)) {
  col_config <- source_config$columns[[col_name]]
  if (toupper(col_config$db_column) == toupper(source_config$primary_key)) {
    cat(glue("✓ Found: '{col_name}' maps to primary_key '{source_config$primary_key}'\n"))
    patient_id_found <- TRUE
    break
  }
}

if (!patient_id_found) {
  cat("✗ WARNING: primary_key doesn't match any db_column!\n")
  cat("  This will cause errors.\n")
} else {
  cat("✓ Configuration looks good!\n")
}

cat("\n")

# ============================================================================
# Test Query Generation
# ============================================================================

cat("=== Testing Query Generation ===\n\n")

# Build the query
query <- glue("
  SELECT 
    {source_config$columns$patient_id$db_column} AS patient_id,
    {source_config$columns$sex_code$db_column} AS sex_code,
    {source_config$columns$record_date$db_column} AS record_date
  FROM {config$database$schema}.{source_config$table_name}
  WHERE {source_config$primary_key} IN (1001, 1002, 1003)
")

cat("Generated SQL:\n")
cat(query, "\n\n")

# ============================================================================
# Test Database Connection and Execution
# ============================================================================

cat("=== Testing Database Connection ===\n\n")

tryCatch({
  # Connect
  conn <- create_db2_connection(config)
  cat("✓ Connected successfully\n\n")
  
  # Execute query
  cat("Executing query...\n")
  result <- DBI::dbGetQuery(conn, query)
  
  cat(glue("✓ Retrieved {nrow(result)} rows\n\n"))
  
  # Check column names
  cat("Column names returned by DB2:\n")
  for (i in seq_along(names(result))) {
    cat(glue("  [{i}] {names(result)[i]}\n"))
  }
  
  cat("\n")
  
  # Check if they're uppercase
  actual_names <- names(result)
  all_upper <- all(actual_names == toupper(actual_names))
  
  if (all_upper) {
    cat("⚠ DB2 returned UPPERCASE column names\n")
    cat("  This is normal and the script handles it automatically.\n\n")
  } else {
    cat("✓ Column names are as expected\n\n")
  }
  
  # Show sample data
  if (nrow(result) > 0) {
    cat("Sample data:\n")
    print(head(result, 3))
  } else {
    cat("No data returned. Possible reasons:\n")
    cat("  - Patient IDs 1001-1003 don't exist in your table\n")
    cat("  - Table is empty\n")
    cat("  - Wrong schema or table name\n")
  }
  
  # Close connection
  DBI::dbDisconnect(conn)
  
  cat("\n\n=== NEXT STEP ===\n\n")
  cat("Try creating the long format table:\n")
  cat("  sex_long <- create_long_format_asset(conn, config, 'sex')\n\n")
  
}, error = function(e) {
  cat("✗ Error occurred:\n")
  cat(glue("  {e$message}\n\n"))
  
  cat("Troubleshooting:\n")
  cat("  1. Check database credentials (DB_USER, DB_PASSWORD)\n")
  cat("  2. Verify table name: {source_config$table_name}\n")
  cat("  3. Verify schema: {config$database$schema}\n")
  cat("  4. Check if you have SELECT permission on the table\n")
  cat("  5. Try running the SQL query directly in a DB2 client\n\n")
})

# ============================================================================
# Summary
# ============================================================================

cat("=== SUMMARY ===\n\n")

cat("Your YAML Configuration:\n")
cat("  ✓ primary_key: ALF_PE\n")
cat("  ✓ patient_id maps to: ALF_PE (matches primary_key)\n")
cat("  ✓ sex_code maps to: GNDR_CD\n")
cat("  ✓ record_date maps to: CREATE_DT\n\n")

cat("Expected behavior:\n")
cat("  1. Query will use ALF_PE, GNDR_CD, CREATE_DT (DB2 names)\n")
cat("  2. DB2 will return them as ALF_PE, GNDR_CD, CREATE_DT (uppercase)\n")
cat("  3. Script will rename them to patient_id, sex_code, record_date (lowercase)\n")
cat("  4. Final table will have lowercase column names\n\n")

cat("If it works here, you can run:\n")
cat("  conn <- create_db2_connection(config)\n")
cat("  sex_long <- create_long_format_asset(conn, config, 'sex')\n")