# R Script for Creating Long Format Asset Tables
# Each asset table contains data from ALL source tables in long format

library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)
library(tidyr)

# ============================================================================
# 1. Configuration and Connection
# ============================================================================

read_db_config <- function(config_path = "db2_config_multi_source.yaml") {
  config <- yaml::read_yaml(config_path)
  return(config)
}

create_db2_connection <- function(config) {
  # Get credentials from environment variables
  db_user <- Sys.getenv("DB_USER")
  db_password <- Sys.getenv("DB_PASSWORD")
  
  if (db_user == "" || db_password == "") {
    stop("Database credentials not found. Set DB_USER and DB_PASSWORD environment variables.")
  }
  
  # Use config settings with Docker-compatible parameter names
  conn <- DBI::dbConnect(
    odbc::odbc(),
    Driver = config$database$driver,           # "DB2"
    Database = config$database$database_name,  # "DEVDB"
    Hostname = config$database$hostname,       # "db"
    Port = config$database$port,               # 50000
    UID = db_user,
    PWD = db_password,
    Protocol = config$database$protocol        # "TCPIP"
  )
  
  return(conn)
}

# ============================================================================
# 2. Core Function: Create Long Format Asset Table
# ============================================================================

standardize_patient_id_column <- function(df, standard_name = "patient_id") {
  # Rename the first column (patient ID) to a standard name
  # This makes downstream processing easier
  
  if (ncol(df) == 0) return(df)
  
  first_col <- names(df)[1]
  
  if (first_col != standard_name) {
    df <- df %>%
      rename(!!standard_name := all_of(first_col))
  }
  
  return(df)
}

create_long_format_asset <- function(conn, config, asset_name, 
                                     patient_ids = NULL,
                                     include_sources = NULL,
                                     standardize_patient_id = TRUE) {
  # Create a long format table for an asset from ALL source tables
  # 
  # Returns: A data frame where each row is one source's data for a patient
  # Columns: patient_id, source_table, source_priority, [asset columns...]
  
  asset_config <- config$assets[[asset_name]]
  
  if (is.null(asset_config)) {
    stop(glue("Asset '{asset_name}' not found in configuration"))
  }
  
  # Get all source tables or specified subset
  if (is.null(include_sources)) {
    source_names <- names(asset_config$sources)
  } else {
    source_names <- include_sources
  }
  
  cat(glue("\n=== Creating long format table for: {asset_name} ===\n"))
  cat(glue("Including {length(source_names)} source tables\n\n"))
  
  # Collect data from each source
  all_source_data <- list()
  
  for (source_name in source_names) {
    cat(glue("Extracting from: {source_name}...\n"))
    
    source_config <- asset_config$sources[[source_name]]
    
    # Build query for this source
    query <- build_source_query(config, asset_name, source_name, 
                                patient_ids)
    
    # Debug: print query
    if (getOption("debug_queries", FALSE)) {
      cat("Generated query:\n")
      cat(query, "\n\n")
    }
    
    # Execute query
    tryCatch({
      source_data <- DBI::dbGetQuery(conn, query)
      
      cat(glue("  ✓ Retrieved {nrow(source_data)} rows\n"))
      cat(glue("  Columns: {paste(names(source_data), collapse=', ')}\n"))
      
      # Ensure column names match expected R names from YAML
      # DB2 sometimes returns uppercase column names even with aliases
      expected_names <- names(source_config$columns)
      actual_names <- names(source_data)
      
      # Create mapping from uppercase to expected names
      name_mapping <- setNames(expected_names, toupper(expected_names))
      
      # Rename columns to match YAML specification
      for (i in seq_along(actual_names)) {
        actual_upper <- toupper(actual_names[i])
        if (actual_upper %in% names(name_mapping)) {
          names(source_data)[i] <- name_mapping[[actual_upper]]
        }
      }
      
      cat(glue("  Renamed to: {paste(names(source_data), collapse=', ')}\n"))
      
      # Add source metadata
      source_data <- source_data %>%
        mutate(
          source_table = source_name,
          source_db_table = source_config$table_name,
          source_priority = ifelse(is.null(source_config$priority), NA, source_config$priority)
        )
      
      # Add optional metadata fields if they exist
      if (!is.null(source_config$data_quality)) {
        source_data <- source_data %>%
          mutate(source_quality = source_config$data_quality)
      }
      
      if (!is.null(source_config$coverage)) {
        source_data <- source_data %>%
          mutate(source_coverage = source_config$coverage)
      }
      
      if (!is.null(source_config$last_updated)) {
        source_data <- source_data %>%
          mutate(source_last_updated = source_config$last_updated)
      }
      
      all_source_data[[source_name]] <- source_data
      
      cat(glue("  ✓ Final columns: {paste(names(source_data), collapse=', ')}\n\n"))
      
    }, error = function(e) {
      warning(glue("  ✗ Failed to retrieve from {source_name}: {e$message}"))
    })
  }
  
  # Combine all sources into long format
  if (length(all_source_data) == 0) {
    stop("No data retrieved from any source")
  }
  
  long_format_table <- bind_rows(all_source_data)
  
  # Get patient ID column name from YAML config
  # Use the R column name (the key in the columns list) that corresponds to primary_key
  first_source <- source_names[1]
  first_source_config <- asset_config$sources[[first_source]]
  
  # Find which R column name maps to the primary key
  patient_id_col <- NULL
  for (col_name in names(first_source_config$columns)) {
    col_config <- first_source_config$columns[[col_name]]
    # Case-insensitive comparison
    if (toupper(col_config$db_column) == toupper(first_source_config$primary_key)) {
      patient_id_col <- col_name
      break
    }
  }
  
  # Fallback: if not found, look for 'patient_id' column (case-insensitive)
  if (is.null(patient_id_col)) {
    available_cols <- names(long_format_table)
    patient_id_matches <- available_cols[toupper(available_cols) == "PATIENT_ID"]
    if (length(patient_id_matches) > 0) {
      patient_id_col <- patient_id_matches[1]
      warning(glue("Could not find patient ID column from primary_key in YAML. Using column: {patient_id_col}"))
    } else {
      # Last resort: use first column
      patient_id_col <- available_cols[1]
      warning(glue("Could not find patient ID column. Using first column: {patient_id_col}"))
    }
  }
  
  # Verify the column exists
  if (!patient_id_col %in% names(long_format_table)) {
    stop(glue("Patient ID column '{patient_id_col}' not found in data. Available columns: {paste(names(long_format_table), collapse=', ')}"))
  }
  
  source_info_cols <- c("source_table", "source_db_table", "source_priority", 
                        "source_quality", "source_coverage", "source_last_updated")
  
  # Find which source info columns actually exist
  existing_source_cols <- intersect(source_info_cols, names(long_format_table))
  
  # Data columns are everything else
  id_and_source_cols <- c(patient_id_col, existing_source_cols)
  data_cols <- setdiff(names(long_format_table), id_and_source_cols)
  
  # Reorder columns
  long_format_table <- long_format_table %>%
    select(all_of(patient_id_col), all_of(existing_source_cols), all_of(data_cols)) %>%
    arrange(across(all_of(patient_id_col)), source_priority)
  
  # Standardize patient ID column name if requested
  if (standardize_patient_id) {
    original_col <- patient_id_col
    long_format_table <- standardize_patient_id_column(long_format_table, "patient_id")
    patient_id_col <- "patient_id"
    if (original_col != "patient_id") {
      cat(glue("  Standardized '{original_col}' → 'patient_id'\n"))
    }
  }
  
  cat(glue("\n✓ Long format table created: {nrow(long_format_table)} rows, {ncol(long_format_table)} columns\n"))
  cat(glue("  Unique patients: {n_distinct(long_format_table[[patient_id_col]])}\n"))
  cat(glue("  Sources included: {length(source_names)}\n\n"))
  
  return(long_format_table)
}

# ============================================================================
# 3. Query Building Helper
# ============================================================================

build_source_query <- function(config, asset_name, source_name, 
                               patient_ids = NULL) {
  # Build query for a specific source table
  
  schema <- config$database$schema
  source_config <- config$assets[[asset_name]]$sources[[source_name]]
  
  # Get column mappings
  columns <- source_config$columns
  db_columns <- sapply(columns, function(x) x$db_column)
  
  # Build SELECT clause with aliases
  select_list <- paste(
    glue("{db_columns} AS {names(db_columns)}"),
    collapse = ",\n      "
  )
  
  # Build query
  query <- glue("
    SELECT 
      {select_list}
    FROM {schema}.{source_config$table_name}
  ")
  
  # Add WHERE clause for patient IDs if provided
  if (!is.null(patient_ids)) {
    pk <- source_config$primary_key
    ids_string <- paste(patient_ids, collapse = ", ")
    query <- paste(query, glue("WHERE {pk} IN ({ids_string})"))
  }
  
  return(query)
}

# ============================================================================
# 4. Create All Asset Tables
# ============================================================================

create_all_asset_tables <- function(conn, config, patient_ids = NULL,
                                    assets = NULL) {
  # Create long format tables for multiple assets
  
  if (is.null(assets)) {
    assets <- names(config$assets)
  }
  
  asset_tables <- list()
  
  for (asset_name in assets) {
    cat(glue("\n{'='*60}\n"))
    
    asset_table <- create_long_format_asset(
      conn, config, asset_name, patient_ids
    )
    
    asset_tables[[asset_name]] <- asset_table
  }
  
  return(asset_tables)
}

# ============================================================================
# 5. Analysis Functions for Long Format Tables
# ============================================================================

summarize_long_format_table <- function(long_table, asset_name) {
  # Provide summary statistics for a long format asset table
  
  # Get the patient ID column name (first column)
  patient_id_col <- names(long_table)[1]
  
  cat(glue("\n=== Summary: {asset_name} ===\n\n"))
  
  # Overall statistics
  cat("Overall Statistics:\n")
  cat(glue("  Total rows: {nrow(long_table)}\n"))
  cat(glue("  Unique patients: {n_distinct(long_table[[patient_id_col]])}\n"))
  cat(glue("  Number of sources: {n_distinct(long_table$source_table)}\n\n"))
  
  # Per-source statistics
  cat("Per-Source Statistics:\n")
  
  # Build group_by columns dynamically based on what exists
  group_cols <- c("source_table", "source_priority")
  if ("source_quality" %in% names(long_table)) {
    group_cols <- c(group_cols, "source_quality")
  }
  if ("source_coverage" %in% names(long_table)) {
    group_cols <- c(group_cols, "source_coverage")
  }
  
  source_summary <- long_table %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(
      n_patients = n_distinct(.data[[patient_id_col]]),
      n_rows = n(),
      .groups = "drop"
    ) %>%
    arrange(source_priority)
  
  print(source_summary)
  
  # Patient coverage across sources
  cat("\nPatient Coverage Across Sources:\n")
  coverage <- long_table %>%
    group_by(.data[[patient_id_col]]) %>%
    summarise(
      n_sources = n_distinct(source_table),
      sources = paste(source_table, collapse = ", "),
      .groups = "drop"
    ) %>%
    count(n_sources, name = "n_patients")
  
  print(coverage)
  
  return(list(
    source_summary = source_summary,
    coverage = coverage
  ))
}

check_conflicts <- function(long_table, asset_name, key_column) {
  # Check for conflicts in data values across sources
  # key_column: the main data column to check (e.g., "ethnicity_code", "sex_code")
  
  # Get the patient ID column name (first column)
  patient_id_col <- names(long_table)[1]
  
  cat(glue("\n=== Conflict Analysis: {asset_name} ===\n\n"))
  
  # Find patients with data from multiple sources
  multi_source_patients <- long_table %>%
    group_by(.data[[patient_id_col]]) %>%
    filter(n_distinct(source_table) > 1) %>%
    ungroup()
  
  if (nrow(multi_source_patients) == 0) {
    cat("No patients found with data from multiple sources.\n")
    return(NULL)
  }
  
  cat(glue("Patients with multiple sources: {n_distinct(multi_source_patients[[patient_id_col]])}\n\n"))
  
  # Check for actual conflicts (different values)
  conflicts <- multi_source_patients %>%
    group_by(.data[[patient_id_col]]) %>%
    filter(n_distinct(.data[[key_column]], na.rm = TRUE) > 1) %>%
    ungroup()
  
  if (nrow(conflicts) == 0) {
    cat("No conflicts found - all sources agree!\n")
    return(NULL)
  }
  
  cat(glue("Conflicts found: {n_distinct(conflicts[[patient_id_col]])} patients\n\n"))
  
  # Summarize conflicts
  conflict_summary <- conflicts %>%
    group_by(.data[[patient_id_col]]) %>%
    summarise(
      n_sources = n(),
      values = paste(unique(.data[[key_column]]), collapse = " vs "),
      sources = paste(source_table, collapse = " | "),
      .groups = "drop"
    )
  
  cat("Sample of conflicts:\n")
  print(head(conflict_summary, 10))
  
  return(conflict_summary)
}

get_highest_priority_per_patient <- function(long_table) {
  # Get one row per patient, choosing the highest priority source
  
  # Get the patient ID column name (first column)
  patient_id_col <- names(long_table)[1]
  
  wide_format <- long_table %>%
    arrange(.data[[patient_id_col]], source_priority) %>%
    group_by(.data[[patient_id_col]]) %>%
    slice(1) %>%
    ungroup()
  
  return(wide_format)
}

# ============================================================================
# 6. Export Functions
# ============================================================================

export_asset_table <- function(long_table, asset_name, 
                               output_dir = "/mnt/user-data/outputs",
                               format = "csv") {
  # Export long format table to file
  
  filename <- glue("{output_dir}/{asset_name}_long_format.{format}")
  
  if (format == "csv") {
    write.csv(long_table, filename, row.names = FALSE)
  } else if (format == "rds") {
    saveRDS(long_table, filename)
  } else {
    stop("Format must be 'csv' or 'rds'")
  }
  
  cat(glue("✓ Exported {asset_name} to: {filename}\n"))
  cat(glue("  Rows: {nrow(long_table)}, Columns: {ncol(long_table)}\n"))
  
  return(filename)
}

export_all_asset_tables <- function(asset_tables, 
                                    output_dir = "/mnt/user-data/outputs",
                                    format = "csv") {
  # Export all asset tables
  
  exported_files <- list()
  
  for (asset_name in names(asset_tables)) {
    filename <- export_asset_table(
      asset_tables[[asset_name]],
      asset_name,
      output_dir,
      format
    )
    exported_files[[asset_name]] <- filename
  }
  
  return(exported_files)
}

# ============================================================================
# 7. Pivoting Functions (Long to Wide)
# ============================================================================

pivot_to_wide_by_source <- function(long_table, value_columns) {
  # Pivot long format to wide format with one column per source
  # Useful for comparing values across sources side-by-side
  
  # Get the patient ID column name (first column)
  patient_id_col <- names(long_table)[1]
  
  wide_table <- long_table %>%
    select(all_of(patient_id_col), source_table, all_of(value_columns)) %>%
    pivot_wider(
      id_cols = all_of(patient_id_col),
      names_from = source_table,
      values_from = all_of(value_columns),
      names_glue = "{source_table}_{.value}"
    )
  
  return(wide_table)
}

# ============================================================================
# 8. Example Usage
# ============================================================================

example_create_ethnicity_table <- function() {
  # Example: Create long format ethnicity table from all sources
  
  config <- read_db_config("db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)
  
  # Create long format ethnicity table
  ethnicity_long <- create_long_format_asset(
    conn, config,
    asset_name = "ethnicity",
    patient_ids = c(1001, 1002, 1003, 1004, 1005)
  )
  
  # View the table
  print(ethnicity_long)
  
  # Summarize
  summarize_long_format_table(ethnicity_long, "ethnicity")
  
  # Check for conflicts
  conflicts <- check_conflicts(ethnicity_long, "ethnicity", "ethnicity_code")
  
  # Export
  export_asset_table(ethnicity_long, "ethnicity")
  
  DBI::dbDisconnect(conn)
}

example_create_all_assets <- function() {
  # Example: Create long format tables for all assets
  
  config <- read_db_config("db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)
  
  # Create all asset tables
  asset_tables <- create_all_asset_tables(
    conn, config,
    patient_ids = c(1001:1100)  # First 100 patients
  )
  
  # Access individual tables
  dob_long <- asset_tables$date_of_birth
  sex_long <- asset_tables$sex
  ethnicity_long <- asset_tables$ethnicity
  lsoa_long <- asset_tables$lsoa
  
  # Summarize each
  for (asset_name in names(asset_tables)) {
    summarize_long_format_table(asset_tables[[asset_name]], asset_name)
  }
  
  # Export all
  export_all_asset_tables(asset_tables)
  
  DBI::dbDisconnect(conn)
}

example_ethnicity_analysis <- function() {
  # Example: Detailed ethnicity analysis
  
  config <- read_db_config("db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)
  
  # Create long format ethnicity table
  ethnicity_long <- create_long_format_asset(
    conn, config,
    asset_name = "ethnicity"
  )
  
  # 1. Check conflicts
  cat("\n1. Checking for conflicts...\n")
  conflicts <- check_conflicts(ethnicity_long, "ethnicity", "ethnicity_code")
  
  # 2. Get highest priority value per patient
  cat("\n2. Creating wide format with highest priority per patient...\n")
  ethnicity_wide <- get_highest_priority_per_patient(ethnicity_long)
  
  # 3. Pivot to compare sources side-by-side
  cat("\n3. Creating comparison table (wide format by source)...\n")
  ethnicity_comparison <- pivot_to_wide_by_source(
    ethnicity_long,
    c("ethnicity_code", "ethnicity_category")
  )
  
  print("Comparison table (first 5 patients):")
  print(head(ethnicity_comparison, 5))
  
  # 4. Export both formats
  export_asset_table(ethnicity_long, "ethnicity", format = "csv")
  export_asset_table(ethnicity_wide, "ethnicity_priority", format = "csv")
  
  DBI::dbDisconnect(conn)
}

# ============================================================================
# 9. Main Pipeline Function
# ============================================================================

create_asset_pipeline <- function(config_path = "db2_config_multi_source.yaml",
                                  patient_ids = NULL,
                                  assets = c("date_of_birth", "sex", 
                                             "ethnicity", "lsoa"),
                                  output_dir = "/mnt/user-data/outputs") {
  # Complete pipeline to create and export all asset tables
  
  cat("\n==========================================================\n")
  cat("ASSET LONG FORMAT TABLE CREATION PIPELINE\n")
  cat("==========================================================\n\n")
  
  # 1. Load configuration
  cat("Step 1: Loading configuration...\n")
  config <- read_db_config(config_path)
  
  # 2. Connect to database
  cat("Step 2: Connecting to database...\n")
  conn <- create_db2_connection(config)
  
  # 3. Create asset tables
  cat("\nStep 3: Creating long format tables...\n")
  asset_tables <- create_all_asset_tables(conn, config, patient_ids, assets)
  
  # 4. Generate summaries
  cat("\nStep 4: Generating summaries...\n")
  summaries <- list()
  for (asset_name in names(asset_tables)) {
    summaries[[asset_name]] <- summarize_long_format_table(
      asset_tables[[asset_name]], 
      asset_name
    )
  }
  
  # 5. Check conflicts
  cat("\nStep 5: Checking for conflicts...\n")
  conflicts <- list()
  key_columns <- list(
    date_of_birth = "date_of_birth",
    sex = "sex_code",
    ethnicity = "ethnicity_code",
    lsoa = "lsoa_code"
  )
  
  for (asset_name in names(asset_tables)) {
    if (asset_name %in% names(key_columns)) {
      conflicts[[asset_name]] <- check_conflicts(
        asset_tables[[asset_name]],
        asset_name,
        key_columns[[asset_name]]
      )
    }
  }
  
  # 6. Export tables
  cat("\nStep 6: Exporting tables...\n")
  exported_files <- export_all_asset_tables(asset_tables, output_dir)
  
  # 7. Disconnect
  DBI::dbDisconnect(conn)
  
  cat("\n==========================================================\n")
  cat("PIPELINE COMPLETE!\n")
  cat("==========================================================\n\n")
  cat("Exported files:\n")
  for (asset_name in names(exported_files)) {
    cat(glue("  {asset_name}: {exported_files[[asset_name]]}\n"))
  }
  
  return(list(
    asset_tables = asset_tables,
    summaries = summaries,
    conflicts = conflicts,
    exported_files = exported_files
  ))
}

# ============================================================================
# Run Examples
# ============================================================================

# Uncomment to run:
# example_create_ethnicity_table()
# example_create_all_assets()
# example_ethnicity_analysis()

# Or run complete pipeline:
# results <- create_asset_pipeline(patient_ids = c(1001:2000))