# R Script for Multi-Source DB2 Asset Management
# Handles multiple source tables per asset with flexible selection

library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)
library(purrr)

# ============================================================================
# 1. Configuration Reading Functions
# ============================================================================

read_db_config <- function(config_path = "db2_config_multi_source.yaml") {
  config <- yaml::read_yaml(config_path)
  return(config)
}

# ============================================================================
# 2. Database Connection
# ============================================================================
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
# 3. Source Table Selection Functions
# ============================================================================

get_asset_sources <- function(config, asset_name) {
  # Get all available source tables for an asset
  asset_config <- config$assets[[asset_name]]
  
  if (is.null(asset_config)) {
    stop(glue("Asset '{asset_name}' not found in configuration"))
  }
  
  sources <- names(asset_config$sources)
  
  # Get metadata for each source
  source_info <- lapply(sources, function(source) {
    source_config <- asset_config$sources[[source]]
    list(
      name = source,
      table_name = source_config$table_name,
      priority = source_config$priority,
      data_quality = source_config$data_quality,
      coverage = source_config$coverage,
      description = source_config$description
    )
  })
  
  # Sort by priority
  source_info <- source_info[order(sapply(source_info, function(x) x$priority))]
  
  return(source_info)
}

select_source_for_asset <- function(config, asset_name, 
                                    preferred_source = NULL,
                                    project_name = NULL) {
  # Select which source table to use for an asset
  
  # If project is specified, use project preferences
  if (!is.null(project_name)) {
    project_config <- config$projects[[project_name]]
    if (!is.null(project_config)) {
      preferred_source <- project_config$preferred_sources[[asset_name]]
    }
  }
  
  # If preferred source specified, use it
  if (!is.null(preferred_source)) {
    return(preferred_source)
  }
  
  # Otherwise, use default source
  default_source <- config$assets[[asset_name]]$default_source
  return(default_source)
}

# ============================================================================
# 4. Query Building Functions
# ============================================================================

get_source_columns <- function(config, asset_name, source_name) {
  # Get column mappings for a specific source
  source_config <- config$assets[[asset_name]]$sources[[source_name]]
  
  if (is.null(source_config)) {
    stop(glue("Source '{source_name}' not found for asset '{asset_name}'"))
  }
  
  columns <- source_config$columns
  db_columns <- sapply(columns, function(x) x$db_column)
  
  return(list(
    table_name = source_config$table_name,
    columns = db_columns,
    primary_key = source_config$primary_key,
    description = source_config$description
  ))
}

build_select_query <- function(config, asset_name, source_name, 
                               patient_ids = NULL, schema = NULL) {
  # Build a SELECT query for a specific source
  
  if (is.null(schema)) {
    schema <- config$database$schema
  }
  
  source_info <- get_source_columns(config, asset_name, source_name)
  
  # Build column list with aliases
  column_list <- paste(
    glue("{source_info$columns} AS {names(source_info$columns)}"),
    collapse = ",\n    "
  )
  
  # Add source identifier column
  query <- glue("
    SELECT 
      '{source_name}' AS data_source,
      {column_list}
    FROM {schema}.{source_info$table_name}
  ")
  
  # Add WHERE clause if patient IDs specified
  if (!is.null(patient_ids)) {
    ids_string <- paste(patient_ids, collapse = ", ")
    # Use primary_key from config
    pk <- source_info$primary_key
    query <- paste(query, glue("WHERE {pk} IN ({ids_string})"))
  }
  
  return(query)
}

# ============================================================================
# 5. Data Retrieval Functions
# ============================================================================

get_asset_data <- function(conn, config, asset_name, 
                           source_name = NULL,
                           patient_ids = NULL,
                           project_name = NULL) {
  # Get data from a single source for an asset
  
  # Select source if not specified
  if (is.null(source_name)) {
    source_name <- select_source_for_asset(config, asset_name, 
                                           project_name = project_name)
  }
  
  # Build and execute query
  query <- build_select_query(config, asset_name, source_name, patient_ids)
  
  cat(glue("Retrieving {asset_name} from source: {source_name}\n"))
  data <- DBI::dbGetQuery(conn, query)
  
  return(data)
}

get_asset_data_multi_source <- function(conn, config, asset_name,
                                        source_names,
                                        patient_ids = NULL) {
  # Get data from multiple sources and combine
  
  all_data <- list()
  
  for (source in source_names) {
    cat(glue("Retrieving {asset_name} from source: {source}\n"))
    
    data <- get_asset_data(conn, config, asset_name, 
                           source_name = source,
                           patient_ids = patient_ids)
    
    all_data[[source]] <- data
  }
  
  # Combine all sources
  combined_data <- bind_rows(all_data)
  
  return(combined_data)
}

get_asset_data_with_fallback <- function(conn, config, asset_name,
                                         preferred_source,
                                         patient_ids = NULL) {
  # Try preferred source, fall back to priority order if needed
  
  # Try preferred source first
  tryCatch({
    data <- get_asset_data(conn, config, asset_name,
                           source_name = preferred_source,
                           patient_ids = patient_ids)
    
    if (nrow(data) > 0) {
      return(data)
    }
  }, error = function(e) {
    warning(glue("Failed to retrieve from {preferred_source}: {e$message}"))
  })
  
  # Fall back to other sources by priority
  sources <- get_asset_sources(config, asset_name)
  
  for (source in sources) {
    if (source$name == preferred_source) next  # Already tried
    
    cat(glue("Falling back to: {source$name}\n"))
    
    tryCatch({
      data <- get_asset_data(conn, config, asset_name,
                             source_name = source$name,
                             patient_ids = patient_ids)
      
      if (nrow(data) > 0) {
        return(data)
      }
    }, error = function(e) {
      warning(glue("Failed to retrieve from {source$name}: {e$message}"))
    })
  }
  
  stop(glue("Could not retrieve {asset_name} from any source"))
}

# ============================================================================
# 6. Project-Based Data Retrieval
# ============================================================================

get_project_data <- function(conn, config, project_name, patient_ids = NULL) {
  # Get all assets according to project configuration
  
  project_config <- config$projects[[project_name]]
  
  if (is.null(project_config)) {
    stop(glue("Project '{project_name}' not found in configuration"))
  }
  
  cat(glue("\n=== Retrieving data for project: {project_name} ===\n"))
  cat(glue("Description: {project_config$description}\n\n"))
  
  # Get data for each asset
  project_data <- list()
  
  for (asset_name in names(project_config$preferred_sources)) {
    preferred <- project_config$preferred_sources[[asset_name]]
    
    # Handle multiple sources for one asset
    if (is.list(preferred)) {
      # Multiple sources requested
      project_data[[asset_name]] <- get_asset_data_multi_source(
        conn, config, asset_name, preferred, patient_ids
      )
    } else {
      # Single source
      project_data[[asset_name]] <- get_asset_data(
        conn, config, asset_name, 
        source_name = preferred,
        patient_ids = patient_ids
      )
    }
  }
  
  return(project_data)
}

# ============================================================================
# 7. Data Combination Functions
# ============================================================================

combine_ethnicity_sources <- function(data_list) {
  # Combine ethnicity from multiple sources using priority rules
  
  # Priority order (as defined in config)
  priority_order <- c("self_reported_ethnicity", 
                      "admission_ethnicity",
                      "outpatient_ethnicity",
                      "gp_ethnicity")
  
  # Start with highest priority
  combined <- NULL
  
  for (source in priority_order) {
    if (source %in% names(data_list)) {
      source_data <- data_list[[source]]
      
      if (is.null(combined)) {
        combined <- source_data
      } else {
        # Add patients not already in combined
        new_patients <- source_data %>%
          anti_join(combined, by = "patient_id")
        
        combined <- bind_rows(combined, new_patients)
      }
    }
  }
  
  return(combined)
}

resolve_conflicts <- function(multi_source_data, asset_name, 
                              resolution_method = "priority") {
  # Resolve conflicts when same patient appears in multiple sources
  
  if (resolution_method == "priority") {
    # Keep first occurrence (highest priority source)
    resolved <- multi_source_data %>%
      arrange(patient_id) %>%
      group_by(patient_id) %>%
      slice(1) %>%
      ungroup()
    
  } else if (resolution_method == "most_recent") {
    # Keep most recent record
    resolved <- multi_source_data %>%
      arrange(patient_id, desc(record_date)) %>%
      group_by(patient_id) %>%
      slice(1) %>%
      ungroup()
    
  } else if (resolution_method == "consensus") {
    # Flag conflicts, keep most common value
    resolved <- multi_source_data %>%
      group_by(patient_id) %>%
      mutate(
        n_sources = n(),
        conflict_flag = n_distinct(ethnicity_code) > 1
      ) %>%
      slice(1) %>%
      ungroup()
  }
  
  return(resolved)
}

# ============================================================================
# 8. Utility Functions
# ============================================================================

print_asset_sources <- function(config, asset_name) {
  # Print all available sources for an asset
  
  sources <- get_asset_sources(config, asset_name)
  
  cat(glue("\n=== Available sources for {asset_name} ===\n"))
  cat(glue("Default: {config$assets[[asset_name]]$default_source}\n\n"))
  
  for (source in sources) {
    cat(glue("Source: {source$name}\n"))
    cat(glue("  Table: {source$table_name}\n"))
    cat(glue("  Priority: {source$priority}\n"))
    cat(glue("  Quality: {source$data_quality}\n"))
    cat(glue("  Coverage: {source$coverage}\n"))
    cat(glue("  Description: {source$description}\n\n"))
  }
}

print_project_config <- function(config, project_name) {
  # Print project configuration
  
  project_config <- config$projects[[project_name]]
  
  if (is.null(project_config)) {
    stop(glue("Project '{project_name}' not found"))
  }
  
  cat(glue("\n=== Project Configuration: {project_name} ===\n"))
  cat(glue("Description: {project_config$description}\n\n"))
  cat("Preferred Sources:\n")
  
  for (asset in names(project_config$preferred_sources)) {
    source <- project_config$preferred_sources[[asset]]
    cat(glue("  {asset}: {paste(source, collapse = ', ')}\n"))
  }
  
  cat(glue("\nFallback Strategy: {project_config$fallback_strategy}\n"))
}

compare_sources <- function(conn, config, asset_name, patient_ids = NULL) {
  # Compare data across all sources for an asset
  
  sources <- get_asset_sources(config, asset_name)
  source_names <- sapply(sources, function(x) x$name)
  
  comparison_data <- get_asset_data_multi_source(
    conn, config, asset_name, source_names, patient_ids
  )
  
  # Analyze differences
  patient_summary <- comparison_data %>%
    group_by(patient_id) %>%
    summarise(
      n_sources = n(),
      sources = paste(data_source, collapse = ", "),
      .groups = "drop"
    )
  
  cat(glue("\n=== Source Comparison for {asset_name} ===\n"))
  cat(glue("Total unique patients: {n_distinct(comparison_data$patient_id)}\n"))
  cat(glue("Patients in multiple sources: {sum(patient_summary$n_sources > 1)}\n\n"))
  
  return(list(
    data = comparison_data,
    summary = patient_summary
  ))
}

# ============================================================================
# 9. Example Usage
# ============================================================================

example_basic_usage <- function() {
  # Example: Basic usage with source selection
  
  config <- read_db_config("db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)
  
  # Get ethnicity from preferred self-reported source
  ethnicity_data <- get_asset_data(
    conn, config,
    asset_name = "ethnicity",
    source_name = "self_reported_ethnicity",
    patient_ids = c(1001, 1002, 1003)
  )
  
  print(ethnicity_data)
  
  DBI::dbDisconnect(conn)
}

example_project_usage <- function() {
  # Example: Retrieve data for a specific project
  
  config <- read_db_config("db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)
  
  # Get all data for clinical research study project
  project_data <- get_project_data(
    conn, config,
    project_name = "clinical_research_study",
    patient_ids = c(1001, 1002, 1003)
  )
  
  # Access individual assets
  dob_data <- project_data$date_of_birth
  ethnicity_data <- project_data$ethnicity
  lsoa_data <- project_data$lsoa
  
  print("Date of Birth Data:")
  print(dob_data)
  
  print("Ethnicity Data:")
  print(ethnicity_data)
  
  DBI::dbDisconnect(conn)
}

example_multi_source <- function() {
  # Example: Get ethnicity from multiple sources and combine
  
  config <- read_db_config("db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)
  
  # Get ethnicity from all sources
  ethnicity_sources <- c("self_reported_ethnicity", 
                         "admission_ethnicity", 
                         "gp_ethnicity")
  
  ethnicity_multi <- get_asset_data_multi_source(
    conn, config,
    asset_name = "ethnicity",
    source_names = ethnicity_sources,
    patient_ids = c(1001, 1002, 1003)
  )
  
  # Resolve conflicts using priority
  ethnicity_resolved <- resolve_conflicts(
    ethnicity_multi,
    asset_name = "ethnicity",
    resolution_method = "priority"
  )
  
  print("Ethnicity from multiple sources:")
  print(ethnicity_multi)
  
  print("Resolved ethnicity:")
  print(ethnicity_resolved)
  
  DBI::dbDisconnect(conn)
}

example_source_comparison <- function() {
  # Example: Compare data across sources
  
  config <- read_db_config("db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)
  
  # Print available sources
  print_asset_sources(config, "ethnicity")
  
  # Compare all sources
  comparison <- compare_sources(
    conn, config,
    asset_name = "ethnicity",
    patient_ids = c(1001, 1002, 1003)
  )
  
  print("Comparison Summary:")
  print(comparison$summary)
  
  DBI::dbDisconnect(conn)
}

# ============================================================================
# Print available projects and assets
# ============================================================================

# config <- read_db_config("db2_config_multi_source.yaml")
# print_asset_sources(config, "ethnicity")
# print_project_config(config, "clinical_research_study")