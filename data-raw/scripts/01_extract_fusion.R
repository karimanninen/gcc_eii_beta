# =============================================================================
# GCC EII - UPDATED DATA EXTRACTION FOR MARSA WAREHOUSE
# =============================================================================
#
# Uses actual table names and column mappings from Fusion Registry
#
# =============================================================================

library(DBI)
library(RPostgres)
library(tidyverse)
library(glue)

# Load updated configuration
source("pipeline/config.R")

# =============================================================================
# DATABASE CONNECTION (same as before)
# =============================================================================

connect_marsa <- function() {
  if (db_config$password == "") {
    stop("Database password not set! Check .Renviron file.")
  }
  
  message("Connecting to MARSA warehouse...")
  message(glue("  Host: {db_config$host}:{db_config$port}"))
  message(glue("  Database: {db_config$dbname}"))
  message(glue("  User: {db_config$user}"))
  
  con <- dbConnect(
    RPostgres::Postgres(),
    host     = db_config$host,
    port     = db_config$port,
    dbname   = db_config$dbname,
    user     = db_config$user,
    password = db_config$password
  )
  
  test <- dbGetQuery(con, "SELECT 1 as test")
  if (nrow(test) == 1) {
    message("✓ Connection successful!\n")
  }
  
  return(con)
}

disconnect_marsa <- function(con) {
  if (!is.null(con) && dbIsValid(con)) {
    dbDisconnect(con)
    message("✓ Disconnected from database")
  }
}

# =============================================================================
# SCHEMA EXPLORATION (with special character handling)
# =============================================================================

#' List all tables/views in the database
list_tables <- function(con, schema = "public") {
  query <- glue("
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '{schema}'
    ORDER BY table_name
  ")
  result <- dbGetQuery(con, query)
  return(result$table_name)
}

#' Describe table structure
describe_table <- function(con, table_name) {
  query <- "
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = $1
    ORDER BY ordinal_position
  "
  result <- dbGetQuery(con, query, params = list(table_name))
  return(result)
}

#' Preview table data with proper quoting
preview_table <- function(table_name, n = 10) {
  con <- connect_marsa()
  on.exit(disconnect_marsa(con), add = TRUE)
  
  quoted_name <- dbQuoteIdentifier(con, table_name)
  query <- paste0("SELECT * FROM ", quoted_name, " LIMIT ", n)
  result <- dbGetQuery(con, query)
  return(result)
}

#' Quick list of all tables (names only)
list_all_tables <- function() {
  con <- connect_marsa()
  on.exit(disconnect_marsa(con), add = TRUE)
  
  tables <- list_tables(con)
  message(glue("\nFound {length(tables)} tables/views:\n"))
  for (tbl in tables) {
    message(glue("  {tbl}"))
  }
  return(invisible(tables))
}

#' Search for tables by pattern
find_tables <- function(pattern) {
  con <- connect_marsa()
  on.exit(disconnect_marsa(con), add = TRUE)
  
  tables <- list_tables(con)
  matches <- tables[grepl(pattern, tables, ignore.case = TRUE)]
  
  message(glue("\nTables matching '{pattern}':\n"))
  for (tbl in matches) {
    message(glue("  {tbl}"))
  }
  return(invisible(matches))
}

#' Explore schema with proper quoting for special characters
explore_schema <- function(search_pattern = NULL, show_columns = TRUE) {
  con <- connect_marsa()
  on.exit(disconnect_marsa(con), add = TRUE)
  
  tables <- list_tables(con)
  
  if (!is.null(search_pattern)) {
    tables <- tables[grepl(search_pattern, tables, ignore.case = TRUE)]
  }
  
  message("\n=== Available Tables/Views ===\n")
  message(paste("Found", length(tables), "tables/views\n"))
  
  if (show_columns) {
    for (tbl in tables) {
      message(glue("Table: {tbl}"))
      
      tryCatch({
        cols <- describe_table(con, tbl)
        for (i in seq_len(min(nrow(cols), 10))) {
          message(glue("  - {cols$column_name[i]} ({cols$data_type[i]})"))
        }
        if (nrow(cols) > 10) {
          message(glue("  ... and {nrow(cols) - 10} more columns"))
        }
        
        # Row count with proper quoting
        quoted_name <- dbQuoteIdentifier(con, tbl)
        count_query <- paste0("SELECT COUNT(*) as n FROM ", quoted_name)
        count <- dbGetQuery(con, count_query)
        message(glue("  Rows: {format(count$n, big.mark=',')}"))
      }, error = function(e) {
        message(glue("  Error: {e$message}"))
      })
      message("")
    }
  } else {
    for (tbl in tables) {
      message(glue("  - {tbl}"))
    }
  }
  
  return(invisible(tables))
}

# =============================================================================
# HELPER: Build SELECT query with proper quoting
# =============================================================================

build_select_query <- function(table_name, columns, con) {
  # Quote table name (handles dots and special chars)
  quoted_table <- dbQuoteIdentifier(con, table_name)
  
  # Build column list - some already have quotes for spaces
  col_list <- paste(columns, collapse = ",\n    ")
  
  query <- glue("
    SELECT 
    {col_list}
    FROM {quoted_table}
    WHERE OBS_VALUE IS NOT NULL
  ")
  
  return(query)
}

# =============================================================================
# EXTRACT NATIONAL ACCOUNTS
# =============================================================================

extract_national_accounts <- function(con,
                                      start_year = extraction_config$start_year,
                                      end_year = extraction_config$end_year) {
  
  message("Extracting National Accounts...")
  
  table_name <- dataflow_tables$national_accounts
  
  # Build query with exact column names
  quoted_table <- dbQuoteIdentifier(con, table_name)
  
  query <- glue('
    SELECT 
      "COUNTRY_en" as country,
      "FREQUENCY_en" as frequency,
      "PRICES TYPES_en" as price_type,
      "INDICATOR_en" as indicator,
      "UNIT_en" as unit,
      "TIME_PERIOD" as time_period,
      "OBS_VALUE" as value
    FROM {quoted_table}
    WHERE "OBS_VALUE" IS NOT NULL
  ')
  
  message("  Executing query...")
  raw_data <- dbGetQuery(con, query)
  message(glue("  Retrieved {nrow(raw_data)} records"))
  
  # Transform to expected format
  national_accounts <- raw_data %>%
    mutate(
      # Standardize country names
      country = recode(country, !!!extraction_config$country_standardize, 
                       .default = country),
      # Extract year from time_period (handles "2023", "2023-Q1", "2023-01", etc.)
      year = case_when(
        str_detect(time_period, "^\\d{4}$") ~ as.integer(time_period),
        str_detect(time_period, "^\\d{4}-") ~ as.integer(str_sub(time_period, 1, 4)),
        str_detect(time_period, "^\\d{4}Q") ~ as.integer(str_sub(time_period, 1, 4)),
        TRUE ~ NA_integer_
      ),
      value = as.numeric(value)
    ) %>%
    # Filter to GCC and year range
    filter(
      country %in% c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE"),
      year >= start_year,
      year <= end_year,
      !is.na(value)
    )
  
  message(glue("  Processed: {nrow(national_accounts)} records"))
  message(glue("  Years: {min(national_accounts$year, na.rm=TRUE)} - {max(national_accounts$year, na.rm=TRUE)}"))
  message(glue("  Countries: {paste(unique(national_accounts$country), collapse=', ')}"))
  message("✓ National Accounts complete\n")
  
  return(national_accounts)
}

# =============================================================================
# EXTRACT CPI
# =============================================================================

extract_cpi <- function(con,
                        start_year = extraction_config$start_year,
                        end_year = extraction_config$end_year) {
  
  message("Extracting CPI...")
  
  table_name <- dataflow_tables$cpi
  quoted_table <- dbQuoteIdentifier(con, table_name)
  
  # Query with exact column names from describe_table
  query <- glue('
    SELECT 
      "COUNTRY_en" as country,
      "UNIT_en" as unit,
      "FREQUENCY_en" as frequency,
      "INDICATOR_en" as indicator,
      "TIME_PERIOD" as time_period,
      "OBS_VALUE" as value
    FROM {quoted_table}
    WHERE "OBS_VALUE" IS NOT NULL
  ')
  
  message("  Executing query...")
  raw_data <- dbGetQuery(con, query)
  message(glue("  Retrieved {nrow(raw_data)} records"))
  
  # Transform to match original load_gcc_data format
  cpi <- raw_data %>%
    mutate(
      # Standardize country names
      country = recode(country, !!!extraction_config$country_standardize, 
                       .default = country),
      # Extract year from time_period (handles "2023", "2023-01", "2023-M01", etc.)
      year = case_when(
        str_detect(time_period, "^\\d{4}$") ~ as.integer(time_period),
        str_detect(time_period, "^\\d{4}-") ~ as.integer(str_sub(time_period, 1, 4)),
        str_detect(time_period, "^\\d{4}M") ~ as.integer(str_sub(time_period, 1, 4)),
        TRUE ~ NA_integer_
      ),
      value = as.numeric(value)
    ) %>%
    # Filter to GCC and year range
    filter(
      country %in% c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE"),
      year >= start_year,
      year <= end_year,
      !is.na(value)
    )
  
  message(glue("  Processed: {nrow(cpi)} records"))
  message(glue("  Years: {min(cpi$year, na.rm=TRUE)} - {max(cpi$year, na.rm=TRUE)}"))
  message(glue("  Countries: {paste(unique(cpi$country), collapse=', ')}"))
  message("✓ CPI complete\n")
  
  return(cpi)
}

# =============================================================================
# GENERIC EXTRACTION FUNCTION
# =============================================================================
# For tables where we haven't verified columns yet, use this flexible approach

extract_generic <- function(con, 
                            table_name, 
                            dataset_name,
                            start_year = extraction_config$start_year,
                            end_year = extraction_config$end_year) {
  
  message(glue("Extracting {dataset_name}..."))
  
  # First, check if table exists
  quoted_table <- dbQuoteIdentifier(con, table_name)
  
  # Get all data and let R handle the transformation
  query <- glue("SELECT * FROM {quoted_table} WHERE \"OBS_VALUE\" IS NOT NULL")
  
  tryCatch({
    raw_data <- dbGetQuery(con, query)
    message(glue("  Retrieved {nrow(raw_data)} records"))
    message(glue("  Columns: {paste(names(raw_data), collapse=', ')}"))
    
    # Find the English columns (ending in _en)
    en_cols <- names(raw_data)[grepl("_en$|_en\"$", names(raw_data), ignore.case = TRUE)]
    message(glue("  English columns: {paste(en_cols, collapse=', ')}"))
    
    return(raw_data)
    
  }, error = function(e) {
    message(glue("  ✗ Error: {e$message}"))
    return(NULL)
  })
}

# =============================================================================
# EXTRACT CPI
# =============================================================================

extract_cpi <- function(con,
                        start_year = extraction_config$start_year,
                        end_year = extraction_config$end_year) {
  
  message("Extracting CPI...")
  
  table_name <- dataflow_tables$cpi
  quoted_table <- dbQuoteIdentifier(con, table_name)
  
  # First try to get column names
  cols_query <- glue("SELECT * FROM {quoted_table} LIMIT 1")
  sample <- tryCatch(dbGetQuery(con, cols_query), error = function(e) NULL)
  
  if (is.null(sample)) {
    message("  ✗ Table not found or not accessible")
    return(NULL)
  }
  
  message(glue("  Available columns: {paste(names(sample), collapse=', ')}"))
  
  # Build query based on available columns
  # Adjust column names based on what's actually there
  query <- glue('
    SELECT 
      "COUNTRY_en" as country,
      "FREQUENCY_en" as frequency,
      "INDICATOR_en" as indicator,
      "UNIT_en" as unit,
      "TIME_PERIOD" as time_period,
      "OBS_VALUE" as value
    FROM {quoted_table}
    WHERE "OBS_VALUE" IS NOT NULL
  ')
  
  raw_data <- dbGetQuery(con, query)
  message(glue("  Retrieved {nrow(raw_data)} records"))
  
  cpi <- raw_data %>%
    mutate(
      country = recode(country, !!!extraction_config$country_standardize, 
                       .default = country),
      year = case_when(
        str_detect(time_period, "^\\d{4}$") ~ as.integer(time_period),
        str_detect(time_period, "^\\d{4}-") ~ as.integer(str_sub(time_period, 1, 4)),
        TRUE ~ NA_integer_
      ),
      value = as.numeric(value)
    ) %>%
    filter(
      country %in% c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE"),
      year >= start_year,
      year <= end_year,
      !is.na(value)
    )
  
  message(glue("  Processed: {nrow(cpi)} records"))
  message("✓ CPI complete\n")
  
  return(cpi)
}

# =============================================================================
# EXTRACT ALL WITH DISCOVERY MODE
# =============================================================================

#' Extract all datasets with automatic column discovery
#' 
#' @param save_raw Save timestamped snapshot
#' @param discovery_mode If TRUE, just report what's available without full extraction
extract_all_from_fusion <- function(start_year = extraction_config$start_year,
                                    end_year = extraction_config$end_year,
                                    save_raw = TRUE,
                                    discovery_mode = FALSE) {
  
  message("=======================================================")
  message("  GCC EII - FUSION REGISTRY DATA EXTRACTION")
  message("=======================================================\n")
  
  con <- connect_marsa()
  on.exit(disconnect_marsa(con), add = TRUE)
  
  if (discovery_mode) {
    # Just check what tables exist and their structures
    message("DISCOVERY MODE: Checking available tables...\n")
    
    for (name in names(dataflow_tables)) {
      table <- dataflow_tables[[name]]
      message(glue("--- {name} ---"))
      message(glue("Table: {table}"))
      
      quoted <- dbQuoteIdentifier(con, table)
      test_query <- glue("SELECT * FROM {quoted} LIMIT 1")
      
      tryCatch({
        sample <- dbGetQuery(con, test_query)
        message(glue("✓ Accessible"))
        message(glue("Columns: {paste(names(sample), collapse=', ')}"))
        
        # Count rows
        count_query <- glue("SELECT COUNT(*) as n FROM {quoted}")
        count <- dbGetQuery(con, count_query)
        message(glue("Rows: {format(count$n, big.mark=',')}"))
      }, error = function(e) {
        message(glue("✗ Not accessible: {e$message}"))
      })
      message("")
    }
    
    return(invisible(NULL))
  }
  
  # Full extraction
  data_list <- list()
  
  # National Accounts (verified)
  data_list$national_accounts <- extract_national_accounts(con, start_year, end_year)
  
  # CPI
  tryCatch({
    data_list$cpi <- extract_cpi(con, start_year, end_year)
  }, error = function(e) {
    message(glue("CPI extraction failed: {e$message}"))
    data_list$cpi <- NULL
  })
  
  # Other datasets - use generic extraction for now
  # You can replace these with specific functions once columns are verified
  
  other_tables <- c("monetary", "labor", "population", "energy", "tourism")
  
  for (ds_name in other_tables) {
    if (ds_name %in% names(dataflow_tables)) {
      tryCatch({
        raw <- extract_generic(con, dataflow_tables[[ds_name]], ds_name, 
                               start_year, end_year)
        if (!is.null(raw)) {
          data_list[[ds_name]] <- raw
        }
      }, error = function(e) {
        message(glue("{ds_name} extraction failed: {e$message}"))
      })
    }
  }
  
  # Add metadata
  data_list$extraction_metadata <- list(
    timestamp = Sys.time(),
    start_year = start_year,
    end_year = end_year,
    source = "MARSA Dissemination Warehouse"
  )
  
  # Save raw data
  if (save_raw) {
    dir.create("data/raw", recursive = TRUE, showWarnings = FALSE)
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    filename <- glue("data/raw/raw_extract_{timestamp}.rds")
    saveRDS(data_list, filename)
    message(glue("\n✓ Raw data saved: {filename}"))
  }
  
  message("\n=======================================================")
  message("  EXTRACTION COMPLETE")
  message("=======================================================\n")
  
  return(data_list)
}

# =============================================================================
# QUICK TEST FUNCTION
# =============================================================================

test_national_accounts <- function() {
  con <- connect_marsa()
  on.exit(disconnect_marsa(con), add = TRUE)
  
  na_data <- extract_national_accounts(con)
  
  message("\nSample data:")
  print(head(na_data, 20))
  
  message("\nSummary:")
  message(glue("  Countries: {paste(unique(na_data$country), collapse=', ')}"))
  message(glue("  Years: {min(na_data$year)} - {max(na_data$year)}"))
  message(glue("  Frequencies: {paste(unique(na_data$frequency), collapse=', ')}"))
  message(glue("  Indicators: {length(unique(na_data$indicator))} unique"))
  
  return(na_data)
}

# =============================================================================

message("
=======================================================
  UPDATED EXTRACTION LAYER LOADED
=======================================================

Quick start:

  # 1. Test National Accounts extraction
  na_data <- test_national_accounts()
  
  # 2. Discovery mode - check all tables
  extract_all_from_fusion(discovery_mode = TRUE)
  
  # 3. Full extraction
  data_list <- extract_all_from_fusion()

=======================================================
")
