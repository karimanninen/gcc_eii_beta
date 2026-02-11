# =============================================================================
# GCC EII - FUSION REGISTRY DATA PIPELINES
# =============================================================================
#
# PostgreSQL data pipelines to the GCC-Stat Fusion Registry (MARSA
# Dissemination Warehouse). Extracts GCC-Stat datasets and returns them
# in identical format to the CSV loaders in R/01_data_loading.R, enabling
# seamless source switching via load_gcc_data(source = "fusion").
#
# Usage:
#   source("pipeline/config.R")       # Load connection credentials
#   source("R/11_fusion_registry.R")  # Load this module
#   data_list <- load_gcc_data_fusion()
#
# Configuration:
#   Requires pipeline/config.R with db_config, extraction_config,
#   dataflow_tables, and column_mappings.
#   See that file for credential setup instructions.
#
# Adapted from: data-raw/scripts/01_extract_fusion.R
# =============================================================================

library(DBI)
library(RPostgres)
library(tidyverse)

# =============================================================================
# CONNECTION MANAGEMENT
# =============================================================================

#' Connect to MARSA Dissemination Warehouse
#'
#' Opens a PostgreSQL connection using credentials from db_config
#' (defined in pipeline/config.R).
#'
#' @return DBI connection object
#' @export
fusion_connect <- function() {
  if (!exists("db_config")) {
    stop("db_config not found. Run: source('pipeline/config.R') first.")
  }
  if (db_config$password == "") {
    stop("Database password not set. Add MARSA_PASSWORD to .Renviron\n",
         "  or set via: Sys.setenv(MARSA_PASSWORD = 'your_password')")
  }

  message("Connecting to MARSA warehouse...")
  message("  Host: ", db_config$host, ":", db_config$port)
  message("  Database: ", db_config$dbname)

  con <- dbConnect(
    RPostgres::Postgres(),
    host     = db_config$host,
    port     = db_config$port,
    dbname   = db_config$dbname,
    user     = db_config$user,
    password = db_config$password
  )

  test <- dbGetQuery(con, "SELECT 1 AS test")
  if (nrow(test) == 1) message("  Connection successful")

  return(con)
}

#' Disconnect from MARSA warehouse
#'
#' @param con DBI connection object
fusion_disconnect <- function(con) {
  if (!is.null(con) && dbIsValid(con)) {
    dbDisconnect(con)
    message("  Disconnected from database")
  }
}

# =============================================================================
# CORE DATA RETRIEVAL
# =============================================================================

#' Fetch data from a Fusion Registry table
#'
#' Queries a MARSA warehouse view with optional column aliasing and
#' WHERE clauses. Handles identifier quoting for table names with
#' special characters.
#'
#' @param con DBI connection
#' @param table_name Table/view name in the warehouse
#' @param columns Character vector of SQL SELECT expressions (default: "*")
#' @param extra_where Additional WHERE clauses (AND-joined with OBS_VALUE IS NOT NULL)
#' @return Tibble with query results, or NULL on failure
fusion_fetch <- function(con, table_name, columns = "*", extra_where = NULL) {

  quoted_table <- dbQuoteIdentifier(con, table_name)
  col_clause <- paste(columns, collapse = ", ")

  where_parts <- '"OBS_VALUE" IS NOT NULL'
  if (!is.null(extra_where)) {
    where_parts <- paste(c(where_parts, extra_where), collapse = " AND ")
  }

  query <- paste0("SELECT ", col_clause, " FROM ", quoted_table,
                   " WHERE ", where_parts)

  tryCatch({
    df <- dbGetQuery(con, query)
    message("  Retrieved ", nrow(df), " records from ", table_name)
    return(as_tibble(df))
  }, error = function(e) {
    warning("DB fetch error for ", table_name, ": ", e$message)
    return(NULL)
  })
}

#' Build SQL column aliases from column_mappings
#'
#' Converts a column_mappings entry into SQL SELECT expressions with
#' proper quoting and aliasing. Always appends TIME_PERIOD and OBS_VALUE.
#'
#' @param mapping Named list from column_mappings (e.g. column_mappings$cpi)
#' @return Character vector of SQL column expressions
fusion_build_columns <- function(mapping) {
  cols <- vapply(names(mapping), function(alias) {
    paste0('"', mapping[[alias]], '" AS ', alias)
  }, character(1))

  # Always include time/value columns
  c(cols, '"TIME_PERIOD" AS time_period', '"OBS_VALUE" AS value')
}

# =============================================================================
# PARSING & TRANSFORMATION HELPERS
# =============================================================================

#' Parse year from TIME_PERIOD
#'
#' Handles: "2023", "2023-Q1", "2023Q1", "2023-01", "2023-M01"
fusion_parse_year <- function(time_period) {
  as.integer(str_sub(as.character(time_period), 1, 4))
}

#' Parse quarter from TIME_PERIOD
fusion_parse_quarter <- function(time_period) {
  tp <- as.character(time_period)
  if_else(str_detect(tp, "Q"), str_extract(tp, "Q[1-4]"), NA_character_)
}

#' Standardize Fusion country names to GCC standard
#'
#' Uses extraction_config$country_standardize from pipeline/config.R
fusion_recode_country <- function(country) {
  if (!exists("extraction_config")) return(country)
  recode(country, !!!extraction_config$country_standardize, .default = country)
}

#' Filter to GCC countries and year range
fusion_filter_gcc <- function(df,
                              start_year = extraction_config$start_year,
                              end_year   = extraction_config$end_year) {
  gcc <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  df %>%
    filter(
      country %in% gcc,
      year >= start_year,
      year <= end_year,
      !is.na(value)
    )
}

# =============================================================================
# DATASET-SPECIFIC LOADERS
#
# Each returns data in identical format to the CSV loader in 01_data_loading.R.
# Column names are resolved from column_mappings in pipeline/config.R.
# =============================================================================

#' Load National Accounts from Fusion Registry
#'
#' Output matches load_national_accounts_csv():
#'   country, frequency, price_type, indicator, unit, time_period, value,
#'   year, quarter, source
load_national_accounts_fusion <- function(con,
                                          start_year = extraction_config$start_year,
                                          end_year   = extraction_config$end_year) {

  message("Extracting National Accounts...")
  cols <- fusion_build_columns(column_mappings$national_accounts)
  raw <- fusion_fetch(con, dataflow_tables$national_accounts, columns = cols)
  if (is.null(raw)) return(NULL)

  raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = fusion_parse_year(time_period),
      quarter = fusion_parse_quarter(time_period),
      value   = as.numeric(value),
      source  = "national_accounts"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, frequency, price_type, indicator, unit,
           time_period, value, year, quarter, source)
}

#' Load CPI from Fusion Registry
#'
#' Output matches load_cpi_csv():
#'   country, unit, frequency, indicator, time_period, value, year, source
load_cpi_fusion <- function(con,
                            start_year = extraction_config$start_year,
                            end_year   = extraction_config$end_year) {

  message("Extracting CPI...")
  cols <- fusion_build_columns(column_mappings$cpi)
  raw <- fusion_fetch(con, dataflow_tables$cpi, columns = cols)
  if (is.null(raw)) return(NULL)

  raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = fusion_parse_year(time_period),
      value   = as.numeric(value),
      source  = "cpi"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, unit, frequency, indicator, time_period, value, year, source)
}

#' Load Monetary & Financial from Fusion Registry
#'
#' Output matches load_monetary_csv():
#'   country, frequency, indicator, unit, time_period, value, year, source
load_monetary_fusion <- function(con,
                                 start_year = extraction_config$start_year,
                                 end_year   = extraction_config$end_year) {

  message("Extracting Monetary & Financial...")
  cols <- fusion_build_columns(column_mappings$monetary)
  raw <- fusion_fetch(con, dataflow_tables$monetary, columns = cols)
  if (is.null(raw)) return(NULL)

  raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = fusion_parse_year(time_period),
      value   = as.numeric(value),
      source  = "monetary"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, frequency, indicator, unit, time_period, value, year, source)
}

#' Load Labour Force from Fusion Registry
#'
#' Output matches load_labor_csv():
#'   country, indicator, labour_aspect, nationality, unit, gender,
#'   frequency, time_period, value, year, quarter, source
load_labor_fusion <- function(con,
                              start_year = extraction_config$start_year,
                              end_year   = extraction_config$end_year) {

  message("Extracting Labour Force...")
  cols <- fusion_build_columns(column_mappings$labor)
  raw <- fusion_fetch(con, dataflow_tables$labor, columns = cols)
  if (is.null(raw)) return(NULL)

  raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = fusion_parse_year(time_period),
      quarter = fusion_parse_quarter(time_period),
      value   = as.numeric(value),
      source  = "labor"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, indicator, labour_aspect, nationality, unit, gender,
           frequency, time_period, value, year, quarter, source)
}

#' Load Population from Fusion Registry
#'
#' Output matches load_population_csv():
#'   country, nationality, gender, age, frequency, year, value, source
load_population_fusion <- function(con,
                                   start_year = extraction_config$start_year,
                                   end_year   = extraction_config$end_year) {

  message("Extracting Population...")
  cols <- fusion_build_columns(column_mappings$population)
  raw <- fusion_fetch(con, dataflow_tables$population, columns = cols)
  if (is.null(raw)) return(NULL)

  # Population column_mappings doesn't include time_period/value aliases,
  # but fusion_build_columns() adds them automatically
  raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = fusion_parse_year(time_period),
      value   = as.numeric(value),
      source  = "population"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, nationality, gender, age, frequency, year, value, source)
}

#' Load Energy from Fusion Registry
#'
#' Output matches load_energy_csv():
#'   country, product, flow, indicator, unit, year, value, source
load_energy_fusion <- function(con,
                               start_year = extraction_config$start_year,
                               end_year   = extraction_config$end_year) {

  message("Extracting Energy...")
  cols <- fusion_build_columns(column_mappings$energy)
  raw <- fusion_fetch(con, dataflow_tables$energy, columns = cols)
  if (is.null(raw)) return(NULL)

  raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = fusion_parse_year(time_period),
      value   = as.numeric(value),
      source  = "energy"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, product, flow, indicator, unit, year, value, source)
}

#' Load Tourism from Fusion Registry
#'
#' Output matches load_tourism_csv():
#'   country, partner, indicator, frequency, year, value, source
load_tourism_fusion <- function(con,
                                start_year = extraction_config$start_year,
                                end_year   = extraction_config$end_year) {

  message("Extracting Tourism...")
  cols <- fusion_build_columns(column_mappings$tourism)
  raw <- fusion_fetch(con, dataflow_tables$tourism, columns = cols)
  if (is.null(raw)) return(NULL)

  raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = fusion_parse_year(time_period),
      value   = as.numeric(value),
      source  = "tourism"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, partner, indicator, frequency, year, value, source)
}

#' Load Common Market Tables from Fusion Registry
#'
#' Output matches load_common_market_csv():
#'   country_code, country, track, track_name, indicator_code, indicator,
#'   citizen_code, citizen, sex_code, sex, year, value, source,
#'   citizen_country, host_country
load_common_market_fusion <- function(con,
                                      start_year = extraction_config$start_year,
                                      end_year   = extraction_config$end_year) {

  message("Extracting Common Market Tables...")

  # Build SELECT columns from column_mappings
  cm_map <- column_mappings$common_market
  cols <- fusion_build_columns(cm_map)

  # Rename time_period -> year in the alias for this table (annual only)
  cols <- sub('"TIME_PERIOD" AS time_period', '"TIME_PERIOD" AS year', cols,
              fixed = TRUE)
  # Remove the OBS_VALUE alias and re-add as 'value'
  # (fusion_build_columns already adds it as 'value', so this is fine)

  raw <- fusion_fetch(con, dataflow_tables$common_market, columns = cols)
  if (is.null(raw)) return(NULL)

  raw %>%
    select(any_of(c("country_code", "country", "track", "track_name",
                     "indicator_code", "indicator", "citizen_code", "citizen",
                     "sex_code", "sex", "year", "value"))) %>%
    mutate(
      year   = as.integer(year),
      value  = as.numeric(value),
      source = "common_market"
    ) %>%
    filter(!is.na(value)) %>%
    mutate(
      citizen_country = case_when(
        citizen == "Emirati"  ~ "UAE",
        citizen == "Bahraini" ~ "Bahrain",
        citizen == "Saudi"    ~ "Saudi Arabia",
        citizen == "Omani"    ~ "Oman",
        citizen == "Qatari"   ~ "Qatar",
        citizen == "Kuwaiti"  ~ "Kuwait",
        TRUE ~ citizen
      ),
      host_country = case_when(
        country_code == "AE"     ~ "UAE",
        country_code == "BH"     ~ "Bahrain",
        country_code == "SA"     ~ "Saudi Arabia",
        country_code == "OM"     ~ "Oman",
        country_code == "QA"     ~ "Qatar",
        country_code == "KW"     ~ "Kuwait",
        country_code == "KN_TTL" ~ "GCC",
        TRUE ~ country
      )
    )
}

# =============================================================================
# MASTER LOADER
# =============================================================================

#' Load All GCC-Stat Datasets from Fusion Registry
#'
#' Connects to the MARSA warehouse and extracts all datasets. Returns a
#' named list with identical structure to load_gcc_data(), so all downstream
#' indicator modules work without modification.
#'
#' Requires pipeline/config.R to be sourced first.
#'
#' @param start_year Start of extraction range
#' @param end_year End of extraction range
#' @param standardize Apply country name standardization (default: TRUE)
#' @param remove_aggregates Remove GCC aggregate records (default: TRUE)
#' @param verbose Print progress messages (default: TRUE)
#' @param save_raw Save timestamped RDS snapshot (default: FALSE)
#' @return Named list of dataframes, same structure as load_gcc_data()
#' @export
load_gcc_data_fusion <- function(start_year        = extraction_config$start_year,
                                 end_year          = extraction_config$end_year,
                                 standardize       = TRUE,
                                 remove_aggregates = TRUE,
                                 verbose           = TRUE,
                                 save_raw          = FALSE) {

  if (!exists("db_config") || !exists("dataflow_tables") ||
      !exists("column_mappings")) {
    stop("Fusion config not loaded. Run: source('pipeline/config.R') first.")
  }

  if (verbose) {
    message("=======================================================")
    message("  GCC EII - FUSION REGISTRY DATA EXTRACTION")
    message("=======================================================")
    message("  Range: ", start_year, " - ", end_year, "\n")
  }

  con <- fusion_connect()
  on.exit(fusion_disconnect(con), add = TRUE)

  # Extract each dataset
  data_list <- list()

  safe_extract <- function(name, fn) {
    if (verbose) message("\n--- ", toupper(name), " ---")
    tryCatch(fn(con, start_year, end_year), error = function(e) {
      warning(name, " extraction failed: ", e$message)
      NULL
    })
  }

  data_list$national_accounts <- safe_extract("national_accounts",
                                               load_national_accounts_fusion)
  data_list$cpi          <- safe_extract("cpi", load_cpi_fusion)
  data_list$monetary     <- safe_extract("monetary", load_monetary_fusion)
  data_list$labor        <- safe_extract("labor", load_labor_fusion)
  data_list$population   <- safe_extract("population", load_population_fusion)
  data_list$energy       <- safe_extract("energy", load_energy_fusion)
  data_list$tourism      <- safe_extract("tourism", load_tourism_fusion)
  data_list$common_market <- safe_extract("common_market",
                                           load_common_market_fusion)

  # External sources not in Fusion - load from CSV fallback
  if (verbose) message("\n--- EXTERNAL SOURCES (CSV) ---")
  data_list$fdi <- tryCatch(load_fdi_csv("data-raw"), error = function(e) NULL)
  data_list$icp <- tryCatch(load_icp_csv("data-raw"), error = function(e) NULL)

  if (verbose) message("  Loading Comtrade data...")
  comtrade_data <- tryCatch(load_comtrade_data("data-raw"),
                            error = function(e) list(comtrade = NULL,
                                                      comtrade_hs = NULL))
  data_list$comtrade    <- comtrade_data$comtrade
  data_list$comtrade_hs <- comtrade_data$comtrade_hs

  # Standardize country names
  if (standardize) {
    if (verbose) message("\n  Standardizing country names...")
    data_list <- lapply(data_list, function(df) {
      if (!is.null(df) && "country" %in% names(df)) {
        standardize_countries(df)
      } else df
    })
  }

  # Remove aggregates
  if (remove_aggregates) {
    if (verbose) message("  Removing GCC aggregates...")
    for (ds in c("national_accounts", "population", "cpi",
                 "labor", "monetary", "energy")) {
      if (!is.null(data_list[[ds]])) {
        data_list[[ds]] <- remove_gcc_aggregates(data_list[[ds]])
      }
    }
  }

  # Save raw snapshot
  if (save_raw) {
    dir.create("data/raw", recursive = TRUE, showWarnings = FALSE)
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
    fn <- paste0("data/raw/fusion_extract_", ts, ".rds")
    saveRDS(data_list, fn)
    if (verbose) message("\n  Saved snapshot: ", fn)
  }

  # Summary
  if (verbose) {
    loaded <- sum(sapply(data_list, function(x) !is.null(x)))
    message("\n=======================================================")
    message("  EXTRACTION COMPLETE: ", loaded, " of ",
            length(data_list), " datasets")
    message("=======================================================\n")
  }

  return(data_list)
}

# =============================================================================
# DISCOVERY UTILITY
# =============================================================================

#' Discover available tables in Fusion Registry
#'
#' Checks which dataflow tables are accessible and reports their
#' structure and row counts. Useful for debugging.
#'
#' @param show_columns Show column names for each table (default: TRUE)
#' @export
fusion_discover <- function(show_columns = TRUE) {
  if (!exists("db_config") || !exists("dataflow_tables")) {
    stop("Fusion config not loaded. Run: source('pipeline/config.R') first.")
  }

  message("=======================================================")
  message("  FUSION REGISTRY - DISCOVERY MODE")
  message("=======================================================\n")

  con <- fusion_connect()
  on.exit(fusion_disconnect(con), add = TRUE)

  for (name in names(dataflow_tables)) {
    table <- dataflow_tables[[name]]
    message("--- ", name, " ---")
    message("  Table: ", table)

    tryCatch({
      quoted <- dbQuoteIdentifier(con, table)
      sample <- dbGetQuery(con, paste0("SELECT * FROM ", quoted, " LIMIT 1"))
      count  <- dbGetQuery(con, paste0("SELECT COUNT(*) AS n FROM ", quoted))
      message("  Status: accessible")
      message("  Rows: ", format(count$n, big.mark = ","))
      if (show_columns) {
        message("  Columns: ", paste(names(sample), collapse = ", "))
      }
    }, error = function(e) {
      message("  Status: NOT accessible (", e$message, ")")
    })
    message("")
  }

  return(invisible(NULL))
}

# =============================================================================

message("
=======================================================
  FUSION REGISTRY MODULE LOADED
=======================================================

Quick start:
  source('pipeline/config.R')
  source('R/11_fusion_registry.R')

  # 1. Check what's available
  fusion_discover()

  # 2. Full extraction (replaces load_gcc_data())
  data_list <- load_gcc_data_fusion()

  # 3. Or use via load_gcc_data()
  data_list <- load_gcc_data(source = 'fusion')

=======================================================
")
