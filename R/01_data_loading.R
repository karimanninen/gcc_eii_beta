# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - DATA LOADING MODULE
# =============================================================================
#
# Purpose:
#   Load and clean all data sources for the GCC Economic Integration Index.
#   This module provides a clean, standardized data pipeline that outputs
#   tidy datasets ready for indicator calculation.
#
# Structure:
#   1. Configuration & Constants
#   2. CSV Data Loading (current implementation)
#   3. SDMX Data Loading (future implementation - placeholder)
#   4. Data Cleaning & Standardization
#   5. Main Loading Function
#   6. Raw Indicator Extraction
#
# Future Migration:
#   The CSV loading functions are designed to be replaced with SDMX pipelines
#   connecting to warehouse.marsa.gccstat.org. The output structure will
#   remain identical, enabling seamless transition.
#
# =============================================================================

library(tidyverse)
library(readr)
library(lubridate)

# =============================================================================
# SECTION 1: CONFIGURATION & CONSTANTS
# =============================================================================

#' GCC Country Configuration
#'
#' Standard country codes and names used throughout the index
GCC_CONFIG <- list(
  countries = c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE"),
  iso3 = c("BHR", "KWT", "OMN", "QAT", "SAU", "ARE"),
  

  # Country name variants for standardization
  name_variants = list(
    UAE = c("UAE", "Emirates", "United Arab Emirates", "ARE"),
    `Saudi Arabia` = c("Saudi Arabia", "KSA", "Saudi", "SAU"),
    Bahrain = c("Bahrain", "BHR"),
    Kuwait = c("Kuwait", "KWT"),
    Oman = c("Oman", "OMN"),
    Qatar = c("Qatar", "QAT")
  ),
  
  # Aggregate identifiers to exclude
  aggregate_names = c("GCC", "Gulf Cooperation Council", "Total GCC")
)

#' Data Source Configuration
#'
#' File paths and configurations for each data source
#' Update these paths when migrating to SDMX or changing file locations
DATA_SOURCES <- list(
  common_market = list(
    file = "df_common_market_tables_NEW.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  national_accounts = list(
    file = "DF_ES_NA.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  tourism = list(
    file = "DF_GEETS_TUR.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  cpi = list(
    file = "DF_ES_CPI.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  labor = list(
    file = "DF_PSS_LAB.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  monetary = list(
    file = "DF_ES_MF.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  energy = list(
    file = "DF_GEETS_ENR.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  population = list(
    file = "DF_PSS_DEM_POP.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  fdi = list(
    file = "GCC FDI flows.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  icp = list(
    file = "ICP_data_NEW.csv",
    skip_rows = 0,
    source_type = "csv"
  )
)

# =============================================================================
# SECTION 2: CSV DATA LOADING FUNCTIONS
# =============================================================================

#' Load Common Market Tables
#' Load Common Market Tables Data (Long Format SDMX)
#'
#' The new file (df_common_market_tables_NEW.csv) is already in long format
#' with TIME_PERIOD and OBS_VALUE columns (SDMX style).
#' Data by citizenship (nationality), not host country.
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with common market data in long format
load_common_market_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$common_market$file)

  if (!file.exists(file_path)) {
    warning(paste("Common Market file not found:", file_path))
    return(NULL)
  }

  df <- read_csv(file_path, show_col_types = FALSE)

  # Rename columns to standard names matching the downstream contract
  # CSV has paired code/label columns with duplicate names, which read_csv

  # auto-renames using ...N positional suffixes:
  #   Country...1, Country...2, Track...3, Track...4, Indicator...5,
  #   Indicator...6, Citizen...7, Citizen...8, Sex...9, Sex...10,
  #   Units, Frequency...12, Frequency...13, TIME_PERIOD, OBS_VALUE
  df_long <- df %>%
    rename(
      country_code = `Country...1`,
      country = `Country...2`,
      track = `Track...3`,
      track_name = `Track...4`,
      indicator_code = `Indicator...5`,
      indicator = `Indicator...6`,
      citizen_code = `Citizen...7`,
      citizen = `Citizen...8`,
      sex_code = `Sex...9`,
      sex = `Sex...10`,
      year = TIME_PERIOD,
      value = OBS_VALUE
    ) %>%
    mutate(
      year = as.integer(year),
      value = as.numeric(value),
      source = "common_market"
    ) %>%
    filter(!is.na(value))

  # Map citizenship to standard country names for mobility indicators
  df_long <- df_long %>%
    mutate(
      # The "citizen" column indicates nationality (whose citizens)
      citizen_country = case_when(
        citizen == "Emirati" ~ "UAE",
        citizen == "Bahraini" ~ "Bahrain",
        citizen == "Saudi" ~ "Saudi Arabia",
        citizen == "Omani" ~ "Oman",
        citizen == "Qatari" ~ "Qatar",
        citizen == "Kuwaiti" ~ "Kuwait",
        TRUE ~ citizen
      ),
      # The "country" column indicates host country (where they are)
      host_country = case_when(
        country_code == "AE" ~ "UAE",
        country_code == "BH" ~ "Bahrain",
        country_code == "SA" ~ "Saudi Arabia",
        country_code == "OM" ~ "Oman",
        country_code == "QA" ~ "Qatar",
        country_code == "KW" ~ "Kuwait",
        country_code == "KN_TTL" ~ "GCC",
        TRUE ~ country
      )
    )

  return(df_long)
}

#' Load National Accounts
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with national accounts data
load_national_accounts_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$national_accounts$file)
  
  if (!file.exists(file_path)) {
    warning(paste("National Accounts file not found:", file_path))
    return(NULL)
  }
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    rename(
      country = COUNTRY,
      frequency = FREQUENCY,
      price_type = `PRICES TYPES`,
      indicator = INDICATOR,
      unit = UNIT,
      time_period = TIME_PERIOD,
      value = OBS_VALUE
    ) %>%
    mutate(
      # Extract year from both annual (2015) and quarterly (2020-Q1) formats
      year = if_else(
        str_detect(time_period, "Q"),
        as.integer(str_sub(time_period, 1, 4)),
        as.integer(time_period)
      ),
      # Keep quarter info for reference
      quarter = if_else(
        str_detect(time_period, "Q"),
        str_extract(time_period, "Q[1-4]"),
        NA_character_
      ),
      source = "national_accounts"
    ) %>%
    filter(!is.na(value))
  
  return(df)
}

#' Load Tourism Data
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with tourism data
load_tourism_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$tourism$file)
  
  if (!file.exists(file_path)) {
    warning(paste("Tourism file not found:", file_path))
    return(NULL)
  }
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    rename(
      country = COUNTRY,
      partner = `PARTENER COUNTRY`,
      indicator = INDICATOR,
      frequency = FREQUENCY,
      year = TIME_PERIOD,
      value = OBS_VALUE
    ) %>%
    mutate(
      value = as.numeric(value),
      year = as.integer(year),
      source = "tourism"
    ) %>%
    filter(!is.na(value))
  
  return(df)
}

#' Load CPI/Inflation Data
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with CPI data
load_cpi_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$cpi$file)
  
  if (!file.exists(file_path)) {
    warning(paste("CPI file not found:", file_path))
    return(NULL)
  }
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    rename(
      country = COUNTRY,
      unit = UNIT,
      frequency = FREQUENCY,
      indicator = INDICATOR,
      time_period = TIME_PERIOD,
      value = OBS_VALUE
    ) %>%
    mutate(
      year = if_else(
        str_detect(time_period, "-"),
        as.integer(str_sub(time_period, 1, 4)),
        as.integer(time_period)
      ),
      source = "cpi"
    ) %>%
    filter(!is.na(value))
  
  return(df)
}

#' Load Labor Force Data
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with labor data
load_labor_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$labor$file)
  
  if (!file.exists(file_path)) {
    warning(paste("Labor file not found:", file_path))
    return(NULL)
  }
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    rename(
      country = COUNTRY,
      indicator = INDICATOR,
      labour_aspect = `LABOUR ASPECT`,
      nationality = NATIONALITY,
      unit = UNIT,
      gender = GENDER,
      frequency = FREQUENCY,
      time_period = TIME_PERIOD,
      value = OBS_VALUE
    ) %>%
    mutate(
      # Extract year from both "2023" and "2023-Q1" formats
      year = as.integer(str_sub(time_period, 1, 4)),
      # Keep quarter info for reference
      quarter = if_else(
        str_detect(time_period, "Q"),
        str_extract(time_period, "Q[1-4]"),
        NA_character_
      ),
      value = as.numeric(value),
      source = "labor"
    ) %>%
    filter(!is.na(value))
  
  return(df)
}

#' Load Monetary & Financial Data
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with monetary data
load_monetary_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$monetary$file)
  
  if (!file.exists(file_path)) {
    warning(paste("Monetary file not found:", file_path))
    return(NULL)
  }
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    rename(
      country = COUNTRY,
      frequency = FREQUENCY,
      indicator = INDICATOR,
      unit = UNIT,
      time_period = TIME_PERIOD,
      value = OBS_VALUE
    ) %>%
    mutate(
      year = if_else(
        str_detect(time_period, "-"),
        as.integer(str_sub(time_period, 1, 4)),
        as.integer(time_period)
      ),
      source = "monetary"
    ) %>%
    filter(!is.na(value))
  
  return(df)
}

#' Load Energy Data
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with energy data
load_energy_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$energy$file)
  
  if (!file.exists(file_path)) {
    warning(paste("Energy file not found:", file_path))
    return(NULL)
  }
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    rename(
      country = COUNTRY,
      product = PRODUCT,
      flow = FLOW,
      indicator = INDICATOR,
      unit = UNIT,
      year = TIME_PERIOD,
      value = OBS_VALUE
    ) %>%
    mutate(
      year = as.integer(year),
      source = "energy"
    ) %>%
    filter(!is.na(value))
  
  return(df)
}

#' Load Population Data
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with population data
load_population_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$population$file)
  
  if (!file.exists(file_path)) {
    warning(paste("Population file not found:", file_path))
    return(NULL)
  }
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    rename(
      country = COUNTRY,
      nationality = NATIONALITY,
      gender = GENDER,
      age = AGE,
      frequency = FREQUENCY,
      year = TIME_PERIOD,
      value = OBS_VALUE
    ) %>%
    mutate(
      year = as.integer(year),
      source = "population"
    ) %>%
    filter(!is.na(value))
  
  return(df)
}

#' Load FDI Flows Data
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with FDI data
load_fdi_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$fdi$file)
  
  if (!file.exists(file_path)) {
    warning(paste("FDI file not found:", file_path))
    return(NULL)
  }
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    rename(
      flow = Flow,
      country = Country,
      code = `Alpha-3 code`
    ) %>%
    pivot_longer(
      cols = matches("^\\d{4}"),
      names_to = "year",
      values_to = "value"
    ) %>%
    mutate(
      year = as.integer(year),
      value = as.numeric(value),
      source = "fdi"
    ) %>%
    filter(!is.na(value))
  
  return(df)
}

#' Load ICP (World Bank) Data
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with ICP data
load_icp_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$icp$file)
  
  if (!file.exists(file_path)) {
    warning(paste("ICP file not found:", file_path))
    return(NULL)
  }
  
  df <- read_csv(file_path, show_col_types = FALSE) %>%
    rename(
      country = country_name,
      code = country_code,
      classification = classification_name,
      series = series_name
    ) %>%
    mutate(
      value = as.numeric(value),
      year = as.integer(year),
      source = "icp"
    ) %>%
    filter(!is.na(value))
  
  return(df)
}

#' Load Comtrade Data (RDS files)
#'
#' @param data_dir Directory containing data files
#' @return List with total_trade, bec_trade, non_oil_trade components
load_comtrade_data <- function(data_dir = ".") {
  
  comtrade <- list(
    total_trade = NULL,
    bec_trade = NULL,
    non_oil_trade = NULL
  )
  
  comtrade_hs <- list(
    total_trade = NULL,
    bec_trade = NULL,
    non_oil_trade = NULL
  )
  
  # Load aggregate Comtrade data
  agg_file <- file.path(data_dir, "comtrade_data.rds")
  if (file.exists(agg_file)) {
    comtrade <- readRDS(agg_file)
    message("✓ Loaded comtrade_data.rds")
  }
  
  # Load HS-level Comtrade data
  hs_file <- file.path(data_dir, "comtrade_data_hs.rds")
  if (file.exists(hs_file)) {
    comtrade_hs <- readRDS(hs_file)
    message("✓ Loaded comtrade_data_hs.rds")
  }
  
  return(list(
    comtrade = comtrade,
    comtrade_hs = comtrade_hs
  ))
}

# =============================================================================
# SECTION 3: GCC-STAT FUSION REGISTRY DATA PIPELINES
# =============================================================================
#
# Data pipelines connecting to the GCC-Stat Fusion Registry (MARSA
# Dissemination Warehouse). Supports two connection modes:
#
#   1. SDMX REST API  (primary, recommended for production)
#   2. Direct PostgreSQL (fallback, useful for development/exploration)
#
# All Fusion loaders return data in identical format to their CSV
# counterparts in Section 2, enabling seamless source switching via
# load_gcc_data(source = "fusion").
#
# Environment variables (set in .Renviron or Sys.setenv()):
#
#   GCCSTAT_FUSION_URL  - SDMX REST API base URL
#   GCCSTAT_DB_HOST     - PostgreSQL host (direct DB mode)
#   GCCSTAT_DB_PORT     - PostgreSQL port (default: 5432)
#   GCCSTAT_DB_NAME     - Database name
#   GCCSTAT_DB_USER     - Database username
#   GCCSTAT_DB_PASSWORD - Database password
#
# Adapted from: data-raw/scripts/01_extract_fusion.R
# =============================================================================

# --- 3.1 Fusion Registry Configuration ----------------------------------------

#' Fusion Registry Configuration
#'
#' Central configuration for SDMX REST API and PostgreSQL access to the
#' GCC-Stat Fusion Registry / MARSA Dissemination Warehouse.
#' All credentials are read from environment variables.
FUSION_CONFIG <- list(
  # SDMX REST API settings
  sdmx = list(
    base_url = Sys.getenv("GCCSTAT_FUSION_URL",
                          "https://registry.gccstat.org/sdmx/v2"),
    format   = "csv",
    agency   = "GCCSTAT",
    version  = "1.0"
  ),

  # PostgreSQL direct connection settings
  db = list(
    host     = Sys.getenv("GCCSTAT_DB_HOST", ""),
    port     = as.integer(Sys.getenv("GCCSTAT_DB_PORT", "5432")),
    dbname   = Sys.getenv("GCCSTAT_DB_NAME", ""),
    user     = Sys.getenv("GCCSTAT_DB_USER", ""),
    password = Sys.getenv("GCCSTAT_DB_PASSWORD", "")
  ),

  # Dataflow mappings: internal name -> SDMX dataflow ID / DB table name
  # Table names match the views exposed by the MARSA Dissemination Warehouse.
  dataflows = list(
    national_accounts = list(id = "DF_ES_NA",
                             table = "DF_ES_NA",
                             desc = "National Accounts"),
    cpi               = list(id = "DF_ES_CPI",
                             table = "DF_ES_CPI",
                             desc = "Consumer Price Index"),
    monetary          = list(id = "DF_ES_MF",
                             table = "DF_ES_MF",
                             desc = "Monetary & Financial"),
    labor             = list(id = "DF_PSS_LAB",
                             table = "DF_PSS_LAB",
                             desc = "Labour Force"),
    population        = list(id = "DF_PSS_DEM_POP",
                             table = "DF_PSS_DEM_POP",
                             desc = "Population & Demographics"),
    energy            = list(id = "DF_GEETS_ENR",
                             table = "DF_GEETS_ENR",
                             desc = "Energy"),
    tourism           = list(id = "DF_GEETS_TUR",
                             table = "DF_GEETS_TUR",
                             desc = "Tourism"),
    common_market     = list(id = "DF_Common_Market_Tables",
                             table = "DF_Common_Market_Tables",
                             desc = "Common Market Indicators")
  ),

  # Country name mapping: Fusion Registry labels -> standard GCC names.
  # Handles the _en (English) label variants found in the MARSA warehouse.
  country_recode = c(
    "United Arab Emirates"     = "UAE",
    "The United Arab Emirates" = "UAE",
    "Emirates"                 = "UAE",
    "Kingdom of Saudi Arabia"  = "Saudi Arabia",
    "Kingdom of Bahrain"       = "Bahrain",
    "State of Kuwait"          = "Kuwait",
    "Sultanate of Oman"        = "Oman",
    "State of Qatar"           = "Qatar"
  ),

  # GCC country SDMX codes (REF_AREA dimension values)
  gcc_sdmx_codes = c("AE", "BH", "SA", "OM", "QA", "KW"),

  # Default extraction year range
  start_year = 2015L,
  end_year   = 2023L
)

# --- 3.2 Connection Management ------------------------------------------------

#' Check if SDMX REST API is configured and reachable
#'
#' @return TRUE if the SDMX endpoint responds, FALSE otherwise
fusion_check_sdmx <- function() {
  base_url <- FUSION_CONFIG$sdmx$base_url
  if (nchar(base_url) == 0) return(FALSE)

  tryCatch({
    resp <- httr::HEAD(base_url, httr::timeout(10))
    httr::status_code(resp) < 500
  }, error = function(e) {
    FALSE
  })
}

#' Connect to MARSA Dissemination Warehouse via PostgreSQL
#'
#' Requires DBI and RPostgres packages. Connection parameters are read
#' from FUSION_CONFIG (populated from environment variables).
#'
#' @return DBI connection object
#' @export
fusion_connect_db <- function() {
  cfg <- FUSION_CONFIG$db

  if (nchar(cfg$host) == 0 || nchar(cfg$user) == 0) {
    stop("Fusion DB not configured. Set GCCSTAT_DB_HOST and GCCSTAT_DB_USER ",
         "environment variables (e.g. in .Renviron).")
  }
  if (nchar(cfg$password) == 0) {
    stop("Database password not set. Set GCCSTAT_DB_PASSWORD in .Renviron.")
  }

  if (!requireNamespace("DBI", quietly = TRUE) ||
      !requireNamespace("RPostgres", quietly = TRUE)) {
    stop("Packages 'DBI' and 'RPostgres' are required for direct DB access.\n",
         "Install with: install.packages(c('DBI', 'RPostgres'))")
  }

  message("Connecting to MARSA warehouse...")
  message("  Host: ", cfg$host, ":", cfg$port)
  message("  Database: ", cfg$dbname)

  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host     = cfg$host,
    port     = cfg$port,
    dbname   = cfg$dbname,
    user     = cfg$user,
    password = cfg$password
  )

  # Verify connection
  test <- DBI::dbGetQuery(con, "SELECT 1 AS test")
  if (nrow(test) == 1) message("  Connection successful")


  return(con)
}

#' Disconnect from MARSA warehouse
#'
#' @param con DBI connection object
fusion_disconnect_db <- function(con) {
  if (!is.null(con) && DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con)
  }
}

# --- 3.3 Core Data Retrieval -------------------------------------------------

#' Fetch data from Fusion Registry via SDMX REST API
#'
#' Builds an SDMX 2.1 REST query URL and returns the response as a tibble.
#' Uses SDMX-CSV format with labels for human-readable dimension values.
#'
#' @param dataflow_name Name from FUSION_CONFIG$dataflows (e.g. "national_accounts")
#' @param key SDMX key filter (default: "." for all; use "AE+BH.A.." style for filtering)
#' @param start_year Start of time range (default: FUSION_CONFIG$start_year)
#' @param end_year End of time range (default: FUSION_CONFIG$end_year)
#' @return Tibble with raw SDMX-CSV data, or NULL on failure
fusion_fetch_sdmx <- function(dataflow_name,
                              key = ".",
                              start_year = FUSION_CONFIG$start_year,
                              end_year   = FUSION_CONFIG$end_year) {

  flow <- FUSION_CONFIG$dataflows[[dataflow_name]]
  if (is.null(flow)) {
    warning("Unknown dataflow: ", dataflow_name)
    return(NULL)
  }

  cfg <- FUSION_CONFIG$sdmx

  # Build SDMX 2.1 REST data query URL:
  # {base}/data/{agency},{id},{version}/{key}?startPeriod=...&endPeriod=...&format=csv
  flow_ref <- paste(cfg$agency, flow$id, cfg$version, sep = ",")
  url <- paste0(cfg$base_url, "/data/", flow_ref, "/", key)

  query_params <- list(
    startPeriod = as.character(start_year),
    endPeriod   = as.character(end_year),
    format      = "csv",
    labels      = "both"
  )

  message("  SDMX query: ", flow$desc, " [", flow$id, "]")

  tryCatch({
    resp <- httr::GET(url, query = query_params, httr::timeout(60))

    if (httr::status_code(resp) != 200) {
      warning("SDMX query failed (HTTP ", httr::status_code(resp), ") for: ",
              flow$id)
      return(NULL)
    }

    body <- httr::content(resp, as = "text", encoding = "UTF-8")
    df <- readr::read_csv(body, show_col_types = FALSE)
    message("  Retrieved ", nrow(df), " records via SDMX")
    return(df)

  }, error = function(e) {
    warning("SDMX fetch error for ", flow$id, ": ", e$message)
    return(NULL)
  })
}

#' Fetch data from Fusion Registry via direct PostgreSQL query
#'
#' Queries the MARSA Dissemination Warehouse table corresponding to the
#' given dataflow. Returns all columns with English labels (_en suffix).
#'
#' @param dataflow_name Name from FUSION_CONFIG$dataflows
#' @param con DBI connection (if NULL, a new connection is created and closed)
#' @param columns SQL SELECT column expressions. Default selects all columns.
#' @param extra_where Additional SQL WHERE clauses (character vector, AND-joined)
#' @return Tibble with raw data, or NULL on failure
fusion_fetch_db <- function(dataflow_name,
                            con = NULL,
                            columns = "*",
                            extra_where = NULL) {

  flow <- FUSION_CONFIG$dataflows[[dataflow_name]]
  if (is.null(flow)) {
    warning("Unknown dataflow: ", dataflow_name)
    return(NULL)
  }

  own_con <- is.null(con)
  if (own_con) {
    con <- fusion_connect_db()
    on.exit(fusion_disconnect_db(con), add = TRUE)
  }

  quoted_table <- DBI::dbQuoteIdentifier(con, flow$table)
  col_clause <- paste(columns, collapse = ", ")

  where_parts <- '"OBS_VALUE" IS NOT NULL'
  if (!is.null(extra_where)) {
    where_parts <- paste(c(where_parts, extra_where), collapse = " AND ")
  }

  query <- paste0("SELECT ", col_clause, " FROM ", quoted_table,
                   " WHERE ", where_parts)

  message("  DB query: ", flow$desc, " [", flow$table, "]")

  tryCatch({
    df <- DBI::dbGetQuery(con, query)
    message("  Retrieved ", nrow(df), " records from DB")
    return(tibble::as_tibble(df))
  }, error = function(e) {
    warning("DB fetch error for ", flow$table, ": ", e$message)
    return(NULL)
  })
}

#' Fetch data from Fusion Registry (dispatcher)
#'
#' Tries SDMX REST API first; falls back to direct PostgreSQL if SDMX
#' is unavailable or fails.
#'
#' @param dataflow_name Name from FUSION_CONFIG$dataflows
#' @param method Connection method: "auto" (try SDMX then DB), "sdmx", or "db"
#' @param con Optional DBI connection for DB mode (reuse across calls)
#' @param start_year Start year filter
#' @param end_year End year filter
#' @return Tibble with raw data, or NULL if all methods fail
fusion_fetch <- function(dataflow_name,
                         method     = c("auto", "sdmx", "db"),
                         con        = NULL,
                         start_year = FUSION_CONFIG$start_year,
                         end_year   = FUSION_CONFIG$end_year) {

  method <- match.arg(method)

  if (method %in% c("auto", "sdmx")) {
    result <- fusion_fetch_sdmx(dataflow_name,
                                start_year = start_year,
                                end_year = end_year)
    if (!is.null(result) && nrow(result) > 0) return(result)

    if (method == "sdmx") {
      warning("SDMX fetch failed for ", dataflow_name, " (no DB fallback)")
      return(NULL)
    }
    message("  SDMX unavailable, falling back to DB...")
  }

  # DB path
  fusion_fetch_db(dataflow_name, con = con)
}

# --- 3.4 Parsing & Transformation Helpers ------------------------------------

#' Parse year from SDMX TIME_PERIOD values
#'
#' Handles multiple time period formats:
#'   "2023"       -> 2023 (annual)
#'   "2023-Q1"    -> 2023 (quarterly)
#'   "2023Q1"     -> 2023 (quarterly, no separator)
#'   "2023-01"    -> 2023 (monthly)
#'   "2023-M01"   -> 2023 (monthly, ISO)
#'
#' @param time_period Character vector of TIME_PERIOD values
#' @return Integer vector of years
fusion_parse_year <- function(time_period) {
  as.integer(str_sub(as.character(time_period), 1, 4))
}

#' Parse quarter from SDMX TIME_PERIOD values
#'
#' @param time_period Character vector of TIME_PERIOD values
#' @return Character vector of quarter codes ("Q1"-"Q4") or NA
fusion_parse_quarter <- function(time_period) {
  tp <- as.character(time_period)
  if_else(str_detect(tp, "Q"), str_extract(tp, "Q[1-4]"), NA_character_)
}

#' Standardize Fusion Registry country names
#'
#' Applies FUSION_CONFIG$country_recode mapping and then falls through to
#' the main standardize_countries() logic for any remaining variants.
#'
#' @param country Character vector of country names (from Fusion _en columns)
#' @return Character vector with standardized names
fusion_recode_country <- function(country) {
  recode_map <- FUSION_CONFIG$country_recode
  recoded <- recode_map[country]
  # recode_map returns NA for unmatched names; keep original in that case
  if_else(is.na(recoded), country, recoded)
}

#' Filter to GCC countries and year range
#'
#' Common post-processing step for all Fusion loaders.
#'
#' @param df Tibble with country and year columns
#' @param start_year Start of range
#' @param end_year End of range
#' @return Filtered tibble
fusion_filter_gcc <- function(df,
                              start_year = FUSION_CONFIG$start_year,
                              end_year   = FUSION_CONFIG$end_year) {
  df %>%
    filter(
      country %in% GCC_CONFIG$countries,
      year >= start_year,
      year <= end_year,
      !is.na(value)
    )
}

#' Resolve Fusion column names to standard names
#'
#' The MARSA PostgreSQL views use "_en" suffixed columns for English labels
#' (e.g. "COUNTRY_en"), while SDMX-CSV uses label columns alongside coded
#' columns (e.g. "Reference area" next to "REF_AREA"). This helper renames
#' whichever format is present to our standard internal names.
#'
#' @param df Tibble from fusion_fetch()
#' @param mapping Named character vector: standard_name = "fusion_column_name"
#' @return Tibble with renamed columns (unmatched columns are kept as-is)
fusion_rename_columns <- function(df, mapping) {
  present <- names(df)
  for (std_name in names(mapping)) {
    fusion_name <- mapping[[std_name]]
    # Try exact match first, then _en variant
    if (fusion_name %in% present) {
      names(df)[names(df) == fusion_name] <- std_name
    } else {
      en_name <- paste0(fusion_name, "_en")
      if (en_name %in% present) {
        names(df)[names(df) == en_name] <- std_name
      }
    }
  }
  return(df)
}

# --- 3.5 Dataset-Specific Fusion Loaders --------------------------------------
#
# Each function below fetches data from the Fusion Registry and transforms
# it to match the exact output format of the corresponding CSV loader in
# Section 2. This ensures downstream indicator modules work identically
# regardless of data source.
# -----------------------------------------------------------------------------

#' Load National Accounts from Fusion Registry
#'
#' Returns data in same format as load_national_accounts_csv():
#'   country, frequency, price_type, indicator, unit, time_period, value,
#'   year, quarter, source
#'
#' @param method "auto", "sdmx", or "db"
#' @param con Optional DBI connection for DB mode
#' @param start_year Start of extraction range
#' @param end_year End of extraction range
#' @return Tibble matching load_national_accounts_csv() output, or NULL
load_national_accounts_fusion <- function(method     = "auto",
                                          con        = NULL,
                                          start_year = FUSION_CONFIG$start_year,
                                          end_year   = FUSION_CONFIG$end_year) {

  message("Extracting National Accounts from Fusion...")

  # For DB mode, use targeted column selection (known schema)
  if (method == "db" || (method == "auto" && nchar(FUSION_CONFIG$db$host) > 0)) {
    db_columns <- c(
      '"COUNTRY_en" AS country',
      '"FREQUENCY_en" AS frequency',
      '"PRICES TYPES_en" AS price_type',
      '"INDICATOR_en" AS indicator',
      '"UNIT_en" AS unit',
      '"TIME_PERIOD" AS time_period',
      '"OBS_VALUE" AS value'
    )
    raw <- fusion_fetch_db("national_accounts", con = con, columns = db_columns)
  } else {
    raw <- fusion_fetch_sdmx("national_accounts",
                             start_year = start_year,
                             end_year = end_year)
    if (!is.null(raw)) {
      raw <- fusion_rename_columns(raw, c(
        country    = "COUNTRY",
        frequency  = "FREQUENCY",
        price_type = "PRICES TYPES",
        indicator  = "INDICATOR",
        unit       = "UNIT",
        time_period = "TIME_PERIOD",
        value      = "OBS_VALUE"
      ))
    }
  }

  if (is.null(raw) || nrow(raw) == 0) {
    warning("No National Accounts data retrieved from Fusion")
    return(NULL)
  }

  result <- raw %>%
    mutate(
      country    = fusion_recode_country(country),
      year       = fusion_parse_year(time_period),
      quarter    = fusion_parse_quarter(time_period),
      value      = as.numeric(value),
      source     = "national_accounts"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, frequency, price_type, indicator, unit,
           time_period, value, year, quarter, source)

  message("  Processed: ", nrow(result), " records")
  return(result)
}

#' Load CPI from Fusion Registry
#'
#' Returns data in same format as load_cpi_csv():
#'   country, unit, frequency, indicator, time_period, value, year, source
#'
#' @inheritParams load_national_accounts_fusion
#' @return Tibble matching load_cpi_csv() output, or NULL
load_cpi_fusion <- function(method     = "auto",
                            con        = NULL,
                            start_year = FUSION_CONFIG$start_year,
                            end_year   = FUSION_CONFIG$end_year) {

  message("Extracting CPI from Fusion...")

  if (method == "db" || (method == "auto" && nchar(FUSION_CONFIG$db$host) > 0)) {
    db_columns <- c(
      '"COUNTRY_en" AS country',
      '"UNIT_en" AS unit',
      '"FREQUENCY_en" AS frequency',
      '"INDICATOR_en" AS indicator',
      '"TIME_PERIOD" AS time_period',
      '"OBS_VALUE" AS value'
    )
    raw <- fusion_fetch_db("cpi", con = con, columns = db_columns)
  } else {
    raw <- fusion_fetch_sdmx("cpi", start_year = start_year, end_year = end_year)
    if (!is.null(raw)) {
      raw <- fusion_rename_columns(raw, c(
        country   = "COUNTRY",
        unit      = "UNIT",
        frequency = "FREQUENCY",
        indicator = "INDICATOR",
        time_period = "TIME_PERIOD",
        value     = "OBS_VALUE"
      ))
    }
  }

  if (is.null(raw) || nrow(raw) == 0) {
    warning("No CPI data retrieved from Fusion")
    return(NULL)
  }

  result <- raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = fusion_parse_year(time_period),
      value   = as.numeric(value),
      source  = "cpi"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, unit, frequency, indicator, time_period, value, year, source)

  message("  Processed: ", nrow(result), " records")
  return(result)
}

#' Load Monetary & Financial Data from Fusion Registry
#'
#' Returns data in same format as load_monetary_csv():
#'   country, frequency, indicator, unit, time_period, value, year, source
#'
#' @inheritParams load_national_accounts_fusion
#' @return Tibble matching load_monetary_csv() output, or NULL
load_monetary_fusion <- function(method     = "auto",
                                 con        = NULL,
                                 start_year = FUSION_CONFIG$start_year,
                                 end_year   = FUSION_CONFIG$end_year) {

  message("Extracting Monetary & Financial from Fusion...")

  if (method == "db" || (method == "auto" && nchar(FUSION_CONFIG$db$host) > 0)) {
    db_columns <- c(
      '"COUNTRY_en" AS country',
      '"FREQUENCY_en" AS frequency',
      '"INDICATOR_en" AS indicator',
      '"UNIT_en" AS unit',
      '"TIME_PERIOD" AS time_period',
      '"OBS_VALUE" AS value'
    )
    raw <- fusion_fetch_db("monetary", con = con, columns = db_columns)
  } else {
    raw <- fusion_fetch_sdmx("monetary", start_year = start_year, end_year = end_year)
    if (!is.null(raw)) {
      raw <- fusion_rename_columns(raw, c(
        country   = "COUNTRY",
        frequency = "FREQUENCY",
        indicator = "INDICATOR",
        unit      = "UNIT",
        time_period = "TIME_PERIOD",
        value     = "OBS_VALUE"
      ))
    }
  }

  if (is.null(raw) || nrow(raw) == 0) {
    warning("No Monetary data retrieved from Fusion")
    return(NULL)
  }

  result <- raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = if_else(
        str_detect(time_period, "-"),
        as.integer(str_sub(time_period, 1, 4)),
        as.integer(time_period)
      ),
      value  = as.numeric(value),
      source = "monetary"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, frequency, indicator, unit, time_period, value, year, source)

  message("  Processed: ", nrow(result), " records")
  return(result)
}

#' Load Labour Force Data from Fusion Registry
#'
#' Returns data in same format as load_labor_csv():
#'   country, indicator, labour_aspect, nationality, unit, gender,
#'   frequency, time_period, value, year, quarter, source
#'
#' @inheritParams load_national_accounts_fusion
#' @return Tibble matching load_labor_csv() output, or NULL
load_labor_fusion <- function(method     = "auto",
                              con        = NULL,
                              start_year = FUSION_CONFIG$start_year,
                              end_year   = FUSION_CONFIG$end_year) {

  message("Extracting Labour Force from Fusion...")

  if (method == "db" || (method == "auto" && nchar(FUSION_CONFIG$db$host) > 0)) {
    db_columns <- c(
      '"COUNTRY_en" AS country',
      '"INDICATOR_en" AS indicator',
      '"LABOUR ASPECT_en" AS labour_aspect',
      '"NATIONALITY_en" AS nationality',
      '"UNIT_en" AS unit',
      '"GENDER_en" AS gender',
      '"FREQUENCY_en" AS frequency',
      '"TIME_PERIOD" AS time_period',
      '"OBS_VALUE" AS value'
    )
    raw <- fusion_fetch_db("labor", con = con, columns = db_columns)
  } else {
    raw <- fusion_fetch_sdmx("labor", start_year = start_year, end_year = end_year)
    if (!is.null(raw)) {
      raw <- fusion_rename_columns(raw, c(
        country       = "COUNTRY",
        indicator     = "INDICATOR",
        labour_aspect = "LABOUR ASPECT",
        nationality   = "NATIONALITY",
        unit          = "UNIT",
        gender        = "GENDER",
        frequency     = "FREQUENCY",
        time_period   = "TIME_PERIOD",
        value         = "OBS_VALUE"
      ))
    }
  }

  if (is.null(raw) || nrow(raw) == 0) {
    warning("No Labour data retrieved from Fusion")
    return(NULL)
  }

  result <- raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = as.integer(str_sub(time_period, 1, 4)),
      quarter = fusion_parse_quarter(time_period),
      value   = as.numeric(value),
      source  = "labor"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, indicator, labour_aspect, nationality, unit, gender,
           frequency, time_period, value, year, quarter, source)

  message("  Processed: ", nrow(result), " records")
  return(result)
}

#' Load Population Data from Fusion Registry
#'
#' Returns data in same format as load_population_csv():
#'   country, nationality, gender, age, frequency, year, value, source
#'
#' @inheritParams load_national_accounts_fusion
#' @return Tibble matching load_population_csv() output, or NULL
load_population_fusion <- function(method     = "auto",
                                   con        = NULL,
                                   start_year = FUSION_CONFIG$start_year,
                                   end_year   = FUSION_CONFIG$end_year) {

  message("Extracting Population from Fusion...")

  if (method == "db" || (method == "auto" && nchar(FUSION_CONFIG$db$host) > 0)) {
    db_columns <- c(
      '"COUNTRY_en" AS country',
      '"NATIONALITY_en" AS nationality',
      '"GENDER_en" AS gender',
      '"AGE_en" AS age',
      '"FREQUENCY_en" AS frequency',
      '"TIME_PERIOD" AS year',
      '"OBS_VALUE" AS value'
    )
    raw <- fusion_fetch_db("population", con = con, columns = db_columns)
  } else {
    raw <- fusion_fetch_sdmx("population",
                             start_year = start_year, end_year = end_year)
    if (!is.null(raw)) {
      raw <- fusion_rename_columns(raw, c(
        country     = "COUNTRY",
        nationality = "NATIONALITY",
        gender      = "GENDER",
        age         = "AGE",
        frequency   = "FREQUENCY",
        year        = "TIME_PERIOD",
        value       = "OBS_VALUE"
      ))
    }
  }

  if (is.null(raw) || nrow(raw) == 0) {
    warning("No Population data retrieved from Fusion")
    return(NULL)
  }

  result <- raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = as.integer(year),
      value   = as.numeric(value),
      source  = "population"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, nationality, gender, age, frequency, year, value, source)

  message("  Processed: ", nrow(result), " records")
  return(result)
}

#' Load Energy Data from Fusion Registry
#'
#' Returns data in same format as load_energy_csv():
#'   country, product, flow, indicator, unit, year, value, source
#'
#' @inheritParams load_national_accounts_fusion
#' @return Tibble matching load_energy_csv() output, or NULL
load_energy_fusion <- function(method     = "auto",
                               con        = NULL,
                               start_year = FUSION_CONFIG$start_year,
                               end_year   = FUSION_CONFIG$end_year) {

  message("Extracting Energy from Fusion...")

  if (method == "db" || (method == "auto" && nchar(FUSION_CONFIG$db$host) > 0)) {
    db_columns <- c(
      '"COUNTRY_en" AS country',
      '"PRODUCT_en" AS product',
      '"FLOW_en" AS flow',
      '"INDICATOR_en" AS indicator',
      '"UNIT_en" AS unit',
      '"TIME_PERIOD" AS year',
      '"OBS_VALUE" AS value'
    )
    raw <- fusion_fetch_db("energy", con = con, columns = db_columns)
  } else {
    raw <- fusion_fetch_sdmx("energy",
                             start_year = start_year, end_year = end_year)
    if (!is.null(raw)) {
      raw <- fusion_rename_columns(raw, c(
        country   = "COUNTRY",
        product   = "PRODUCT",
        flow      = "FLOW",
        indicator = "INDICATOR",
        unit      = "UNIT",
        year      = "TIME_PERIOD",
        value     = "OBS_VALUE"
      ))
    }
  }

  if (is.null(raw) || nrow(raw) == 0) {
    warning("No Energy data retrieved from Fusion")
    return(NULL)
  }

  result <- raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = as.integer(year),
      value   = as.numeric(value),
      source  = "energy"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, product, flow, indicator, unit, year, value, source)

  message("  Processed: ", nrow(result), " records")
  return(result)
}

#' Load Tourism Data from Fusion Registry
#'
#' Returns data in same format as load_tourism_csv():
#'   country, partner, indicator, frequency, year, value, source
#'
#' @inheritParams load_national_accounts_fusion
#' @return Tibble matching load_tourism_csv() output, or NULL
load_tourism_fusion <- function(method     = "auto",
                                con        = NULL,
                                start_year = FUSION_CONFIG$start_year,
                                end_year   = FUSION_CONFIG$end_year) {

  message("Extracting Tourism from Fusion...")

  if (method == "db" || (method == "auto" && nchar(FUSION_CONFIG$db$host) > 0)) {
    db_columns <- c(
      '"COUNTRY_en" AS country',
      '"PARTENER COUNTRY_en" AS partner',
      '"INDICATOR_en" AS indicator',
      '"FREQUENCY_en" AS frequency',
      '"TIME_PERIOD" AS year',
      '"OBS_VALUE" AS value'
    )
    raw <- fusion_fetch_db("tourism", con = con, columns = db_columns)
  } else {
    raw <- fusion_fetch_sdmx("tourism",
                             start_year = start_year, end_year = end_year)
    if (!is.null(raw)) {
      raw <- fusion_rename_columns(raw, c(
        country   = "COUNTRY",
        partner   = "PARTENER COUNTRY",
        indicator = "INDICATOR",
        frequency = "FREQUENCY",
        year      = "TIME_PERIOD",
        value     = "OBS_VALUE"
      ))
    }
  }

  if (is.null(raw) || nrow(raw) == 0) {
    warning("No Tourism data retrieved from Fusion")
    return(NULL)
  }

  result <- raw %>%
    mutate(
      country = fusion_recode_country(country),
      year    = as.integer(year),
      value   = as.numeric(value),
      source  = "tourism"
    ) %>%
    fusion_filter_gcc(start_year, end_year) %>%
    select(country, partner, indicator, frequency, year, value, source)

  message("  Processed: ", nrow(result), " records")
  return(result)
}

#' Load Common Market Tables from Fusion Registry
#'
#' Returns data in same format as load_common_market_csv():
#'   country_code, country, track, track_name, indicator_code, indicator,
#'   citizen_code, citizen, sex_code, sex, year, value, source,
#'   citizen_country, host_country
#'
#' The Common Market table has paired code/label columns for each dimension.
#' The Fusion Registry stores these as separate coded and _en columns.
#'
#' @inheritParams load_national_accounts_fusion
#' @return Tibble matching load_common_market_csv() output, or NULL
load_common_market_fusion <- function(method     = "auto",
                                      con        = NULL,
                                      start_year = FUSION_CONFIG$start_year,
                                      end_year   = FUSION_CONFIG$end_year) {

  message("Extracting Common Market Tables from Fusion...")

  # Common Market has paired code/label columns - fetch all and map
  raw <- fusion_fetch("common_market", method = method, con = con,
                      start_year = start_year, end_year = end_year)

  if (is.null(raw) || nrow(raw) == 0) {
    warning("No Common Market data retrieved from Fusion")
    return(NULL)
  }

  cols <- names(raw)

  # Resolve column names from Fusion format.
  # DB format: "COUNTRY", "COUNTRY_en", "TRACK", "TRACK_en", etc.
  # SDMX-CSV format may vary; we handle both patterns.
  find_col <- function(patterns) {
    for (p in patterns) {
      match <- cols[grepl(p, cols, ignore.case = TRUE)]
      if (length(match) > 0) return(match[1])
    }
    return(NA_character_)
  }

  # Build renaming map for Common Market paired columns
  cm_col_map <- list(
    country_code   = find_col(c("^COUNTRY$", "COUNTRY_id", "REF_AREA$")),
    country        = find_col(c("COUNTRY_en$", "^Country\\.\\.\\.2$",
                                "Reference area")),
    track          = find_col(c("^TRACK$", "TRACK_id", "^Track\\.\\.\\.3$")),
    track_name     = find_col(c("TRACK_en$", "^Track\\.\\.\\.4$")),
    indicator_code = find_col(c("^INDICATOR$", "INDICATOR_id",
                                "^Indicator\\.\\.\\.5$")),
    indicator      = find_col(c("INDICATOR_en$", "^Indicator\\.\\.\\.6$")),
    citizen_code   = find_col(c("^CITIZEN$", "CITIZEN_id",
                                "^Citizen\\.\\.\\.7$")),
    citizen        = find_col(c("CITIZEN_en$", "^Citizen\\.\\.\\.8$")),
    sex_code       = find_col(c("^SEX$", "SEX_id", "^Sex\\.\\.\\.9$")),
    sex            = find_col(c("SEX_en$", "^Sex\\.\\.\\.10$")),
    year           = find_col(c("TIME_PERIOD")),
    value          = find_col(c("OBS_VALUE"))
  )

  # Apply renaming (skip NAs)
  for (std_name in names(cm_col_map)) {
    fusion_col <- cm_col_map[[std_name]]
    if (!is.na(fusion_col) && fusion_col %in% cols) {
      names(raw)[names(raw) == fusion_col] <- std_name
    }
  }

  result <- raw %>%
    select(any_of(c("country_code", "country", "track", "track_name",
                     "indicator_code", "indicator", "citizen_code", "citizen",
                     "sex_code", "sex", "year", "value"))) %>%
    mutate(
      year  = as.integer(year),
      value = as.numeric(value),
      source = "common_market"
    ) %>%
    filter(!is.na(value)) %>%
    # Map citizenship to standard country names
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

  message("  Processed: ", nrow(result), " records")
  return(result)
}

# --- 3.6 Master Fusion Loader ------------------------------------------------

#' Load All GCC-Stat Datasets from Fusion Registry
#'
#' Equivalent to load_gcc_data() but pulls from the Fusion Registry / MARSA
#' warehouse instead of local CSV files. Returns a named list with identical
#' structure, so all downstream indicator modules work without modification.
#'
#' @param method Connection method: "auto" (SDMX first, DB fallback), "sdmx", "db"
#' @param start_year Start of extraction range (default: 2015)
#' @param end_year End of extraction range (default: 2023)
#' @param standardize Apply country name standardization (default: TRUE)
#' @param remove_aggregates Remove GCC aggregate records (default: TRUE)
#' @param verbose Print progress messages (default: TRUE)
#' @param cache_dir If non-NULL, save/load cached extracts from this directory
#' @return Named list of dataframes, same structure as load_gcc_data()
#' @export
load_gcc_data_fusion <- function(method            = "auto",
                                 start_year        = FUSION_CONFIG$start_year,
                                 end_year          = FUSION_CONFIG$end_year,
                                 standardize       = TRUE,
                                 remove_aggregates = TRUE,
                                 verbose           = TRUE,
                                 cache_dir         = NULL) {

  if (verbose) {
    message("=======================================================")
    message("  GCC EII - FUSION REGISTRY DATA LOADING")
    message("=======================================================\n")
    message("  Method: ", method)
    message("  Range:  ", start_year, " - ", end_year, "\n")
  }

  # Check for cached data
  if (!is.null(cache_dir)) {
    cache_file <- file.path(cache_dir,
                            paste0("fusion_cache_", start_year, "_", end_year, ".rds"))
    if (file.exists(cache_file)) {
      cache_age <- difftime(Sys.time(), file.info(cache_file)$mtime, units = "hours")
      if (cache_age < 24) {
        if (verbose) message("  Loading from cache (", round(cache_age, 1), "h old)")
        return(readRDS(cache_file))
      }
    }
  }

  # Open a single DB connection if using DB mode (avoids repeated connect/disconnect)
  con <- NULL
  if (method %in% c("db", "auto") && nchar(FUSION_CONFIG$db$host) > 0) {
    tryCatch({
      con <- fusion_connect_db()
    }, error = function(e) {
      if (verbose) message("  DB connection failed: ", e$message)
      con <<- NULL
    })
  }
  on.exit(fusion_disconnect_db(con), add = TRUE)

  # Extract each dataset
  data_list <- list()

  extract_one <- function(name, loader_fn) {
    if (verbose) message("\n--- ", toupper(name), " ---")
    tryCatch(
      loader_fn(method = method, con = con,
                start_year = start_year, end_year = end_year),
      error = function(e) {
        warning(name, " extraction failed: ", e$message)
        NULL
      }
    )
  }

  data_list$national_accounts <- extract_one("national_accounts",
                                              load_national_accounts_fusion)
  data_list$cpi       <- extract_one("cpi", load_cpi_fusion)
  data_list$monetary  <- extract_one("monetary", load_monetary_fusion)
  data_list$labor     <- extract_one("labor", load_labor_fusion)
  data_list$population <- extract_one("population", load_population_fusion)
  data_list$energy    <- extract_one("energy", load_energy_fusion)
  data_list$tourism   <- extract_one("tourism", load_tourism_fusion)
  data_list$common_market <- extract_one("common_market",
                                          load_common_market_fusion)

  # FDI and ICP are external sources - not in Fusion Registry.
  # Load from CSV fallback if available.
  if (verbose) message("\n--- EXTERNAL SOURCES (CSV fallback) ---")
  data_list$fdi <- tryCatch(load_fdi_csv("data-raw"), error = function(e) NULL)
  data_list$icp <- tryCatch(load_icp_csv("data-raw"), error = function(e) NULL)

  # Comtrade data (from RDS files)
  if (verbose) message("  Loading Comtrade data...")
  comtrade_data <- tryCatch(load_comtrade_data("data-raw"),
                            error = function(e) list(comtrade = NULL,
                                                      comtrade_hs = NULL))
  data_list$comtrade    <- comtrade_data$comtrade
  data_list$comtrade_hs <- comtrade_data$comtrade_hs

  # Standardize country names (same logic as CSV path)
  if (standardize) {
    if (verbose) message("\n  Standardizing country names...")
    data_list <- lapply(data_list, function(df) {
      if (!is.null(df) && "country" %in% names(df)) {
        standardize_countries(df)
      } else {
        df
      }
    })
  }

  # Remove GCC aggregates
  if (remove_aggregates) {
    if (verbose) message("  Removing GCC aggregates...")
    for (ds in c("national_accounts", "population", "cpi",
                 "labor", "monetary", "energy")) {
      if (!is.null(data_list[[ds]])) {
        data_list[[ds]] <- remove_gcc_aggregates(data_list[[ds]])
      }
    }
  }

  # Summary
  if (verbose) {
    loaded_count <- sum(sapply(data_list, function(x) !is.null(x)))
    message("\n=======================================================")
    message("  FUSION EXTRACTION COMPLETE")
    message("  Loaded ", loaded_count, " of ", length(data_list), " datasets")
    message("=======================================================\n")
  }

  # Save cache
  if (!is.null(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
    saveRDS(data_list, cache_file)
    if (verbose) message("  Cached to: ", cache_file)
  }

  # Attach extraction metadata
  attr(data_list, "extraction_metadata") <- list(
    timestamp  = Sys.time(),
    source     = "fusion_registry",
    method     = method,
    start_year = start_year,
    end_year   = end_year
  )

  return(data_list)
}

#' Discover available dataflows in Fusion Registry
#'
#' Utility function to check which datasets are accessible and report
#' their structure. Useful for debugging connection issues and exploring
#' new dataflows.
#'
#' @param method "sdmx" or "db"
#' @return Invisible list of discovery results
#' @export
fusion_discover <- function(method = "db") {

  message("=======================================================")
  message("  FUSION REGISTRY - DISCOVERY MODE")
  message("=======================================================\n")

  if (method == "db") {
    con <- fusion_connect_db()
    on.exit(fusion_disconnect_db(con), add = TRUE)

    for (name in names(FUSION_CONFIG$dataflows)) {
      flow <- FUSION_CONFIG$dataflows[[name]]
      message("--- ", name, " ---")
      message("  Table: ", flow$table)

      tryCatch({
        quoted <- DBI::dbQuoteIdentifier(con, flow$table)
        sample <- DBI::dbGetQuery(con,
                                  paste0("SELECT * FROM ", quoted, " LIMIT 1"))
        count <- DBI::dbGetQuery(con,
                                 paste0("SELECT COUNT(*) AS n FROM ", quoted))
        message("  Status: accessible")
        message("  Columns: ", paste(names(sample), collapse = ", "))
        message("  Rows: ", format(count$n, big.mark = ","))
      }, error = function(e) {
        message("  Status: NOT accessible (", e$message, ")")
      })
      message("")
    }
  } else {
    message("SDMX discovery: checking ", FUSION_CONFIG$sdmx$base_url, "\n")
    available <- fusion_check_sdmx()
    message("  API reachable: ", available)

    if (available) {
      for (name in names(FUSION_CONFIG$dataflows)) {
        flow <- FUSION_CONFIG$dataflows[[name]]
        message("--- ", name, " [", flow$id, "] ---")
        # Try fetching 1 record to verify dataflow
        test <- fusion_fetch_sdmx(name, start_year = 2023, end_year = 2023)
        if (!is.null(test)) {
          message("  Status: accessible (", nrow(test), " records for 2023)")
          message("  Columns: ", paste(names(test), collapse = ", "))
        } else {
          message("  Status: NOT accessible")
        }
        message("")
      }
    }
  }

  return(invisible(NULL))
}

# =============================================================================
# SECTION 4: DATA CLEANING & STANDARDIZATION
# =============================================================================

#' Standardize Country Names
#'
#' Converts all country name variants to standard names used in the index.
#' Also adds ISO3 country codes for COINr compatibility.
#'
#' @param df Dataframe with country column
#' @param country_col Name of the country column (default: "country")
#' @return Dataframe with standardized country names and ISO3 codes
standardize_countries <- function(df, country_col = "country") {
  
  if (!country_col %in% names(df)) {
    warning(paste("Column", country_col, "not found in dataframe"))
    return(df)
  }
  
  df %>%
    mutate(
      # Standardize country names
      !!sym(country_col) := case_when(
        str_detect(!!sym(country_col), regex("emirates|UAE|ARE", ignore_case = TRUE)) ~ "UAE",
        str_detect(!!sym(country_col), regex("saudi|KSA", ignore_case = TRUE)) ~ "Saudi Arabia",
        str_detect(!!sym(country_col), regex("bahrain|BHR", ignore_case = TRUE)) ~ "Bahrain",
        str_detect(!!sym(country_col), regex("kuwait|KWT", ignore_case = TRUE)) ~ "Kuwait",
        str_detect(!!sym(country_col), regex("oman|OMN", ignore_case = TRUE)) ~ "Oman",
        str_detect(!!sym(country_col), regex("qatar|QAT", ignore_case = TRUE)) ~ "Qatar",
        str_detect(!!sym(country_col), regex("GCC|gulf cooperation", ignore_case = TRUE)) ~ "GCC",
        TRUE ~ !!sym(country_col)
      ),
      # Add ISO3 code for COINr
      iso3 = case_when(
        !!sym(country_col) == "UAE" ~ "ARE",
        !!sym(country_col) == "Saudi Arabia" ~ "SAU",
        !!sym(country_col) == "Bahrain" ~ "BHR",
        !!sym(country_col) == "Kuwait" ~ "KWT",
        !!sym(country_col) == "Oman" ~ "OMN",
        !!sym(country_col) == "Qatar" ~ "QAT",
        !!sym(country_col) == "GCC" ~ "GCC",
        TRUE ~ NA_character_
      )
    )
}

#' Remove GCC Aggregate Records
#'
#' Filters out GCC-level aggregate records, keeping only individual countries
#'
#' @param df Dataframe with country column
#' @param country_col Name of the country column
#' @return Filtered dataframe
remove_gcc_aggregates <- function(df, country_col = "country") {
  
  df %>%
    filter(!.data[[country_col]] %in% GCC_CONFIG$aggregate_names)
}

#' Ensure All GCC Countries Present
#'
#' Adds missing GCC countries with NA values if not present in data
#'
#' @param df Dataframe with country column
#' @param fill_cols Columns to fill with NA for missing countries
#' @return Dataframe with all 6 GCC countries
ensure_all_gcc_countries <- function(df, fill_cols = NULL) {
  
  existing <- unique(df$country)
  missing <- setdiff(GCC_CONFIG$countries, existing)
  
  if (length(missing) > 0) {
    # Create rows for missing countries
    missing_rows <- tibble(country = missing)
    
    # Add other columns as NA
    if (!is.null(fill_cols)) {
      for (col in fill_cols) {
        missing_rows[[col]] <- NA
      }
    }
    
    # Add ISO3 codes
    missing_rows <- missing_rows %>%
      mutate(iso3 = case_when(
        country == "UAE" ~ "ARE",
        country == "Saudi Arabia" ~ "SAU",
        country == "Bahrain" ~ "BHR",
        country == "Kuwait" ~ "KWT",
        country == "Oman" ~ "OMN",
        country == "Qatar" ~ "QAT",
        TRUE ~ NA_character_
      ))
    
    df <- bind_rows(df, missing_rows)
  }
  
  return(df)
}

# =============================================================================
# SECTION 5: MAIN LOADING FUNCTION
# =============================================================================

#' Load All GCC-Stat Datasets
#'
#' Main entry point for loading all data sources required for the
#' GCC Economic Integration Index calculation. Supports multiple data sources:
#'   - "csv"    : Load from local CSV files in data_dir (default, no network needed)
#'   - "fusion" : Load from GCC-Stat Fusion Registry (SDMX API or PostgreSQL)
#'
#' When source = "fusion", the function delegates to load_gcc_data_fusion()
#' which connects to the MARSA Dissemination Warehouse. Requires environment
#' variables to be set (see Section 3 documentation).
#'
#' @param data_dir Directory containing CSV data files (used when source = "csv")
#' @param source Data source: "csv" (local files) or "fusion" (Fusion Registry)
#' @param fusion_method Connection method for Fusion: "auto", "sdmx", or "db"
#' @param standardize Logical, whether to standardize country names (default: TRUE)
#' @param remove_aggregates Logical, whether to remove GCC aggregates (default: TRUE)
#' @param verbose Logical, whether to print progress messages (default: TRUE)
#' @param cache_dir Cache directory for Fusion extracts (NULL to disable caching)
#' @return Named list of cleaned dataframes
#' @export
load_gcc_data <- function(data_dir = "data-raw",
                          source = c("csv", "fusion"),
                          fusion_method = "auto",
                          standardize = TRUE,
                          remove_aggregates = TRUE,
                          verbose = TRUE,
                          cache_dir = NULL) {

  source <- match.arg(source)

  # --- Fusion Registry path ---
  if (source == "fusion") {
    return(load_gcc_data_fusion(
      method            = fusion_method,
      start_year        = FUSION_CONFIG$start_year,
      end_year          = FUSION_CONFIG$end_year,
      standardize       = standardize,
      remove_aggregates = remove_aggregates,
      verbose           = verbose,
      cache_dir         = cache_dir
    ))
  }

  # --- CSV path (original implementation) ---
  if (verbose) message("Loading GCC-Stat datasets from CSV...")

  # Load all datasets
  data_list <- list(
    common_market = load_common_market_csv(data_dir),
    national_accounts = load_national_accounts_csv(data_dir),
    tourism = load_tourism_csv(data_dir),
    cpi = load_cpi_csv(data_dir),
    labor = load_labor_csv(data_dir),
    monetary = load_monetary_csv(data_dir),
    energy = load_energy_csv(data_dir),
    population = load_population_csv(data_dir),
    fdi = load_fdi_csv(data_dir),
    icp = load_icp_csv(data_dir)
  )

  # Standardize country names if requested
  if (standardize) {
    if (verbose) message("  Standardizing country names...")
    data_list <- lapply(data_list, function(df) {
      if (!is.null(df) && "country" %in% names(df)) {
        standardize_countries(df)
      } else {
        df
      }
    })
  }

  # Remove GCC aggregates from key datasets if requested
  if (remove_aggregates) {
    if (verbose) message("  Removing GCC aggregates from datasets...")
    # Only remove from datasets where we want country-level data
    datasets_to_filter <- c("national_accounts", "population", "cpi",
                            "labor", "monetary", "energy")
    for (ds in datasets_to_filter) {
      if (!is.null(data_list[[ds]])) {
        data_list[[ds]] <- remove_gcc_aggregates(data_list[[ds]])
      }
    }
  }

  # Load Comtrade data
  if (verbose) message("  Loading Comtrade data...")
  comtrade_data <- load_comtrade_data(data_dir)
  data_list$comtrade <- comtrade_data$comtrade
  data_list$comtrade_hs <- comtrade_data$comtrade_hs

  # Summary
  if (verbose) {
    loaded_count <- sum(sapply(data_list, function(x) !is.null(x)))
    message(paste("Loaded", loaded_count, "of", length(data_list), "datasets"))
  }

  return(data_list)
}

# =============================================================================
# SECTION 6: RAW INDICATOR EXTRACTION
# =============================================================================
# These functions extract RAW (unnormalized) indicator values that will be
# passed to COINr for normalization and aggregation.
# =============================================================================

#' Get Total Population
#'
#' @param population_data Population dataset
#' @param year_filter Year to filter (NULL for all years)
#' @return Tibble with country, year, population
get_total_population <- function(population_data, year_filter = NULL) {
  
  if (is.null(population_data)) {
    warning("Population data is NULL")
    return(tibble(country = character(), year = integer(), population = numeric()))
  }
  
  pop <- population_data %>%
    filter(
      nationality == "Total",
      gender == "Total",
      age == "All Ages"
    ) %>%
    select(country, year, population = value)
  
  if (!is.null(year_filter)) {
    pop <- pop %>% filter(year == year_filter)
  }
  
  return(pop)
}

#' Get GDP Data
#'
#' Extracts GDP values from National Accounts, ensuring proper filtering
#' for Annual frequency and handling country name standardization.
#'
#' @param na_data National Accounts dataset
#' @param gdp_type Type of GDP: "total", "non_oil", "oil", "per_capita"
#' @param year_filter Year to filter (NULL for all years)
#' @return Tibble with country, year, gdp
get_gdp <- function(na_data, gdp_type = "total", year_filter = NULL) {
  
  if (is.null(na_data)) {
    warning("National Accounts data is NULL")
    return(tibble(country = character(), year = integer(), gdp = numeric()))
  }
  
  indicator_map <- list(
    total = "Gross Domestic Product at Current Prices",
    non_oil = "Non-oil sector at current prices",
    oil = "Oil sector at current prices",
    per_capita = "GDP per capita at current prices"
  )
  
  if (!gdp_type %in% names(indicator_map)) {
    stop(paste("Invalid gdp_type. Must be one of:", paste(names(indicator_map), collapse = ", ")))
  }
  
  gdp <- na_data %>%
    filter(
      indicator == indicator_map[[gdp_type]],
      frequency == "Annual",  # Critical: ensures annual, not quarterly
      price_type == "Current prices"
    ) %>%
    select(country, year, gdp = value)
  
  if (nrow(gdp) == 0) {
    warning(paste("No Annual GDP data found for indicator:", indicator_map[[gdp_type]]))
    return(tibble(country = character(), year = integer(), gdp = numeric()))
  }
  
  if (!is.null(year_filter)) {
    gdp <- gdp %>% filter(year == year_filter)
  }
  
  # Handle duplicates (take first value)
  gdp <- gdp %>%
    group_by(country, year) %>%
    slice(1) %>%
    ungroup()
  
  return(gdp)
}

#' Calculate Coefficient of Variation
#'
#' Used for convergence indicators - measures dispersion across countries
#'
#' @param x Vector of numeric values
#' @return CV as percentage (0-100+)
calculate_cv <- function(x) {
  if (all(is.na(x)) || length(x) < 2) return(NA_real_)
  mean_val <- mean(x, na.rm = TRUE)
  if (mean_val == 0) return(NA_real_)
  sd(x, na.rm = TRUE) / mean_val * 100
}

#' Extract All Raw Indicators for a Given Year
#'
#' This is the main function that extracts all raw indicator values
#' for a specific year. The output is formatted for COINr's iData structure.
#'
#' NOTE: This function requires the indicator modules to be sourced first:
#'   source("R/03_indicators_trade.R")
#'   source("R/04_indicators_financial.R")
#'   source("R/05_indicators_labor.R")
#'   source("R/06_indicators_infrastructure.R")
#'   source("R/07_indicators_sustainability.R")
#'
#' @param data_list List of all loaded datasets (from load_gcc_data())
#' @param year_filter Year to extract indicators for
#' @param external_data List with comtrade and comtrade_hs (optional, deprecated)
#' @return Tibble in COINr iData format with all raw indicators
#' @export
extract_raw_indicators <- function(data_list, year_filter, external_data = NULL) {
  
  message(paste("Extracting raw indicators for year:", year_filter))
  
  gcc_countries <- GCC_CONFIG$countries
  
  # Use comtrade from data_list if external_data not provided
  if (is.null(external_data)) {
    external_data <- list(
      comtrade = data_list$comtrade,
      comtrade_hs = data_list$comtrade_hs
    )
  }
  
  # Initialize base structure
  raw_data <- tibble(
    uCode = c("BHR", "KWT", "OMN", "QAT", "SAU", "ARE"),
    uName = gcc_countries,
    Year = year_filter
  )
  
  # =========================================================================
  # TRADE INDICATORS (from Comtrade)
  # =========================================================================
  message("  Extracting trade indicators...")
  trade_raw <- extract_trade_raw(data_list, external_data, year_filter)
  raw_data <- raw_data %>% left_join(trade_raw, by = "uName")
  
  # =========================================================================
  # FINANCIAL INDICATORS
  # =========================================================================
  message("  Extracting financial indicators...")
  financial_raw <- extract_financial_raw(data_list, year_filter)
  raw_data <- raw_data %>% left_join(financial_raw, by = "uName")
  
  # =========================================================================
  # LABOR INDICATORS
  # =========================================================================
  message("  Extracting labor indicators...")
  labor_raw <- extract_labor_raw(data_list, year_filter)
  raw_data <- raw_data %>% left_join(labor_raw, by = "uName")
  
  # =========================================================================
  # INFRASTRUCTURE INDICATORS
  # =========================================================================
  message("  Extracting infrastructure indicators...")
  infra_raw <- extract_infrastructure_raw(data_list, year_filter)
  raw_data <- raw_data %>% left_join(infra_raw, by = "uName")
  
  # =========================================================================
  # SUSTAINABILITY INDICATORS
  # =========================================================================
  message("  Extracting sustainability indicators...")
  sustain_raw <- extract_sustainability_raw(data_list, year_filter)
  raw_data <- raw_data %>% left_join(sustain_raw, by = "uName")
  
  # =========================================================================
  # CONVERGENCE BASE INDICATORS
  # =========================================================================
  message("  Extracting convergence base indicators...")
  convergence_raw <- extract_convergence_raw(data_list, year_filter)
  raw_data <- raw_data %>% left_join(convergence_raw, by = "uName")
  
  message(paste("✓ Extracted", ncol(raw_data) - 3, "raw indicators"))
  
  return(raw_data)
}

# NOTE: The extract_*_raw() functions are defined in:
#   - 03_indicators_trade.R
#   - 04_indicators_financial.R
#   - 05_indicators_labor.R
#   - 06_indicators_infrastructure.R
#   - 07_indicators_sustainability.R
#
# Source those files before calling extract_raw_indicators()

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  GCCEII DATA LOADING MODULE LOADED
=======================================================

Data Loading:
  - load_gcc_data()              : Load all datasets (CSV or Fusion)
  - load_gcc_data(source='fusion') : Load from Fusion Registry
  - load_comtrade_data()         : Load Comtrade trade data

Fusion Registry (Section 3):
  - load_gcc_data_fusion()       : Load all datasets from Fusion
  - fusion_discover()            : Explore available dataflows
  - fusion_connect_db()          : Connect to MARSA warehouse
  - fusion_fetch_sdmx()          : Query via SDMX REST API
  - fusion_fetch_db()            : Query via PostgreSQL

Utilities:
  - standardize_countries()      : Standardize country names
  - get_gdp()                    : Extract GDP data
  - get_total_population()       : Extract population data
  - extract_raw_indicators()     : Extract all raw indicators for COINr

Configuration:
  - GCC_CONFIG                   : Country codes and names
  - DATA_SOURCES                 : CSV file paths and settings
  - FUSION_CONFIG                : Fusion Registry connection settings

=======================================================
")
