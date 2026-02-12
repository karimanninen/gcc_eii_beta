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
#   3. Fusion Registry Data Loading (see R/11_fusion_registry.R)
#   4. Data Cleaning & Standardization
#   5. Main Loading Function
#   6. Raw Indicator Extraction
#
# Fusion Registry:
#   Use load_gcc_data(source = "fusion") to pull directly from the MARSA
#   Dissemination Warehouse via PostgreSQL. See R/11_fusion_registry.R and
#   pipeline/config.R for setup. Output format is identical to CSV loading.
#
# =============================================================================

library(tidyverse)
library(readr)
library(readxl)
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
  ),
  fiscal = list(
    file = "Fiscal_data_GCC.csv",
    skip_rows = 0,
    source_type = "csv"
  ),
  interest_rates = list(
    file = "GCC_Interest_Rates_Harmonized.xlsx",
    sheet = "Policy Rates",
    skip_rows = 3,
    source_type = "xlsx"
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

#' Load Fiscal Data (Wide-format GCC-Stat Table)
#'
#' Parses the wide-format fiscal CSV with sections: Total Revenues,
#' Oil Revenues, Other Revenues, Total Expenditures, Investment
#' Expenditures, Current Expenditures, Surplus (Deficit).
#' Converts to long-format tidy tibble.
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with columns: country, year, item, value
load_fiscal_csv <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$fiscal$file)

  if (!file.exists(file_path)) {
    warning(paste("Fiscal file not found:", file_path))
    return(NULL)
  }

  # Read raw lines to parse section-based wide format
  raw <- read_csv(file_path, col_names = FALSE, show_col_types = FALSE)

  # Column mapping: col 2=Kuwait, 3=Qatar, 4=Oman, 5=KSA, 6=Bahrain, 7=UAE
  country_cols <- c("Kuwait", "Qatar", "Oman", "Saudi Arabia", "Bahrain", "UAE")

  # Identify section headers and year rows
  # Section headers are text in column 1, year rows are numeric in column 1
  results <- list()
  current_section <- NA_character_

  for (i in seq_len(nrow(raw))) {
    val0 <- as.character(raw[[1]][i])
    if (is.na(val0) || trimws(val0) == "") next

    val0 <- trimws(val0)

    # Check if this is a section header (non-numeric)
    if (suppressWarnings(is.na(as.numeric(val0)))) {
      current_section <- val0
      next
    }

    # It's a year row
    if (!is.na(current_section)) {
      year_val <- as.integer(val0)
      for (j in seq_along(country_cols)) {
        col_idx <- j + 1  # columns 2-7
        raw_val <- as.character(raw[[col_idx]][i])
        # Remove comma thousands separators and parse
        numeric_val <- suppressWarnings(as.numeric(gsub(",", "", raw_val)))
        results[[length(results) + 1]] <- tibble(
          country = country_cols[j],
          year = year_val,
          item = current_section,
          value = numeric_val
        )
      }
    }
  }

  df <- bind_rows(results) %>%
    filter(!is.na(value)) %>%
    mutate(source = "fiscal")

  message(paste("  ✓ Loaded fiscal data:", nrow(df), "rows"))
  return(df)
}

#' Load Interest Rates Data (Excel - Policy Rates)
#'
#' Reads Panel A (Policy Rates) from the GCC Interest Rates workbook.
#' These are central bank administered rates, the most directly comparable
#' across GCC due to USD-peg alignment with US Federal Reserve.
#'
#' @param data_dir Directory containing data files
#' @return Cleaned tibble with columns: country, year, policy_rate
load_interest_rates_xlsx <- function(data_dir = ".") {
  file_path <- file.path(data_dir, DATA_SOURCES$interest_rates$file)

  if (!file.exists(file_path)) {
    warning(paste("Interest rates file not found:", file_path))
    return(NULL)
  }

  if (!requireNamespace("readxl", quietly = TRUE)) {
    warning("readxl package required for interest rates data")
    return(NULL)
  }

  df <- readxl::read_excel(
    file_path,
    sheet = DATA_SOURCES$interest_rates$sheet,
    skip = DATA_SOURCES$interest_rates$skip_rows
  )

  # Pivot to long format: Year + 6 country columns -> tidy
  df_long <- df %>%
    select(year = Year, `Saudi Arabia`, UAE, Qatar, Kuwait, Oman, Bahrain) %>%
    pivot_longer(
      cols = -year,
      names_to = "country",
      values_to = "policy_rate"
    ) %>%
    mutate(
      year = as.integer(year),
      policy_rate = as.numeric(policy_rate),
      source = "interest_rates"
    ) %>%
    filter(!is.na(policy_rate))

  message(paste("  ✓ Loaded interest rates:", nrow(df_long), "rows"))
  return(df_long)
}

# =============================================================================
# SECTION 3: FUSION REGISTRY DATA LOADING
# =============================================================================
#
# PostgreSQL data pipelines to the GCC-Stat Fusion Registry (MARSA
# Dissemination Warehouse). The full implementation lives in a separate
# module to keep this file focused:
#
#   R/11_fusion_registry.R   - All Fusion extraction functions
#   pipeline/config.R        - Database credentials & table mappings
#
# Usage:
#   source("pipeline/config.R")           # Load DB credentials
#   source("R/11_fusion_registry.R")      # Load Fusion module
#   data_list <- load_gcc_data(source = "fusion")
#
# Or equivalently:
#   data_list <- load_gcc_data_fusion()
#
# Credential setup:
#   1. Add MARSA_PASSWORD=your_password to .Renviron
#   2. Edit pipeline/config.R with host, dbname, user if needed
#   3. Restart R so .Renviron is loaded
#
# See pipeline/config.R for full documentation.
# =============================================================================

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
#' GCC Economic Integration Index calculation. Supports two data sources:
#'   - "csv"    : Load from local CSV files in data_dir (default, no network)
#'   - "fusion" : Load from Fusion Registry via PostgreSQL
#'
#' When source = "fusion", delegates to load_gcc_data_fusion() from
#' R/11_fusion_registry.R. Requires pipeline/config.R to be sourced first.
#' See that file for credential setup instructions.
#'
#' @param data_dir Directory containing CSV data files (used when source = "csv")
#' @param source Data source: "csv" (local files) or "fusion" (Fusion Registry)
#' @param standardize Logical, whether to standardize country names (default: TRUE)
#' @param remove_aggregates Logical, whether to remove GCC aggregates (default: TRUE)
#' @param verbose Logical, whether to print progress messages (default: TRUE)
#' @return Named list of cleaned dataframes
#' @export
load_gcc_data <- function(data_dir = "data-raw",
                          source = c("csv", "fusion"),
                          standardize = TRUE,
                          remove_aggregates = TRUE,
                          verbose = TRUE) {

  source <- match.arg(source)

  # --- Fusion Registry path ---
  if (source == "fusion") {
    if (!exists("load_gcc_data_fusion", mode = "function")) {
      stop("Fusion module not loaded. Run these first:\n",
           '  source("pipeline/config.R")\n',
           '  source("R/11_fusion_registry.R")')
    }
    return(load_gcc_data_fusion(
      standardize       = standardize,
      remove_aggregates = remove_aggregates,
      verbose           = verbose
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
    icp = load_icp_csv(data_dir),
    fiscal = load_fiscal_csv(data_dir),
    interest_rates = load_interest_rates_xlsx(data_dir)
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
  - load_gcc_data()                : Load from CSV (default)
  - load_gcc_data(source='fusion') : Load from Fusion Registry
  - load_comtrade_data()           : Load Comtrade trade data

Fusion Registry (source R/11_fusion_registry.R separately):
  - source('pipeline/config.R')    : Load DB credentials
  - source('R/11_fusion_registry.R') : Load Fusion module
  - load_gcc_data_fusion()         : Extract all from MARSA warehouse
  - fusion_discover()              : Explore available tables

Utilities:
  - standardize_countries()        : Standardize country names
  - get_gdp()                      : Extract GDP data
  - get_total_population()         : Extract population data
  - extract_raw_indicators()       : Extract all raw indicators for COINr

=======================================================
")
