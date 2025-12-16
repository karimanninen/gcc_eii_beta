# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - SUSTAINABILITY & CONVERGENCE INDICATORS
# =============================================================================
#
# Purpose:
#   Extract RAW (unnormalized) sustainability and convergence indicator values.
#   COINr will handle normalization in the build phase.
#
# Sustainability Indicators:
#   - ind_non_oil_share: Non-oil GDP as % of total GDP
#   - ind_oil_share: Oil GDP as % of total GDP
#   - ind_manufacturing_share: Manufacturing as % of GDP
#
# Convergence Base Indicators:
#   These are the SOURCE values used to calculate CV-based convergence scores.
#   The actual convergence score (100 - CV) will be calculated in COINr.
#   - ind_ppp_gdp_pc: PPP GDP per capita (USD)
#   - ind_price_level: Price level index (World = 100)
#
# Data Sources:
#   - National Accounts (GDP components)
#   - ICP data (PPP, price levels)
#
# =============================================================================

# =============================================================================
# MAIN EXTRACTION FUNCTIONS
# =============================================================================

#' Extract Raw Sustainability Indicators
#'
#' Extracts sustainability/diversification raw indicator values.
#'
#' @param data_list List of GCC-Stat datasets (from load_gcc_data())
#' @param year_filter Year to extract
#' @return Tibble with uName and raw indicator columns
#' @export
extract_sustainability_raw <- function(data_list, year_filter) {
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  message(paste("    Extracting sustainability indicators for", year_filter))
  
  na_data <- data_list$national_accounts
  
  # Get GDP components
  total_gdp <- get_gdp(na_data, "total", year_filter) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council"))
  
  non_oil_gdp <- get_gdp(na_data, "non_oil", year_filter) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    rename(non_oil = gdp)
  
  oil_gdp <- get_gdp(na_data, "oil", year_filter) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    rename(oil = gdp)
  
  # Get manufacturing GDP
  manufacturing <- na_data %>%
    filter(
      indicator == "03. Manufacturing at current prices",
      frequency == "Annual",
      year == year_filter
    ) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    select(country, manufacturing_gdp = value)
  
  # Calculate shares
  sustainability <- total_gdp %>%
    left_join(non_oil_gdp %>% select(country, non_oil), by = "country") %>%
    left_join(oil_gdp %>% select(country, oil), by = "country") %>%
    left_join(manufacturing, by = "country") %>%
    mutate(
      ind_non_oil_share = (non_oil / gdp) * 100,
      ind_oil_share = (oil / gdp) * 100,
      ind_manufacturing_share = (manufacturing_gdp / gdp) * 100
    ) %>%
    select(uName = country, ind_non_oil_share, ind_oil_share, ind_manufacturing_share)
  
  # Ensure all countries present
  all_countries <- tibble(uName = gcc_countries)
  sustainability <- all_countries %>%
    left_join(sustainability, by = "uName")
  
  return(sustainability)
}

#' Extract Raw Convergence Base Indicators
#'
#' Extracts the base values used to calculate convergence scores.
#' These are NOT the final convergence scores - those are calculated
#' as (100 - CV) in COINr using custom aggregation.
#'
#' @param data_list List of GCC-Stat datasets (from load_gcc_data())
#' @param year_filter Year to extract
#' @return Tibble with uName and raw indicator columns
#' @export
extract_convergence_raw <- function(data_list, year_filter) {
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  message(paste("    Extracting convergence base indicators for", year_filter))
  
  icp_data <- data_list$icp
  
  # PPP GDP per capita
  ppp_gdp_pc <- calc_ppp_gdp_pc_raw(icp_data, year_filter)
  
  # Price level index
  price_level <- calc_price_level_raw(icp_data, year_filter)
  
  # Combine
  convergence_raw <- tibble(uName = gcc_countries) %>%
    left_join(ppp_gdp_pc, by = "uName") %>%
    left_join(price_level, by = "uName")
  
  return(convergence_raw)
}

# =============================================================================
# INDIVIDUAL INDICATOR FUNCTIONS
# =============================================================================

#' PPP GDP Per Capita (Raw)
#'
#' GDP per capita at purchasing power parity (US$)
#' Used as base for real income convergence calculation
#' Note: ICP data only available for benchmark years (2017, 2021)
#'
#' @param icp_data ICP dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_ppp_gdp_pc
calc_ppp_gdp_pc_raw <- function(icp_data, year_filter) {
  
  if (is.null(icp_data)) {
    return(tibble(uName = character(), ind_ppp_gdp_pc = numeric()))
  }
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  ppp_gdp_pc <- icp_data %>%
    filter(
      classification == "Expenditure per capita, PPP-based (US$)",
      series == "1000000:GROSS DOMESTIC PRODUCT",
      year == year_filter,
      country %in% gcc_countries
    ) %>%
    group_by(country) %>%
    summarize(ind_ppp_gdp_pc = first(value), .groups = "drop") %>%
    rename(uName = country)
  
  return(ppp_gdp_pc)
}

#' Price Level Index (Raw)
#'
#' Price level index (World = 100)
#' Used as base for price convergence calculation
#' Note: ICP data only available for benchmark years (2017, 2021)
#'
#' @param icp_data ICP dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_price_level
calc_price_level_raw <- function(icp_data, year_filter) {
  
  if (is.null(icp_data)) {
    return(tibble(uName = character(), ind_price_level = numeric()))
  }
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  price_level <- icp_data %>%
    filter(
      classification == "Price level index (World = 100)",
      series == "1000000:GROSS DOMESTIC PRODUCT",
      year == year_filter,
      country %in% gcc_countries
    ) %>%
    group_by(country) %>%
    summarize(ind_price_level = first(value), .groups = "drop") %>%
    rename(uName = country)
  
  return(price_level)
}


# =============================================================================
# CONVERGENCE SCORE CALCULATION
# =============================================================================
#
# Note on Convergence Indicators:
#
# Unlike other indicators that are normalized per-country, convergence
# indicators measure REGIONAL dispersion. The score is:
#   convergence_score = 100 - CV(indicator across all countries)
#
# This means ALL countries get the SAME score for each convergence indicator.
#
# In COINr, we'll handle this using custom aggregation that:
# 1. Calculates CV across the 6 GCC countries for each base indicator
# 2. Converts to score using: 100 - CV
# 3. Assigns the same score to all countries
#
# The following helper function can be used outside COINr if needed:
# =============================================================================

#' Calculate Convergence Score from Values
#'
#' Calculates a single convergence score from a vector of country values.
#' Returns the same score for all countries (regional indicator).
#'
#' @param values Numeric vector of country values
#' @param countries Character vector of country names (same order as values)
#' @return Tibble with uName and convergence_score (same for all)
#' @export
calculate_convergence_score <- function(values, countries) {
  
  cv <- calculate_cv(values)
  score <- max(0, 100 - cv)
  
  tibble(
    uName = countries,
    convergence_score = rep(score, length(countries))
  )
}

#' Calculate All Convergence Scores
#'
#' Calculates convergence scores for all base indicators.
#' This is a convenience function that can be used if not using COINr.
#'
#' @param raw_data Tibble with raw convergence base indicators
#' @return Tibble with uName and convergence scores for each dimension
#' @export
calculate_all_convergence_scores <- function(raw_data) {
  
  countries <- raw_data$uName
  
  # Real income convergence (from PPP GDP per capita)
  cv_income <- calculate_cv(raw_data$ind_ppp_gdp_pc)
  
  # Price level convergence
  cv_price <- calculate_cv(raw_data$ind_price_level)
  
  # Non-oil share convergence
  cv_non_oil <- if ("ind_non_oil_share" %in% names(raw_data)) {
    calculate_cv(raw_data$ind_non_oil_share)
  } else NA_real_
  
  # Manufacturing share convergence
  cv_manufacturing <- if ("ind_manufacturing_share" %in% names(raw_data)) {
    calculate_cv(raw_data$ind_manufacturing_share)
  } else NA_real_
  
  # Oil dependency convergence
  cv_oil <- if ("ind_oil_share" %in% names(raw_data)) {
    calculate_cv(raw_data$ind_oil_share)
  } else NA_real_
  
  tibble(
    uName = countries,
    conv_real_income = rep(max(0, 100 - cv_income), length(countries)),
    conv_price_level = rep(max(0, 100 - cv_price), length(countries)),
    conv_non_oil = rep(max(0, 100 - cv_non_oil), length(countries)),
    conv_manufacturing = rep(max(0, 100 - cv_manufacturing), length(countries)),
    conv_oil_dependency = rep(max(0, 100 - cv_oil), length(countries))
  )
}

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  SUSTAINABILITY & CONVERGENCE INDICATORS MODULE LOADED
=======================================================

Main functions:
  - extract_sustainability_raw() : Extract sustainability indicators
  - extract_convergence_raw()    : Extract convergence base indicators

Sustainability indicators:
  - ind_non_oil_share: Non-oil GDP share (%)
  - ind_oil_share: Oil GDP share (%)
  - ind_manufacturing_share: Manufacturing share (%)

Convergence base indicators:
  - ind_ppp_gdp_pc: PPP GDP per capita (USD)
  - ind_price_level: Price level index (World=100)

Helper functions:
  - calculate_convergence_score()     : CV to score conversion
  - calculate_all_convergence_scores(): All convergence scores

=======================================================
")
