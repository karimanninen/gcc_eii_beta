# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - CONVERGENCE INDICATORS
# =============================================================================
#
# Purpose:
#   Calculate COUNTRY-SPECIFIC convergence indicators measuring how close
#   each country is to the GCC regional average. Higher score = more aligned.
#
# Methodology:
#   convergence_score = 100 - |country_value - GCC_mean| / GCC_mean × 100
#   
#   This gives each country a unique score based on their deviation from
#   the regional average, making it policy-actionable at country level.
#
# Indicators:
#   - ind_conv_non_oil: Non-oil share convergence
#   - ind_conv_manufacturing: Manufacturing share convergence
#   - ind_conv_oil: Oil dependency convergence
#   - ind_conv_income: Real income convergence (ICP years only)
#   - ind_conv_price: Price level convergence (ICP years only)
#   - ind_conv_fiscal: Fiscal balance convergence
#   - ind_conv_interest: Interest rate convergence (policy rates)
#
# Data Sources:
#   - National Accounts (non-oil, manufacturing, oil shares)
#   - ICP data (PPP GDP per capita, price levels) - 2017, 2021 only
#   - Fiscal data (surplus/deficit ratio) - 2015-2024
#   - Interest rates Excel (policy rates) - 2015-2025
#
# =============================================================================

# =============================================================================
# HELPER FUNCTION
# =============================================================================

#' Calculate Distance-based Convergence Score
#'
#' Measures how close each value is to the group mean.
#' Score = 100 - percentage deviation from mean.
#' Higher score = more aligned with regional average.
#'
#' @param x Numeric vector of country values
#' @param cap_at Maximum deviation to consider (default 100%)
#' @return Numeric vector of scores (0-100)
#' @export
calc_distance_score <- function(x, cap_at = 100) {
  
  if (all(is.na(x))) {
    return(rep(NA_real_, length(x)))
  }
  
  gcc_mean <- mean(x, na.rm = TRUE)
  
  if (is.na(gcc_mean) || gcc_mean == 0) {
    return(rep(NA_real_, length(x)))
  }
  
  # Percentage deviation from mean
  pct_deviation <- abs(x - gcc_mean) / gcc_mean * 100
  
  # Cap deviation and invert (higher = more converged)
  scores <- pmax(0, cap_at - pmin(pct_deviation, cap_at))
  
  return(scores)
}

# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

#' Extract Convergence Indicators (Country-Specific)
#'
#' Calculates how close each country is to the GCC average
#' for key structural and price indicators.
#'
#' @param data_list List of GCC-Stat datasets
#' @param year_filter Year to calculate
#' @return Tibble with uName and convergence score columns
#' @export
extract_convergence_raw <- function(data_list, year_filter) {
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  message(paste("    Extracting convergence indicators for", year_filter))
  
  # =========================================================================
  # GET BASE VALUES
  # =========================================================================
  
  # Sustainability indicators (available all years)
  sustain <- extract_sustainability_raw(data_list, year_filter)
  
 # ICP indicators (2017, 2021 only)
  icp_ppp <- calc_ppp_gdp_pc_raw(data_list$icp, year_filter)
  icp_price <- calc_price_level_raw(data_list$icp, year_filter)

  # Fiscal balance for convergence (all years with fiscal data)
  fiscal_bal <- calc_fiscal_balance_raw(data_list$fiscal, year_filter)

  # Policy rates for interest rate convergence
  policy_rates <- calc_policy_rate_raw(data_list$interest_rates, year_filter)

  # =========================================================================
  # MERGE BASE DATA
  # =========================================================================

  base_data <- tibble(uName = gcc_countries) %>%
    left_join(sustain, by = "uName") %>%
    left_join(icp_ppp, by = "uName") %>%
    left_join(icp_price, by = "uName") %>%
    left_join(fiscal_bal, by = "uName") %>%
    left_join(policy_rates, by = "uName")

  # =========================================================================
  # CALCULATE COUNTRY-SPECIFIC CONVERGENCE SCORES
  # =========================================================================

  convergence <- base_data %>%
    mutate(
      # Structural convergence (available all years)
      ind_conv_non_oil = calc_distance_score(ind_non_oil_share),
      ind_conv_manufacturing = calc_distance_score(ind_manufacturing_share),
      ind_conv_oil = calc_distance_score(ind_oil_share),

      # Real convergence (ICP years only)
      ind_conv_income = calc_distance_score(ind_ppp_gdp_pc),
      ind_conv_price = calc_distance_score(ind_price_level),

      # Fiscal convergence (available all years with fiscal data)
      ind_conv_fiscal = calc_distance_score(ind_fiscal_balance),

      # Interest rate convergence (policy rates, 2015-2025)
      ind_conv_interest = calc_distance_score(ind_policy_rate)
    ) %>%
    select(uName, starts_with("ind_conv_"))

  return(convergence)
}

# =============================================================================
# INDIVIDUAL INDICATOR FUNCTIONS (for testing/debugging)
# =============================================================================

#' Non-oil Share Convergence (Raw)
#'
#' How close is each country's non-oil GDP share to the GCC average?
#'
#' @param sustain_data Sustainability indicators tibble
#' @return Tibble with uName, ind_conv_non_oil
calc_conv_non_oil_raw <- function(sustain_data) {
  
  if (is.null(sustain_data) || !"ind_non_oil_share" %in% names(sustain_data)) {
    return(tibble(uName = character(), ind_conv_non_oil = numeric()))
  }
  
  sustain_data %>%
    mutate(ind_conv_non_oil = calc_distance_score(ind_non_oil_share)) %>%
    select(uName, ind_conv_non_oil)
}

#' Manufacturing Share Convergence (Raw)
#'
#' How close is each country's manufacturing share to the GCC average?
#'
#' @param sustain_data Sustainability indicators tibble
#' @return Tibble with uName, ind_conv_manufacturing
calc_conv_manufacturing_raw <- function(sustain_data) {
  
  if (is.null(sustain_data) || !"ind_manufacturing_share" %in% names(sustain_data)) {
    return(tibble(uName = character(), ind_conv_manufacturing = numeric()))
  }
  
  sustain_data %>%
    mutate(ind_conv_manufacturing = calc_distance_score(ind_manufacturing_share)) %>%
    select(uName, ind_conv_manufacturing)
}

#' Oil Dependency Convergence (Raw)
#'
#' How close is each country's oil dependency to the GCC average?
#'
#' @param sustain_data Sustainability indicators tibble
#' @return Tibble with uName, ind_conv_oil
calc_conv_oil_raw <- function(sustain_data) {
  
  if (is.null(sustain_data) || !"ind_oil_share" %in% names(sustain_data)) {
    return(tibble(uName = character(), ind_conv_oil = numeric()))
  }
  
  sustain_data %>%
    mutate(ind_conv_oil = calc_distance_score(ind_oil_share)) %>%
    select(uName, ind_conv_oil)
}

#' Real Income Convergence (Raw)
#'
#' How close is each country's PPP GDP per capita to the GCC average?
#' Only available for ICP benchmark years (2017, 2021).
#'
#' @param icp_ppp PPP GDP per capita tibble
#' @return Tibble with uName, ind_conv_income
calc_conv_income_raw <- function(icp_ppp) {
  
  if (is.null(icp_ppp) || nrow(icp_ppp) == 0) {
    return(tibble(uName = character(), ind_conv_income = numeric()))
  }
  
  icp_ppp %>%
    mutate(ind_conv_income = calc_distance_score(ind_ppp_gdp_pc)) %>%
    select(uName, ind_conv_income)
}

#' Price Level Convergence (Raw)
#'
#' How close is each country's price level to the GCC average?
#' Only available for ICP benchmark years (2017, 2021).
#'
#' @param icp_price Price level tibble
#' @return Tibble with uName, ind_conv_price
calc_conv_price_raw <- function(icp_price) {
  
  if (is.null(icp_price) || nrow(icp_price) == 0) {
    return(tibble(uName = character(), ind_conv_price = numeric()))
  }
  
  icp_price %>%
    mutate(ind_conv_price = calc_distance_score(ind_price_level)) %>%
    select(uName, ind_conv_price)
}

#' Fiscal Balance Convergence (Raw)
#'
#' How close is each country's fiscal balance ratio to the GCC average?
#' Uses the fiscal balance (surplus/deficit as % of total revenues).
#'
#' @param fiscal_bal Fiscal balance tibble (from calc_fiscal_balance_raw)
#' @return Tibble with uName, ind_conv_fiscal
calc_conv_fiscal_raw <- function(fiscal_bal) {

  if (is.null(fiscal_bal) || !"ind_fiscal_balance" %in% names(fiscal_bal)) {
    return(tibble(uName = character(), ind_conv_fiscal = numeric()))
  }

  fiscal_bal %>%
    mutate(ind_conv_fiscal = calc_distance_score(ind_fiscal_balance)) %>%
    select(uName, ind_conv_fiscal)
}

#' Policy Rate (Raw Helper)
#'
#' Extracts policy rates for a given year from interest rates data.
#' These are central bank administered rates (Panel A).
#'
#' @param interest_rates_data Interest rates dataset from load_interest_rates_xlsx()
#' @param year_filter Year to extract
#' @return Tibble with uName, ind_policy_rate
calc_policy_rate_raw <- function(interest_rates_data, year_filter) {

  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")

  if (is.null(interest_rates_data)) {
    return(tibble(uName = gcc_countries, ind_policy_rate = NA_real_))
  }

  result <- interest_rates_data %>%
    filter(year == year_filter) %>%
    select(uName = country, ind_policy_rate = policy_rate)

  # Ensure all countries present
  all_countries <- tibble(uName = gcc_countries)
  result <- all_countries %>%
    left_join(result, by = "uName")

  return(result)
}

#' Interest Rate Convergence (Raw)
#'
#' How close is each country's policy rate to the GCC average?
#' Uses central bank policy rates (Panel A, most comparable across GCC).
#'
#' @param policy_rates Policy rate tibble (from calc_policy_rate_raw)
#' @return Tibble with uName, ind_conv_interest
calc_conv_interest_raw <- function(policy_rates) {

  if (is.null(policy_rates) || !"ind_policy_rate" %in% names(policy_rates)) {
    return(tibble(uName = character(), ind_conv_interest = numeric()))
  }

  policy_rates %>%
    mutate(ind_conv_interest = calc_distance_score(ind_policy_rate)) %>%
    select(uName, ind_conv_interest)
}

# =============================================================================
# DIAGNOSTIC FUNCTIONS
# =============================================================================

#' Summarize Convergence Status
#'
#' Shows which countries are above/below GCC average for each measure.
#'
#' @param data_list List of datasets
#' @param year_filter Year to analyze
#' @return Summary tibble
#' @export
summarize_convergence <- function(data_list, year_filter) {
  
  sustain <- extract_sustainability_raw(data_list, year_filter)
  icp_ppp <- calc_ppp_gdp_pc_raw(data_list$icp, year_filter)
  icp_price <- calc_price_level_raw(data_list$icp, year_filter)
  
  base_data <- tibble(uName = c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")) %>%
    left_join(sustain, by = "uName") %>%
    left_join(icp_ppp, by = "uName") %>%
    left_join(icp_price, by = "uName")
  
  # Calculate means
  means <- base_data %>%
    summarize(
      across(where(is.numeric), ~mean(.x, na.rm = TRUE))
    ) %>%
    mutate(uName = "GCC_Mean") %>%
    select(uName, everything())
  
  # Combine with country data
  bind_rows(base_data, means) %>%
    mutate(across(where(is.numeric), ~round(.x, 2)))
}

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  CONVERGENCE INDICATORS MODULE LOADED
=======================================================

Main function:
  - extract_convergence_raw()  : Extract all convergence indicators

Individual indicators:
  - ind_conv_non_oil: Non-oil share convergence (all years)
  - ind_conv_manufacturing: Manufacturing convergence (all years)
  - ind_conv_oil: Oil dependency convergence (all years)
  - ind_conv_income: Real income convergence (ICP years)
  - ind_conv_price: Price level convergence (ICP years)
  - ind_conv_fiscal: Fiscal balance convergence (2015-2024)
  - ind_conv_interest: Interest rate convergence (2015-2025)

Helper functions:
  - calc_distance_score()      : Distance from mean calculation
  - calc_policy_rate_raw()     : Extract policy rates
  - summarize_convergence()    : Diagnostic summary

Methodology:
  Score = 100 - |value - GCC_mean| / GCC_mean × 100
  Higher score = more aligned with regional average

=======================================================
")
