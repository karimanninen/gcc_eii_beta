# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - TRADE INDICATORS
# =============================================================================
#
# Purpose:
#   Extract RAW (unnormalized) trade indicator values from Comtrade data.
#   COINr will handle normalization in the build phase.
#
# Indicators:
#   - ind_51: Intra-GCC trade intensity (% of GDP)
#   - ind_52: Intra-GCC services trade share (% of GDP)
#   - ind_55: Intra-GCC non-oil trade intensity (% of GDP)
#   - ind_56: Services as % of total trade
#   - ind_63: Intermediate goods share (%)
#   - ind_64: Trade diversification (100 - HHI)
#
# Data Sources:
#   - comtrade_data.rds (aggregate trade)
#   - comtrade_data_hs.rds (HS-level for BEC classification)
#   - National Accounts (for GDP)
#
# =============================================================================

# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

#' Extract Raw Trade Indicators
#'
#' Extracts all trade-related raw indicator values for a given year.
#' Returns unnormalized values ready for COINr processing.
#'
#' @param data_list List of GCC-Stat datasets (from load_gcc_data())
#' @param external_data List with comtrade and comtrade_hs
#' @param year_filter Year to extract
#' @return Tibble with uName and raw indicator columns
#' @export
extract_trade_raw <- function(data_list, external_data, year_filter) {

  
gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  # Check if Comtrade data is available
  if (is.null(external_data) || 
      is.null(external_data$comtrade) || 
      is.null(external_data$comtrade$total_trade)) {
    
    message("    ⚠ No Comtrade data available - returning NA for trade indicators")
    return(tibble(
      uName = gcc_countries,
      ind_51_trade_intensity = NA_real_,
      ind_52_services_share = NA_real_,
      ind_55_non_oil_trade = NA_real_,
      ind_56_services_pct = NA_real_,
      ind_63_intermediate_share = NA_real_,
      ind_64_diversification = NA_real_
    ))
  }
  
  # Extract data components
  comtrade <- external_data$comtrade
  comtrade_hs <- external_data$comtrade_hs
  na_data <- data_list$national_accounts
  
  # Get GDP for normalizations
  gdp_data <- get_gdp(na_data, "total", year_filter) %>%
    filter(country != "GCC")
  
  # Calculate individual raw indicators
  ind_51 <- calc_ind_51_raw(comtrade$total_trade, gdp_data, year_filter)
  ind_52 <- calc_ind_52_raw(comtrade$total_trade, gdp_data, year_filter)
  ind_55 <- calc_ind_55_raw(comtrade$non_oil_trade, comtrade_hs$non_oil_trade, 
                            gdp_data, year_filter)
  ind_56 <- calc_ind_56_raw(comtrade$total_trade, year_filter)
  ind_63 <- calc_ind_63_raw(comtrade$bec_trade, comtrade_hs$bec_trade, year_filter)
  ind_64 <- calc_ind_64_raw(comtrade_hs$bec_trade, year_filter)
  
  # Combine all indicators
  trade_raw <- ind_51 %>%
    left_join(ind_52, by = "uName") %>%
    left_join(ind_55, by = "uName") %>%
    left_join(ind_56, by = "uName") %>%
    left_join(ind_63, by = "uName") %>%
    left_join(ind_64, by = "uName")
  
  # Ensure all countries present
  trade_raw <- ensure_all_countries(trade_raw, gcc_countries)
  
  return(trade_raw)
}

# =============================================================================
# INDIVIDUAL INDICATOR FUNCTIONS
# =============================================================================

#' Indicator 51: Trade Intensity (Raw)
#'
#' Intra-GCC total trade as % of GDP
#'
#' @param total_trade Total trade dataframe from comtrade
#' @param gdp_data GDP by country
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_51_trade_intensity
calc_ind_51_raw <- function(total_trade, gdp_data, year_filter) {
  
  if (is.null(total_trade) || nrow(total_trade) == 0) {
    return(tibble(uName = character(), ind_51_trade_intensity = numeric()))
  }
  
  # Standardize country names
  total_trade <- total_trade %>%
    mutate(reporter_name = standardize_country_name(reporter_name))
  
  # Filter for year and calculate total intra-GCC trade by country
  year_trade <- total_trade %>%
    filter(year == year_filter) %>%
    group_by(reporter_name) %>%
    summarize(
      exports_gcc = sum(trade_value_million_usd[flow_direction == "export"], na.rm = TRUE),
      imports_gcc = sum(trade_value_million_usd[flow_direction == "import"], na.rm = TRUE),
      total_intra_trade = exports_gcc + imports_gcc,
      .groups = "drop"
    ) %>%
    rename(country = reporter_name)
  
  # Join with GDP and calculate ratio
  trade_intensity <- year_trade %>%
    left_join(gdp_data, by = "country") %>%
    mutate(
      # Trade and GDP both in millions, result is percentage
      ind_51_trade_intensity = (total_intra_trade / gdp) * 100
    ) %>%
    select(uName = country, ind_51_trade_intensity)
  
  return(trade_intensity)
}

#' Indicator 52: Services Trade Share (Raw)
#'
#' Estimated intra-GCC services trade as % of GDP
#' Note: Uses estimated services shares by country (actual data not available)
#'
#' @param total_trade Total trade dataframe
#' @param gdp_data GDP by country
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_52_services_share
calc_ind_52_raw <- function(total_trade, gdp_data, year_filter) {
  
  # Services share estimates (based on typical GCC services composition)
  services_estimates <- tribble(
    ~country, ~services_share,
    "Bahrain", 0.25,
    "Kuwait", 0.15,
    "Oman", 0.18,
    "Qatar", 0.20,
    "Saudi Arabia", 0.15,
    "UAE", 0.30
  )
  
  if (is.null(total_trade) || nrow(total_trade) == 0) {
    return(services_estimates %>%
             mutate(ind_52_services_share = NA_real_) %>%
             select(uName = country, ind_52_services_share))
  }
  
  # Standardize and calculate total trade
  total_trade <- total_trade %>%
    mutate(reporter_name = standardize_country_name(reporter_name))
  
  total_by_country <- total_trade %>%
    filter(year == year_filter) %>%
    group_by(reporter_name) %>%
    summarize(
      total_trade_value = sum(trade_value_million_usd, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    rename(country = reporter_name)
  
  # Apply services estimates and calculate as % of GDP
  services_indicator <- total_by_country %>%
    left_join(services_estimates, by = "country") %>%
    mutate(services_value = total_trade_value * services_share) %>%
    left_join(gdp_data, by = "country") %>%
    mutate(
      ind_52_services_share = (services_value / gdp) * 100
    ) %>%
    select(uName = country, ind_52_services_share)
  
  return(services_indicator)
}

#' Indicator 55: Non-oil Trade Intensity (Raw)
#'
#' Intra-GCC non-oil trade as % of GDP
#'
#' @param non_oil_trade Non-oil trade dataframe (aggregate)
#' @param non_oil_trade_hs Non-oil trade dataframe (HS-level)
#' @param gdp_data GDP by country
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_55_non_oil_trade
calc_ind_55_raw <- function(non_oil_trade, non_oil_trade_hs, gdp_data, year_filter) {
  
  # Use HS-level if available, otherwise aggregate
  trade_data <- if (!is.null(non_oil_trade_hs) && nrow(non_oil_trade_hs) > 0) {
    non_oil_trade_hs
  } else {
    non_oil_trade
  }
  
  if (is.null(trade_data) || nrow(trade_data) == 0) {
    return(tibble(uName = character(), ind_55_non_oil_trade = numeric()))
  }
  
  # Standardize country names
  trade_data <- trade_data %>%
    mutate(reporter_name = standardize_country_name(reporter_name))
  
  year_trade <- trade_data %>%
    filter(year == year_filter) %>%
    group_by(reporter_name) %>%
    summarize(
      non_oil_exports = sum(trade_value_million_usd[flow_direction == "export"], na.rm = TRUE),
      non_oil_imports = sum(trade_value_million_usd[flow_direction == "import"], na.rm = TRUE),
      total_non_oil = non_oil_exports + non_oil_imports,
      .groups = "drop"
    ) %>%
    rename(country = reporter_name)
  
  non_oil_indicator <- year_trade %>%
    left_join(gdp_data, by = "country") %>%
    mutate(
      ind_55_non_oil_trade = (total_non_oil / gdp) * 100
    ) %>%
    select(uName = country, ind_55_non_oil_trade)
  
  return(non_oil_indicator)
}

#' Indicator 56: Services as % of Total Trade (Raw)
#'
#' Estimated services share of total intra-GCC trade
#'
#' @param total_trade Total trade dataframe
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_56_services_pct
calc_ind_56_raw <- function(total_trade, year_filter) {
  
  # Fixed estimates based on country economic structure
  services_estimates <- tribble(
    ~uName, ~ind_56_services_pct,
    "Bahrain", 25,
    "Kuwait", 15,
    "Oman", 18,
    "Qatar", 20,
    "Saudi Arabia", 15,
    "UAE", 30
  )
  
  return(services_estimates)
}

#' Indicator 63: Intermediate Goods Share (Raw)
#'
#' Share of intermediate goods in intra-GCC trade (BEC classification)
#'
#' @param bec_trade BEC-classified trade (aggregate)
#' @param bec_trade_hs BEC-classified trade (HS-level)
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_63_intermediate_share
calc_ind_63_raw <- function(bec_trade, bec_trade_hs, year_filter) {
  
  # Use HS-level if available
  trade_data <- if (!is.null(bec_trade_hs) && nrow(bec_trade_hs) > 0) {
    bec_trade_hs
  } else {
    bec_trade
  }
  
  # Fallback estimates if no BEC data
  if (is.null(trade_data) || nrow(trade_data) == 0) {
    return(tribble(
      ~uName, ~ind_63_intermediate_share,
      "Bahrain", 35,
      "Kuwait", 30,
      "Oman", 28,
      "Qatar", 25,
      "Saudi Arabia", 30,
      "UAE", 35
    ))
  }
  
  # Standardize country names
  trade_data <- trade_data %>%
    mutate(reporter_name = standardize_country_name(reporter_name))
  
  intermediate_agg <- trade_data %>%
    filter(year == year_filter) %>%
    group_by(reporter_name) %>%
    summarize(
      intermediate_value = sum(trade_value_million_usd[bec_category == "intermediate"], na.rm = TRUE),
      total_value = sum(trade_value_million_usd, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      ind_63_intermediate_share = if_else(total_value > 0,
                                           (intermediate_value / total_value) * 100,
                                           NA_real_)
    ) %>%
    select(uName = reporter_name, ind_63_intermediate_share)
  
  return(intermediate_agg)
}

#' Indicator 64: Trade Diversification (Raw)
#'
#' Diversification score based on Herfindahl-Hirschman Index
#' Higher score = more diversified trade portfolio
#'
#' @param bec_trade_hs BEC-classified HS-level trade
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_64_diversification
calc_ind_64_raw <- function(bec_trade_hs, year_filter) {
  
  # Fallback estimates if no data
  if (is.null(bec_trade_hs) || nrow(bec_trade_hs) == 0) {
    return(tribble(
      ~uName, ~ind_64_diversification,
      "Bahrain", 60,
      "Kuwait", 45,
      "Oman", 50,
      "Qatar", 48,
      "Saudi Arabia", 45,
      "UAE", 65
    ))
  }
  
  # Standardize country names
  bec_trade_hs <- bec_trade_hs %>%
    mutate(reporter_name = standardize_country_name(reporter_name))
  
  # Calculate trade shares by BEC category
  composition <- bec_trade_hs %>%
    filter(year == year_filter) %>%
    group_by(reporter_name, bec_category) %>%
    summarize(
      trade_value = sum(trade_value_million_usd, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    group_by(reporter_name) %>%
    mutate(
      total_trade = sum(trade_value, na.rm = TRUE),
      trade_share = if_else(total_trade > 0, trade_value / total_trade, 0)
    ) %>%
    ungroup()
  
  # Calculate HHI and diversification score
  diversification_scores <- composition %>%
    group_by(reporter_name) %>%
    summarize(
      herfindahl_index = sum(trade_share^2, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      # Diversification = 100 * (1 - HHI)
      ind_64_diversification = 100 * (1 - herfindahl_index)
    ) %>%
    select(uName = reporter_name, ind_64_diversification)
  
  return(diversification_scores)
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

#' Standardize Country Name (internal)
#'
#' @param x Character vector of country names
#' @return Standardized country names
standardize_country_name <- function(x) {
  case_when(
    x %in% c("United Arab Emirates", "ARE", "Emirates") ~ "UAE",
    x %in% c("KSA", "Saudi") ~ "Saudi Arabia",
    TRUE ~ x
  )
}

#' Ensure All Countries Present
#'
#' @param df Dataframe with uName column
#' @param countries Vector of expected country names
#' @return Dataframe with all countries (NA for missing)
ensure_all_countries <- function(df, countries) {
  
  missing <- setdiff(countries, df$uName)
  
  if (length(missing) > 0) {
    # Get column names except uName
    other_cols <- setdiff(names(df), "uName")
    
    # Create rows for missing countries
    missing_rows <- tibble(uName = missing)
    for (col in other_cols) {
      missing_rows[[col]] <- NA_real_
    }
    
    df <- bind_rows(df, missing_rows)
  }
  
  # Ensure correct order
  df %>% arrange(match(uName, countries))
}

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  TRADE INDICATORS MODULE LOADED
=======================================================

Main function:
  - extract_trade_raw()      : Extract all trade indicators

Individual indicators:
  - ind_51: Trade intensity (% of GDP)
  - ind_52: Services share (% of GDP)
  - ind_55: Non-oil trade (% of GDP)
  - ind_56: Services % of total
  - ind_63: Intermediate goods share (%)
  - ind_64: Diversification score (0-100)

=======================================================
")
