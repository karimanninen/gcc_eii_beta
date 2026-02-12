# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - FINANCIAL INDICATORS
# =============================================================================
#
# Purpose:
#   Extract RAW (unnormalized) financial indicator values.
#   COINr will handle normalization in the build phase.
#
# Indicators:
#   - ind_inflation: Annual inflation rate (%)
#   - ind_m2_growth: M2 money supply growth rate (%)
#   - ind_gdp_growth: Real GDP growth rate (%)
#   - ind_39_banking: GCC banks per million population
#   - ind_44_stock: Stock market openness composite (%)
#   - ind_31_fdi: Intra-GCC FDI as % of GDP
#   - ind_bank_depth: Bank assets as % of GDP
#   - ind_fiscal_balance: Fiscal balance as % of total revenues
#
# Data Sources:
#   - CPI data (inflation)
#   - Monetary data (M2, bank assets)
#   - National Accounts (GDP, growth)
#   - Common Market (banking, stock market)
#   - FDI flows
#   - Fiscal data (revenues, surplus/deficit)
#
# =============================================================================

# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

#' Extract Raw Financial Indicators
#'
#' Extracts all financial/monetary raw indicator values for a given year.
#'
#' @param data_list List of GCC-Stat datasets (from load_gcc_data())
#' @param year_filter Year to extract
#' @return Tibble with uName and raw indicator columns
#' @export
extract_financial_raw <- function(data_list, year_filter) {
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  message(paste("    Extracting financial indicators for", year_filter))
  
  # Extract individual indicators
  inflation <- calc_inflation_raw(data_list$cpi, year_filter)
  m2_growth <- calc_m2_growth_raw(data_list$monetary, year_filter)
  gdp_growth <- calc_gdp_growth_raw(data_list$national_accounts, year_filter)
  banking <- calc_banking_raw(data_list$common_market, data_list$population, year_filter)
  stock_market <- calc_stock_market_raw(data_list$common_market, year_filter)
  fdi <- calc_fdi_raw(data_list$fdi, data_list$national_accounts, year_filter)
  bank_depth <- calc_bank_depth_raw(data_list$monetary, data_list$national_accounts, year_filter)
  fiscal_balance <- calc_fiscal_balance_raw(data_list$fiscal, year_filter)

  # Combine all indicators
  financial_raw <- tibble(uName = gcc_countries) %>%
    left_join(inflation, by = "uName") %>%
    left_join(m2_growth, by = "uName") %>%
    left_join(gdp_growth, by = "uName") %>%
    left_join(banking, by = "uName") %>%
    left_join(stock_market, by = "uName") %>%
    left_join(fdi, by = "uName") %>%
    left_join(bank_depth, by = "uName") %>%
    left_join(fiscal_balance, by = "uName")
  
  return(financial_raw)
}

# =============================================================================
# INDIVIDUAL INDICATOR FUNCTIONS
# =============================================================================

#' Inflation Rate (Raw)
#'
#' Annual CPI inflation rate
#'
#' @param cpi_data CPI dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_inflation
calc_inflation_raw <- function(cpi_data, year_filter) {
  
  if (is.null(cpi_data)) {
    return(tibble(uName = character(), ind_inflation = numeric()))
  }
  
  annual_inflation <- cpi_data %>%
    filter(
      indicator == "Individual consumption expenditure of households",
      unit == "Percentage change - previous period",
      frequency == "Annual",
      year == year_filter,
      !country %in% c("GCC", "Gulf Cooperation Council")
    ) %>%
    group_by(country) %>%
    summarize(ind_inflation = mean(value, na.rm = TRUE), .groups = "drop") %>%
    rename(uName = country)
  
  return(annual_inflation)
}

#' M2 Growth Rate (Raw)
#'
#' Year-over-year growth in M2 money supply
#'
#' @param monetary_data Monetary dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_m2_growth
calc_m2_growth_raw <- function(monetary_data, year_filter) {
  
  if (is.null(monetary_data)) {
    return(tibble(uName = character(), ind_m2_growth = numeric()))
  }
  
  m2_growth <- monetary_data %>%
    filter(
      indicator == "Money Supply M2",
      !country %in% c("GCC", "Gulf Cooperation Council")
    ) %>%
    arrange(country, time_period) %>%
    group_by(country) %>%
    mutate(
      value_lag12 = lag(value, 12),
      m2_growth = (value - value_lag12) / value_lag12 * 100
    ) %>%
    filter(year == year_filter, !is.na(m2_growth)) %>%
    summarize(ind_m2_growth = mean(m2_growth, na.rm = TRUE), .groups = "drop") %>%
    rename(uName = country)
  
  return(m2_growth)
}

#' Real GDP Growth Rate (Raw)
#'
#' Year-over-year real GDP growth
#'
#' @param na_data National Accounts dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_gdp_growth
calc_gdp_growth_raw <- function(na_data, year_filter) {
  
  if (is.null(na_data)) {
    return(tibble(uName = character(), ind_gdp_growth = numeric()))
  }
  
  gdp_growth <- na_data %>%
    filter(
      indicator == "Gross Domestic Product at Constant Prices",
      frequency == "Annual",
      !country %in% c("GCC", "Gulf Cooperation Council")
    ) %>%
    arrange(country, year) %>%
    group_by(country) %>%
    mutate(
      gdp_growth = (value / lag(value) - 1) * 100
    ) %>%
    filter(year == year_filter, !is.na(gdp_growth)) %>%
    summarize(ind_gdp_growth = first(gdp_growth), .groups = "drop") %>%
    rename(uName = country)
  
  return(gdp_growth)
}

#' Banking Penetration (Raw)
#'
#' Number of GCC banks operating in each member state per million population
#'
#' @param common_market Common Market dataset
#' @param population_data Population dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_39_banking
calc_banking_raw <- function(common_market, population_data, year_filter) {
  
  if (is.null(common_market)) {
    return(tibble(uName = character(), ind_39_banking = numeric()))
  }
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  # Get GCC banks count by HOST country (use only KN_TTL aggregate rows
  # to avoid double-counting with nationality-breakdown rows)
  gcc_banks <- common_market %>%
    filter(
      indicator_code == "KN_A15",
      citizen_code == "KN_TTL",
      year == year_filter,
      host_country %in% gcc_countries
    )

  # Return NA if no data for this year
  if (nrow(gcc_banks) == 0) {
    return(tibble(uName = gcc_countries, ind_39_banking = NA_real_))
  }

  gcc_banks <- gcc_banks %>%
    select(country = host_country, gcc_banks_count = value)
  
  # Get population
  pop <- get_total_population(population_data, year_filter) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council"))
  
  banking <- gcc_banks %>%
    left_join(pop, by = "country") %>%
    mutate(ind_39_banking = gcc_banks_count / (population / 1000000)) %>%
    select(uName = country, ind_39_banking)
  
  # Ensure all countries present
  all_countries <- tibble(uName = gcc_countries)
  banking <- all_countries %>%
    left_join(banking, by = "uName")
  
  return(banking)
}


#' Stock Market Openness (Raw)
#'
#' Composite measure of stock market integration:
#' - % of companies open to GCC citizens
#' - % of capital open to GCC citizens
#' - Number of GCC shareholders
#'
#' @param common_market Common Market dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_44_stock
calc_stock_market_raw <- function(common_market, year_filter) {
  
  if (is.null(common_market)) {
    return(tibble(uName = character(), ind_44_stock = numeric()))
  }
  
  # Total companies
  total_companies <- common_market %>%
    filter(
      indicator == "No. of Stock Companies",
      year == year_filter
    ) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    select(country, total_companies = value)
  
  # GCC-open companies
  gcc_companies <- common_market %>%
    filter(
      indicator == "No. of Stock Companies Permitted for GCC Citizens to Trade and Own",
      year == year_filter
    ) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    select(country, gcc_companies = value)
  
  # Total capital
  total_cap <- common_market %>%
    filter(
      indicator == "Capital of Stock Companies",
      year == year_filter
    ) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    select(country, total_cap = value)
  
  # GCC-open capital
  gcc_cap <- common_market %>%
    filter(
      indicator == "Capital of Stock Companies Permitted for GCC Citizens to Trade and Own",
      year == year_filter
    ) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    select(country, gcc_cap = value)
  
  # Combine and calculate composite
  stock_market <- total_companies %>%
    full_join(gcc_companies, by = "country") %>%
    full_join(total_cap, by = "country") %>%
    full_join(gcc_cap, by = "country") %>%
    mutate(
      company_openness = if_else(total_companies > 0,
                                  (gcc_companies / total_companies) * 100,
                                  NA_real_),
      capital_openness = if_else(total_cap > 0,
                                  (gcc_cap / total_cap) * 100,
                                  NA_real_),
      # Composite score (simple average of the two ratios)
      ind_44_stock = (coalesce(company_openness, 0) + coalesce(capital_openness, 0)) / 2
    ) %>%
    select(uName = country, ind_44_stock)
  
  return(stock_market)
}

#' Inward FDI Intensity (Raw)
#'
#' Inward FDI stock as % of GDP
#' Note: Proxy for intra-GCC FDI (bilateral data not available)
#'
#' @param fdi_data FDI dataset
#' @param na_data National Accounts dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_31_fdi
calc_fdi_raw <- function(fdi_data, na_data, year_filter) {
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  if (is.null(fdi_data)) {
    return(tibble(uName = gcc_countries, ind_31_fdi = NA_real_))
  }
  
  # Get inward FDI by country
  inward_fdi <- fdi_data %>%
    filter(
      flow == "Inward FDI",
      country %in% gcc_countries,
      year == year_filter
    ) %>%
    select(country, fdi_value = value)
  
  # Get GDP
  gdp <- get_gdp(na_data, "total", year_filter) %>%
    filter(country %in% gcc_countries)
  
  # Calculate FDI as % of GDP
  fdi_intensity <- inward_fdi %>%
    left_join(gdp, by = "country") %>%
    mutate(
      ind_31_fdi = (fdi_value / gdp) * 100
    ) %>%
    select(uName = country, ind_31_fdi)
  
  return(fdi_intensity)
}

#' Bank Depth (Raw)
#'
#' Total bank assets as % of GDP
#'
#' @param monetary_data Monetary dataset
#' @param na_data National Accounts dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_bank_depth
calc_bank_depth_raw <- function(monetary_data, na_data, year_filter) {
  
  if (is.null(monetary_data)) {
    return(tibble(uName = character(), ind_bank_depth = numeric()))
  }
  
  # Get bank assets (average over year)
  bank_assets <- monetary_data %>%
    filter(
      indicator == "Banks' Total Assets\\ Liabilities",
      year == year_filter,
      !country %in% c("GCC", "Gulf Cooperation Council")
    ) %>%
    group_by(country) %>%
    summarize(banks_total_assets = mean(value, na.rm = TRUE), .groups = "drop")
  
  # Get GDP
  gdp <- get_gdp(na_data, "total", year_filter)
  
  # Calculate depth ratio
  bank_depth <- bank_assets %>%
    left_join(gdp, by = "country") %>%
    mutate(
      ind_bank_depth = (banks_total_assets / gdp) * 100
    ) %>%
    select(uName = country, ind_bank_depth)
  
  return(bank_depth)
}

#' Fiscal Balance Ratio (Raw)
#'
#' Surplus (Deficit) / Total Revenues * 100.
#' Positive = surplus, negative = deficit. Higher values indicate
#' stronger fiscal position, supporting macroeconomic convergence.
#'
#' @param fiscal_data Fiscal dataset from load_fiscal_csv()
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_fiscal_balance
calc_fiscal_balance_raw <- function(fiscal_data, year_filter) {

  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")

  if (is.null(fiscal_data)) {
    return(tibble(uName = gcc_countries, ind_fiscal_balance = NA_real_))
  }

  total_rev <- fiscal_data %>%
    filter(item == "Total Revenues", year == year_filter) %>%
    select(country, total_rev = value)

  surplus <- fiscal_data %>%
    filter(item == "Surplus ( Deficit )", year == year_filter) %>%
    select(country, surplus = value)

  result <- total_rev %>%
    inner_join(surplus, by = "country") %>%
    mutate(
      ind_fiscal_balance = (surplus / total_rev) * 100
    ) %>%
    select(uName = country, ind_fiscal_balance)

  # Ensure all countries present
  all_countries <- tibble(uName = gcc_countries)
  result <- all_countries %>%
    left_join(result, by = "uName")

  return(result)
}

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  FINANCIAL INDICATORS MODULE LOADED
=======================================================

Main function:
  - extract_financial_raw()  : Extract all financial indicators

Individual indicators:
  - ind_inflation: Annual inflation rate (%)
  - ind_m2_growth: M2 money supply growth (%)
  - ind_gdp_growth: Real GDP growth (%)
  - ind_39_banking: Banks per million population
  - ind_44_stock: Stock market openness (%)
  - ind_31_fdi: Intra-GCC FDI (% of GDP)
  - ind_bank_depth: Bank assets (% of GDP)
  - ind_fiscal_balance: Fiscal balance ratio (% of revenues)

=======================================================
")
