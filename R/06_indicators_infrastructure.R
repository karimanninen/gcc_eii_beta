# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - INFRASTRUCTURE INDICATORS
# =============================================================================
#
# Purpose:
#   Extract RAW (unnormalized) infrastructure indicator values.
#   COINr will handle normalization in the build phase.
#
# Indicators:
#   - ind_3_aviation: Intra-GCC air passengers (thousands)
#   - ind_elec_pc: Electricity production per capita (kWh/person)
#
# Note: Additional infrastructure indicators (railway, ports, digital) are
# planned for the full 90-indicator framework but require external data
# sources not yet integrated.
#
# Data Sources:
#   - Tourism data (aviation connectivity via air tourists)
#   - Energy data (electricity production)
#   - Population data (for per capita calculations)
#
# =============================================================================

# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

#' Extract Raw Infrastructure Indicators
#'
#' Extracts all infrastructure raw indicator values for a given year.
#'
#' @param data_list List of GCC-Stat datasets (from load_gcc_data())
#' @param year_filter Year to extract
#' @return Tibble with uName and raw indicator columns
#' @export
extract_infrastructure_raw <- function(data_list, year_filter) {
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  message(paste("    Extracting infrastructure indicators for", year_filter))
  
  # Extract individual indicators
  aviation <- calc_aviation_raw(data_list$tourism, year_filter)
  electricity <- calc_electricity_raw(data_list$energy, data_list$population, year_filter)
  
  # Combine all indicators
  infra_raw <- tibble(uName = gcc_countries) %>%
    left_join(aviation, by = "uName") %>%
    left_join(electricity, by = "uName")
  
  return(infra_raw)
}

# =============================================================================
# INDIVIDUAL INDICATOR FUNCTIONS
# =============================================================================

#' Aviation Connectivity (Raw)
#' Aviation Connectivity (Raw)
#'
#' Intra-GCC inbound tourists (thousands) as proxy for connectivity
#' Note: Uses total GCC inbound as most countries don't report air-only separately
#'
#' @param tourism_data Tourism dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_3_aviation
calc_aviation_raw <- function(tourism_data, year_filter) {
  
  if (is.null(tourism_data)) {
    return(tibble(uName = character(), ind_3_aviation = numeric()))
  }
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  # Use total GCC inbound as proxy (better coverage than air-only)
  aviation <- tourism_data %>%
    filter(
      indicator == "Inbound Tourism from GCC (000)",
      year == year_filter,
      country %in% gcc_countries
    ) %>%
    group_by(country) %>%
    summarize(ind_3_aviation = sum(value, na.rm = TRUE), .groups = "drop") %>%
    rename(uName = country)
  
  return(aviation)
}

#' Electricity Per Capita (Raw)
#'
#' Electricity production per capita (kWh per person)
#' Indicator of energy infrastructure capacity
#'
#' @param energy_data Energy dataset
#' @param population_data Population dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_elec_pc
calc_electricity_raw <- function(energy_data, population_data, year_filter) {
  
  if (is.null(energy_data)) {
    return(tibble(uName = character(), ind_elec_pc = numeric()))
  }
  
  # Get electricity production
  elec_prod <- energy_data %>%
    filter(
      indicator == "Electricity production",
      unit == "GWH",
      year == year_filter
    ) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    group_by(country) %>%
    summarize(elec_production_gwh = sum(value, na.rm = TRUE), .groups = "drop")
  
  # Get population
  pop <- get_total_population(population_data, year_filter) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council"))
  
  # Calculate per capita (convert GWH to kWh)
  electricity_pc <- elec_prod %>%
    left_join(pop, by = "country") %>%
    mutate(
      # GWH * 1,000,000 = kWh, then divide by population
      ind_elec_pc = (elec_production_gwh * 1000000) / population
    ) %>%
    select(uName = country, ind_elec_pc)
  
  return(electricity_pc)
}

# =============================================================================
# PLACEHOLDER FUNCTIONS FOR FUTURE INDICATORS
# =============================================================================

#' Railway Connectivity (Raw) - Placeholder
#'
#' GCC Railway project completion rate
#' Requires external data from GCC Railway Committee
#'
#' @param external_data External datasets
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_1_railway
calc_railway_raw <- function(external_data, year_filter) {
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  tibble(uName = gcc_countries, ind_1_railway = NA_real_)
}

#' Port Connectivity (Raw) - Placeholder
#'
#' Port connectivity and freight efficiency index
#' Requires UNCTAD LSCI and port authority data
#'
#' @param external_data External datasets
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_2_port
calc_port_raw <- function(external_data, year_filter) {
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  tibble(uName = gcc_countries, ind_2_port = NA_real_)
}

#' GCCIA Electricity Trade (Raw) - Placeholder
#'
#' Power grid interconnection utilization
#' Requires GCCIA data
#'
#' @param external_data External datasets
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_6_gccia
calc_gccia_raw <- function(external_data, year_filter) {
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  tibble(uName = gcc_countries, ind_6_gccia = NA_real_)
}

#' Digital Infrastructure (Raw) - Placeholder
#'
#' Cross-border data flows, 5G coverage, etc.
#' Requires ITU and telecom regulator data
#'
#' @param external_data External datasets
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_8_digital
calc_digital_raw <- function(external_data, year_filter) {
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  tibble(uName = gcc_countries, ind_8_digital = NA_real_)
}

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  INFRASTRUCTURE INDICATORS MODULE LOADED
=======================================================

Main function:
  - extract_infrastructure_raw() : Extract all infrastructure indicators

Currently available:
  - ind_3_aviation: Intra-GCC air passengers (thousands)
  - ind_elec_pc: Electricity per capita (kWh/person)

Placeholders for future:
  - ind_1_railway: GCC Railway completion
  - ind_2_port: Port connectivity index
  - ind_6_gccia: GCCIA utilization
  - ind_8_digital: Digital infrastructure

=======================================================
")
