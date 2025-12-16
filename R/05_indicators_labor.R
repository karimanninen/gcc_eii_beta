# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - LABOR & MOBILITY INDICATORS
# =============================================================================
#
# Purpose:
#   Extract RAW (unnormalized) labor and mobility indicator values.
#   COINr will handle normalization in the build phase.
#
# Indicators:
#   - ind_69_labor: GCC workers in other member states per 1000 population
#   - ind_71_student: GCC students in other member states per 1000 population
#   - ind_72_tourism: Intra-GCC tourism as % of total inbound
#   - ind_lfpr: Labor force participation rate (%)
#   - ind_unemployment: Unemployment rate (%)
#
# Data Sources:
#   - Common Market (labor mobility, students)
#   - Tourism data
#   - Labor Force data
#
# =============================================================================

# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

#' Extract Raw Labor Indicators
#'
#' Extracts all labor/mobility raw indicator values for a given year.
#'
#' @param data_list List of GCC-Stat datasets (from load_gcc_data())
#' @param year_filter Year to extract
#' @return Tibble with uName and raw indicator columns
#' @export
extract_labor_raw <- function(data_list, year_filter) {
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  message(paste("    Extracting labor indicators for", year_filter))
  
  # Extract individual indicators
  labor_mobility <- calc_labor_mobility_raw(data_list$common_market, 
                                             data_list$population, year_filter)
  student_mobility <- calc_student_mobility_raw(data_list$common_market,
                                                 data_list$population, year_filter)
  tourism <- calc_tourism_raw(data_list$tourism, year_filter)
  lfpr <- calc_lfpr_raw(data_list$labor, year_filter)
  unemployment <- calc_unemployment_raw(data_list$labor, year_filter)
  
  # Combine all indicators
  labor_raw <- tibble(uName = gcc_countries) %>%
    left_join(labor_mobility, by = "uName") %>%
    left_join(student_mobility, by = "uName") %>%
    left_join(tourism, by = "uName") %>%
    left_join(lfpr, by = "uName") %>%
    left_join(unemployment, by = "uName")
  
  return(labor_raw)
}

# =============================================================================
# INDIVIDUAL INDICATOR FUNCTIONS
# =============================================================================

#' Labor Mobility - GCC Workers (Raw)
#'
#' Total GCC citizens working in other member states (govt + private)
#' per 1000 population of origin country.
#'
#' @param common_market Common Market dataset
#' @param population_data Population dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_69_labor
#' Labor Mobility - GCC Workers (Raw)
calc_labor_mobility_raw <- function(common_market, population_data, year_filter) {
  
  if (is.null(common_market)) {
    return(tibble(uName = character(), ind_69_labor = numeric()))
  }
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  # Get workers in government + private sectors
  gcc_workers <- common_market %>%
    filter(
      indicator_code %in% c("KN_A2", "KN_A3"),
      country_code == "KN_TTL",  # GCC total
      year == year_filter
    ) %>%
    filter(citizen_country %in% gcc_countries) %>%
    group_by(citizen_country) %>%
    summarize(gcc_workers_count = sum(value, na.rm = TRUE), .groups = "drop") %>%
    rename(country = citizen_country)
  
  # Return NA if no data
  if (nrow(gcc_workers) == 0) {
    return(tibble(uName = gcc_countries, ind_69_labor = NA_real_))
  }
  
  # Get population
  pop <- get_total_population(population_data, year_filter) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council"))
  
  labor_mobility <- gcc_workers %>%
    left_join(pop, by = "country") %>%
    mutate(ind_69_labor = (gcc_workers_count / population) * 1000) %>%
    select(uName = country, ind_69_labor)
  
  # Ensure all countries present
  all_countries <- tibble(uName = gcc_countries)
  labor_mobility <- all_countries %>%
    left_join(labor_mobility, by = "uName")
  
  return(labor_mobility)
}

#' Student Mobility (Raw)
calc_student_mobility_raw <- function(common_market, population_data, year_filter) {
  
  if (is.null(common_market)) {
    return(tibble(uName = character(), ind_71_student = numeric()))
  }
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  # Get students in general education
  gcc_students <- common_market %>%
    filter(
      indicator_code == "KN_A9",
      country_code == "KN_TTL",  # GCC total
      year == year_filter
    ) %>%
    filter(citizen_country %in% gcc_countries) %>%
    group_by(citizen_country) %>%
    summarize(gcc_students_count = sum(value, na.rm = TRUE), .groups = "drop") %>%
    rename(country = citizen_country)
  
  # Return NA if no data
  if (nrow(gcc_students) == 0) {
    return(tibble(uName = gcc_countries, ind_71_student = NA_real_))
  }
  
  # Get population
  pop <- get_total_population(population_data, year_filter) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council"))
  
  student_mobility <- gcc_students %>%
    left_join(pop, by = "country") %>%
    mutate(ind_71_student = (gcc_students_count / population) * 1000) %>%
    select(uName = country, ind_71_student)
  
  # Ensure all countries present
  all_countries <- tibble(uName = gcc_countries)
  student_mobility <- all_countries %>%
    left_join(student_mobility, by = "uName")
  
  return(student_mobility)
}


#' Tourism Share (Raw)
#'
#' Intra-GCC tourism as percentage of total inbound tourists
#'
#' @param tourism_data Tourism dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_72_tourism
calc_tourism_raw <- function(tourism_data, year_filter) {
  
  if (is.null(tourism_data)) {
    return(tibble(uName = character(), ind_72_tourism = numeric()))
  }
  
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  
  # Intra-GCC tourists (single aggregate value per country)
  intra_gcc <- tourism_data %>%
    filter(
      indicator == "Inbound Tourism from GCC (000)",
      year == year_filter,
      country %in% gcc_countries
    ) %>%
    group_by(country) %>%
    summarize(intra_gcc_tourists = sum(value, na.rm = TRUE), .groups = "drop")
  
  # Total inbound visitors (includes same-day)
  total_visitors <- tourism_data %>%
    filter(
      indicator == "Total number of inbound visitors (000)",
      year == year_filter,
      country %in% gcc_countries
    ) %>%
    group_by(country) %>%
    summarize(total_visitors = sum(value, na.rm = TRUE), .groups = "drop")
  
  # Calculate share
  tourism_share <- intra_gcc %>%
    full_join(total_visitors, by = "country") %>%
    mutate(
      ind_72_tourism = if_else(total_visitors > 0,
                               (intra_gcc_tourists / total_visitors) * 100,
                               NA_real_)
    ) %>%
    select(uName = country, ind_72_tourism)
  
  return(tourism_share)
}

#' Labor Force Participation Rate (Raw)
#'
#' Labor Force Participation Rate (Raw)
calc_lfpr_raw <- function(labor_data, year_filter) {
  
  if (is.null(labor_data)) {
    return(tibble(uName = character(), ind_lfpr = numeric()))
  }
  
  lfpr <- labor_data %>%
    filter(
      indicator == "Labour Force Participation Rate",
      nationality %in% c("Citzens", "Total"),  # Note: typo in source data
      labour_aspect == "Total",
      gender == "Total",
      year == year_filter
    ) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    # Average across quarters if multiple observations per country-year
    group_by(country) %>%
    summarize(ind_lfpr = mean(value, na.rm = TRUE), .groups = "drop") %>%
    rename(uName = country)
  
  return(lfpr)
}

#' Unemployment Rate (Raw)
calc_unemployment_raw <- function(labor_data, year_filter) {
  
  if (is.null(labor_data)) {
    return(tibble(uName = character(), ind_unemployment = numeric()))
  }
  
  unemployment <- labor_data %>%
    filter(
      indicator == "Unemployment Rate",
      nationality == "Citzens",  # Note: typo in source data
      labour_aspect == "Total",
      gender == "Total",
      year == year_filter
    ) %>%
    filter(!country %in% c("GCC", "Gulf Cooperation Council")) %>%
    # Average across quarters if multiple observations per country-year
    group_by(country) %>%
    summarize(ind_unemployment = mean(value, na.rm = TRUE), .groups = "drop") %>%
    rename(uName = country)
  
  return(unemployment)
}

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  LABOR INDICATORS MODULE LOADED
=======================================================

Main function:
  - extract_labor_raw()      : Extract all labor indicators

Individual indicators:
  - ind_69_labor: GCC workers per 1000 population
  - ind_71_student: GCC students per 1000 population
  - ind_72_tourism: Intra-GCC tourism share (%)
  - ind_lfpr: Labor force participation rate (%)
  - ind_unemployment: Unemployment rate (%)

=======================================================
")
