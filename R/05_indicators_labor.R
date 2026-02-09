# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - LABOR & MOBILITY INDICATORS
# =============================================================================
#
# Purpose:
#   Extract RAW (unnormalized) labor and mobility indicator values.
#   COINr will handle normalization in the build phase.
#
# Indicators:
#   - ind_69_labor: Total GCC workers hosted per 1000 population
#   - ind_71_student: Total GCC students hosted per 1000 population
#   - ind_72_tourism: Intra-GCC tourism as % of total inbound
#   - ind_lfpr: Labor force participation rate (%)
#   - ind_unemployment: Unemployment rate (%)
#
# Data Sources:
#   - Common Market (labor mobility, students)
#   - Tourism data
#   - Labor Force data
#
# Methodology Note:
#   Labor and student mobility use the HOST-COUNTRY perspective:
#   "How many GCC workers/students does each country host?"
#   This uses aggregate totals (Citizen=KN_TTL) per host country,
#   giving continuous coverage 2015-2023 without requiring the
#   citizenship breakdown that is only available 2016-2021.
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

#' Labor Mobility - GCC Workers Hosted (Raw)
#'
#' Total GCC citizens working in each host country (govt + private)
#' per 1000 host-country population. Uses aggregate totals per host
#' country (Citizen=Total), giving coverage from 2000-2023.
#'
#' @param common_market Common Market dataset
#' @param population_data Population dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_69_labor
calc_labor_mobility_raw <- function(common_market, population_data, year_filter) {

  if (is.null(common_market)) {
    return(tibble(uName = character(), ind_69_labor = numeric()))
  }

  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")

  # Host-country approach: total GCC workers hosted by each country
  # citizen_code == "KN_TTL" gives the aggregate across all citizenships
  gcc_workers <- common_market %>%
    filter(
      indicator_code %in% c("KN_A2", "KN_A3"),
      citizen_code == "KN_TTL",
      host_country %in% gcc_countries,
      year == year_filter
    ) %>%
    group_by(host_country) %>%
    summarize(gcc_workers_count = sum(value, na.rm = TRUE), .groups = "drop") %>%
    rename(country = host_country)

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

#' Student Mobility - GCC Students Hosted (Raw)
#'
#' Total GCC students in general education hosted by each country
#' per 1000 host-country population. Uses nationality-level breakdowns
#' (citizen_code != KN_TTL) summed per host country, which avoids the
#' UAE data quality issue where the KN_TTL row reports total enrollment
#' instead of GCC students.
#'
#' Nationality-level data is available for 2016-2021. Years 2015 and
#' 2022-2023 are linearly extrapolated from the nearest two data years.
#'
#' @param common_market Common Market dataset
#' @param population_data Population dataset
#' @param year_filter Year to calculate
#' @return Tibble with uName, ind_71_student
calc_student_mobility_raw <- function(common_market, population_data, year_filter) {

  if (is.null(common_market)) {
    return(tibble(uName = character(), ind_71_student = numeric()))
  }

  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")

  # Years with nationality-level data
  data_years <- 2016:2021

  if (year_filter %in% data_years) {
    # --- Direct calculation from nationality breakdowns ---
    gcc_students <- common_market %>%
      filter(
        indicator_code == "KN_A9",
        citizen_code != "KN_TTL",
        sex_code == "KN_T",
        host_country %in% gcc_countries,
        year == year_filter
      ) %>%
      group_by(host_country) %>%
      summarize(gcc_students_count = sum(value, na.rm = TRUE), .groups = "drop") %>%
      rename(country = host_country)

  } else {
    # --- Extrapolate from nearest two data years ---
    if (year_filter < min(data_years)) {
      y1 <- min(data_years)
      y2 <- y1 + 1
    } else {
      y2 <- max(data_years)
      y1 <- y2 - 1
    }

    counts_y1 <- common_market %>%
      filter(indicator_code == "KN_A9", citizen_code != "KN_TTL",
             sex_code == "KN_T", host_country %in% gcc_countries, year == y1) %>%
      group_by(host_country) %>%
      summarize(count_y1 = sum(value, na.rm = TRUE), .groups = "drop")

    counts_y2 <- common_market %>%
      filter(indicator_code == "KN_A9", citizen_code != "KN_TTL",
             sex_code == "KN_T", host_country %in% gcc_countries, year == y2) %>%
      group_by(host_country) %>%
      summarize(count_y2 = sum(value, na.rm = TRUE), .groups = "drop")

    gcc_students <- counts_y1 %>%
      left_join(counts_y2, by = "host_country") %>%
      mutate(
        annual_change = count_y2 - count_y1,
        gcc_students_count = pmax(0, count_y1 + annual_change * (year_filter - y1))
      ) %>%
      select(country = host_country, gcc_students_count)

    message(paste0("  ind_71_student ", year_filter,
                   ": extrapolated from ", y1, "-", y2))
  }

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
  - ind_69_labor: GCC workers hosted per 1000 population
  - ind_71_student: GCC students hosted per 1000 population
  - ind_72_tourism: Intra-GCC tourism share (%)
  - ind_lfpr: Labor force participation rate (%)
  - ind_unemployment: Unemployment rate (%)

=======================================================
")
