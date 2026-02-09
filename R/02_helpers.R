# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - HELPER FUNCTIONS
# =============================================================================
#
# Purpose:
#   Utility functions used across all indicator calculations.
#   These are pure functions with no side effects.
#
# Contents:
#   1. Country Standardization (moved to 01_data_loading.R)
#   2. Data Extraction Helpers
#   3. Statistical Functions
#   4. Normalization Functions (for reference - COINr will handle these)
#   5. Safe Filtering Functions
#   6. Validation Functions
#
# =============================================================================

library(tidyverse)

# =============================================================================
# SECTION 1: RE-EXPORTS FROM DATA LOADING
# =============================================================================

# These functions are defined in 01_data_loading.R but re-exported here
# for convenience. If 01_data_loading.R is sourced first, these will be
# available automatically.

# standardize_countries() - see 01_data_loading.R
# get_gdp() - see 01_data_loading.R
# get_total_population() - see 01_data_loading.R
# calculate_cv() - see 01_data_loading.R

# =============================================================================
# SECTION 2: DATA EXTRACTION HELPERS
# =============================================================================

#' Safely Filter Data by Year with Fallback
#'
#' Filters dataframe for a specific year. If year not available,
#' uses the closest available year and issues a warning.
#'
#' @param df Dataframe to filter
#' @param year_col Name of year column (as string)
#' @param target_year Year to filter for
#' @return Filtered dataframe
#' @export
safe_year_filter <- function(df, year_col = "year", target_year) {
  
  if (!year_col %in% names(df)) {
    warning(paste("Year column", year_col, "not found in data"))
    return(tibble())
  }
  
  available_years <- df[[year_col]] %>% unique() %>% na.omit()
  
  if (length(available_years) == 0) {
    warning("No valid years in dataset")
    return(tibble())
  }
  
  if (!target_year %in% available_years) {
    closest_year <- available_years[which.min(abs(available_years - target_year))]
    warning(paste("Year", target_year, "not available. Using closest year:", closest_year))
    target_year <- closest_year
  }
  
  df %>% filter(!!sym(year_col) == target_year)
}

#' Get Indicator Data for Specific Country-Year
#'
#' Extracts a single indicator value for a country-year combination
#'
#' @param df Dataframe with indicator data
#' @param indicator_name Name of indicator to extract
#' @param country_name Country to extract
#' @param year_filter Year to extract
#' @param value_col Name of value column (default: "value")
#' @return Single numeric value or NA
get_indicator_value <- function(df, indicator_name, country_name, year_filter, 
                                 value_col = "value") {
  
  result <- df %>%
    filter(
      indicator == indicator_name,
      country == country_name,
      year == year_filter
    )
  
  if (nrow(result) == 0) {
    return(NA_real_)
  }
  
  # If multiple values, take mean
  mean(result[[value_col]], na.rm = TRUE)
}

#' Extract Wide Format Indicator Data
#'
#' Converts long-format indicator data to wide format by country
#'
#' @param df Long-format dataframe
#' @param indicator_name Indicator to pivot
#' @param year_filter Year to filter
#' @param value_col Column containing values
#' @param output_col Name for output column
#' @return Wide tibble with one row per country
extract_indicator_wide <- function(df, indicator_name, year_filter, 
                                    value_col = "value", output_col = NULL) {
  
  if (is.null(output_col)) {
    output_col <- janitor::make_clean_names(indicator_name)
  }
  
  df %>%
    filter(
      indicator == indicator_name,
      year == year_filter
    ) %>%
    group_by(country) %>%
    summarize(!!output_col := mean(!!sym(value_col), na.rm = TRUE), .groups = "drop")
}

# =============================================================================
# SECTION 3: STATISTICAL FUNCTIONS
# =============================================================================

#' Calculate Coefficient of Variation
#'
#' Used for convergence indicators to measure dispersion across countries
#'
#' @param x Numeric vector
#' @return CV as percentage (0-100+), or NA if invalid
#' @export
calculate_cv <- function(x) {
  if (all(is.na(x)) || length(x) < 2) return(NA_real_)
  mean_val <- mean(x, na.rm = TRUE)
  if (mean_val == 0) return(NA_real_)
  sd(x, na.rm = TRUE) / mean_val * 100
}

#' Calculate Year-over-Year Growth Rate
#'
#' @param x Numeric vector of values
#' @param periods Number of periods for growth calculation (default: 1)
#' @return Vector of growth rates as percentages
calculate_yoy_growth <- function(x, periods = 1) {
  if (length(x) <= periods) return(rep(NA_real_, length(x)))
  
  growth <- c(rep(NA_real_, periods), 
              (x[(periods + 1):length(x)] / x[1:(length(x) - periods)] - 1) * 100)
  
  return(growth)
}

#' Calculate Herfindahl-Hirschman Index
#'
#' Used for measuring concentration/diversification
#'
#' @param shares Vector of market shares (should sum to 1 or 100)
#' @return HHI value (0-10000 if percentages, 0-1 if proportions)
calculate_hhi <- function(shares) {
  if (all(is.na(shares))) return(NA_real_)
  
  shares_clean <- shares[!is.na(shares)]
  
  # Normalize to proportions if given as percentages
  if (sum(shares_clean) > 1.5) {
    shares_clean <- shares_clean / sum(shares_clean)
  }
  
  sum(shares_clean^2)
}

#' Calculate Z-Score
#'
#' @param x Numeric vector
#' @return Vector of z-scores
calculate_zscore <- function(x) {
  if (all(is.na(x))) return(rep(NA_real_, length(x)))
  
  mean_val <- mean(x, na.rm = TRUE)
  sd_val <- sd(x, na.rm = TRUE)
  
  if (sd_val == 0) return(rep(0, length(x)))
  
  (x - mean_val) / sd_val
}

#' Calculate Distance from Mean
#'
#' Used for convergence scoring - how far each country is from the mean
#'
#' @param x Numeric vector
#' @return Vector of absolute distances from mean
calculate_distance_from_mean <- function(x) {
  if (all(is.na(x))) return(rep(NA_real_, length(x)))
  abs(x - mean(x, na.rm = TRUE))
}

# =============================================================================
# SECTION 4: NORMALIZATION FUNCTIONS
# =============================================================================
# NOTE: In COINr migration, these will be replaced by COINr's Normalise()
# function. Kept here for reference and potential manual calculations.
# =============================================================================

#' Min-Max Normalization
#'
#' Scales values to 0-100 range
#'
#' @param x Numeric vector
#' @param inverse Logical, if TRUE lower values get higher scores
#' @param min_val Optional minimum for scaling (default: min of x)
#' @param max_val Optional maximum for scaling (default: max of x)
#' @return Normalized vector (0-100)
#' @export
normalize_minmax <- function(x, inverse = FALSE, min_val = NULL, max_val = NULL) {
  
  if (all(is.na(x))) return(rep(NA_real_, length(x)))
  
  if (is.null(min_val)) min_val <- min(x, na.rm = TRUE)
  if (is.null(max_val)) max_val <- max(x, na.rm = TRUE)
  
  if (min_val == max_val) return(rep(50, length(x)))
  
  normalized <- (x - min_val) / (max_val - min_val) * 100
  
  if (inverse) normalized <- 100 - normalized
  
  # Ensure bounds
  pmax(0, pmin(100, normalized))
}

#' Z-Score Normalization
#'
#' Scales based on standard deviations, then maps to 0-100
#'
#' @param x Numeric vector
#' @param center Target center score (default: 50)
#' @param scale Multiplier for standard deviation (default: 15)
#' @return Normalized vector (capped 0-100)
#' @export
normalize_zscore <- function(x, center = 50, scale = 15) {
  
  if (all(is.na(x))) return(rep(NA_real_, length(x)))
  
  mean_val <- mean(x, na.rm = TRUE)
  sd_val <- sd(x, na.rm = TRUE)
  
  if (sd_val == 0) return(rep(center, length(x)))
  
  normalized <- center + ((x - mean_val) / sd_val) * scale
  
  # Cap at 0-100
  pmax(0, pmin(100, normalized))
}

#' Convergence Score from CV
#'
#' Converts a CV (coefficient of variation) to a convergence score.
#' Lower CV = higher convergence = higher score.
#'
#' @param cv Coefficient of variation (percentage)
#' @param max_cv CV value that gives score of 0 (default: 100)
#' @return Convergence score (0-100)
cv_to_convergence_score <- function(cv, max_cv = 100) {
  if (is.na(cv)) return(NA_real_)
  pmax(0, pmin(100, 100 - cv))
}

# =============================================================================
# SECTION 5: VALIDATION FUNCTIONS
# =============================================================================

#' Check if All GCC Countries Present
#'
#' @param df Dataframe to check
#' @param country_col Name of country column
#' @return Logical TRUE if all 6 countries present
check_gcc_complete <- function(df, country_col = "country") {
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  all(gcc_countries %in% unique(df[[country_col]]))
}

#' Get Missing Countries
#'
#' @param df Dataframe to check
#' @param country_col Name of country column
#' @return Character vector of missing country names
get_missing_countries <- function(df, country_col = "country") {
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")
  setdiff(gcc_countries, unique(df[[country_col]]))
}

#' Validate Indicator Range
#'
#' Checks if indicator values are within expected range
#'
#' @param x Numeric vector
#' @param min_expected Minimum expected value
#' @param max_expected Maximum expected value
#' @param indicator_name Name for warning message
#' @return Logical vector (TRUE = valid)
validate_range <- function(x, min_expected, max_expected, indicator_name = "indicator") {
  
  out_of_range <- x < min_expected | x > max_expected
  out_of_range[is.na(x)] <- FALSE  # NA is allowed
  
  if (any(out_of_range)) {
    warning(paste(indicator_name, "has", sum(out_of_range), 
                  "values outside expected range [", min_expected, ",", max_expected, "]"))
  }
  
  !out_of_range
}

#' Summarize Data Quality
#'
#' Provides a summary of data quality for an indicator
#'
#' @param x Numeric vector
#' @param indicator_name Name of indicator
#' @return List with quality metrics
summarize_data_quality <- function(x, indicator_name = "indicator") {
  list(
    indicator = indicator_name,
    n_total = length(x),
    n_missing = sum(is.na(x)),
    pct_missing = round(sum(is.na(x)) / length(x) * 100, 1),
    min = min(x, na.rm = TRUE),
    max = max(x, na.rm = TRUE),
    mean = mean(x, na.rm = TRUE),
    sd = sd(x, na.rm = TRUE),
    cv = calculate_cv(x)
  )
}

# =============================================================================
# SECTION 6: AGGREGATION HELPERS
# =============================================================================

#' GDP-Weighted Mean
#'
#' Calculates GDP-weighted average for GCC aggregate
#'
#' @param values Numeric vector of country values
#' @param gdp_weights Numeric vector of GDP values (same order as values)
#' @return Single weighted mean value
gdp_weighted_mean <- function(values, gdp_weights) {
  
  if (all(is.na(values)) || all(is.na(gdp_weights))) {
    return(NA_real_)
  }
  
  # Normalize weights
  weights <- gdp_weights / sum(gdp_weights, na.rm = TRUE)
  
  # Calculate weighted mean, handling NAs
  valid_idx <- !is.na(values) & !is.na(weights)
  
  if (sum(valid_idx) == 0) return(NA_real_)
  
  sum(values[valid_idx] * weights[valid_idx]) / sum(weights[valid_idx])
}

#' Population-Weighted Mean
#'
#' Calculates population-weighted average for GCC aggregate
#'
#' @param values Numeric vector of country values
#' @param pop_weights Numeric vector of population values
#' @return Single weighted mean value
population_weighted_mean <- function(values, pop_weights) {
  gdp_weighted_mean(values, pop_weights)  # Same logic
}

# =============================================================================
# SECTION 7: EXCEL EXPORT
# =============================================================================

#' Export Full Methodology XLSX
#'
#' Exports the complete GCCEII methodology workbook with all data stages
#' extracted directly from the coin object and supporting data frames.
#'
#' @param coin COINr coin object (after aggregation)
#' @param iMeta Indicator metadata tibble
#' @param results Results data frame from get_results()
#' @param gcc_trend GCC trend summary data frame
#' @param output_path Path for output XLSX file
#' @export
export_methodology_xlsx <- function(coin, iMeta, results, gcc_trend,
                                    output_path = "output/GCCEII_Full_Methodology.xlsx") {

  if (!requireNamespace("openxlsx", quietly = TRUE)) {
    message("Package 'openxlsx' not installed. Install with: install.packages('openxlsx')")
    message("Skipping XLSX export.")
    return(invisible(NULL))
  }

  message("Exporting GCCEII Full Methodology XLSX...")

  # Helper to split uCode back into Country and Year
  split_ucode <- function(df) {
    df %>%
      mutate(
        Country = str_remove(uCode, "_\\d{4}$"),
        Year = as.integer(str_extract(uCode, "\\d{4}$"))
      ) %>%
      select(Country, Year, everything(), -uCode) %>%
      arrange(Country, Year)
  }

  # --- Sheet 1: Metadata ---
  sheet_meta <- iMeta %>%
    select(iCode, iName, Level, Parent, Weight, Direction, Type)

  # --- Sheet 2: Raw Data ---
  sheet_raw <- split_ucode(coin$Data$Raw)

  # --- Sheet 3: Normalised Data ---
  sheet_norm <- if ("Normalised" %in% names(coin$Data)) {
    split_ucode(coin$Data$Normalised)
  } else {
    tibble(Note = "Normalised data not available")
  }

  # --- Sheet 4: Aggregated Data ---
  sheet_agg <- if ("Aggregated" %in% names(coin$Data)) {
    split_ucode(coin$Data$Aggregated)
  } else {
    tibble(Note = "Aggregated data not available")
  }

  # --- Sheet 5: Results Summary ---
  sheet_results <- results %>%
    mutate(
      Country = str_remove(uCode, "_\\d{4}$"),
      Year = as.integer(str_extract(uCode, "\\d{4}$"))
    ) %>%
    select(Country, Year, Trade, Financial, Labor,
           Infrastructure, Sustainability, Convergence, Index) %>%
    arrange(Country, Year)

  # --- Sheet 6: Rankings ---
  dim_cols <- c("Trade", "Financial", "Labor", "Infrastructure",
                "Sustainability", "Convergence", "Index")
  years <- sort(unique(sheet_results$Year))

  sheet_rankings <- sheet_results %>%
    select(Country, Year, Index) %>%
    pivot_wider(names_from = Year, values_from = Index,
                names_prefix = "Index_") %>%
    arrange(desc(across(last_col())))

  # Add rank columns
  for (yr in years) {
    idx_col <- paste0("Index_", yr)
    rank_col <- paste0("Rank_", yr)
    if (idx_col %in% names(sheet_rankings)) {
      sheet_rankings <- sheet_rankings %>%
        mutate(!!rank_col := rank(-!!sym(idx_col), na.last = "keep"))
    }
  }

  # --- Sheet 7: GCC Trend ---
  sheet_trend <- gcc_trend

  # --- Sheet 8: Weights ---
  sheet_weights <- iMeta %>%
    filter(Level == 2) %>%
    select(Dimension = iCode, Name = iName, Weight) %>%
    mutate(Weight_Pct = paste0(Weight * 100, "%"))

  # --- Sheet 9: Imputation Log (if available) ---
  sheet_imputation <- if (!is.null(coin$Analysis$Imputation) &&
                          nrow(coin$Analysis$Imputation) > 0) {
    coin$Analysis$Imputation %>%
      mutate(
        Country = str_remove(uCode, "_\\d{4}$"),
        Year = as.integer(str_extract(uCode, "\\d{4}$"))
      ) %>%
      select(Country, Year, indicator, method, imputed_value) %>%
      arrange(indicator, Country, Year)
  } else {
    tibble(Note = "No imputation performed")
  }

  # --- Write workbook ---
  wb <- openxlsx::createWorkbook()

  sheets <- list(
    "1_Metadata" = sheet_meta,
    "2_Raw_Data" = sheet_raw,
    "3_Normalised_Data" = sheet_norm,
    "4_Aggregated_Data" = sheet_agg,
    "5_Results_Summary" = sheet_results,
    "6_Rankings" = sheet_rankings,
    "7_GCC_Trend" = sheet_trend,
    "8_Weights" = sheet_weights,
    "9_Imputation_Log" = sheet_imputation
  )

  for (sheet_name in names(sheets)) {
    openxlsx::addWorksheet(wb, sheet_name)
    openxlsx::writeData(wb, sheet_name, sheets[[sheet_name]])
  }

  openxlsx::saveWorkbook(wb, output_path, overwrite = TRUE)
  message(paste("✓ Methodology XLSX saved to", output_path))

  return(invisible(output_path))
}

# =============================================================================
# SECTION 8: IMPUTATION
# =============================================================================

#' Impute Missing Data in GCCEII Coin
#'
#' Two-pass imputation strategy for panel data (uCode format: "BHR_2023"):
#'   Pass 1: Linear interpolation/extrapolation within each country's time
#'           series. Handles 1-2 year gaps where a country has partial data.
#'           Uses stats::approx() with rule=2 for boundary extrapolation.
#'   Pass 2: EM algorithm (Amelia package) for remaining NAs, typically
#'           countries completely missing for an indicator. Falls back to
#'           year-group median if Amelia is unavailable.
#'
#' Imputed values are tracked in coin$Analysis$Imputation (a tibble with
#' uCode, indicator, method, and imputed_value for each filled cell).
#'
#' @param coin COINr coin object with Raw data
#' @return Coin object with Imputed dataset and imputation log
#' @export
impute_gcceii <- function(coin) {

  message("\n=======================================================")
  message("  IMPUTATION")
  message("=======================================================\n")

  # Extract raw data
  raw_df <- coin$Data$Raw
  imputed_df <- raw_df  # work on a copy

  # Parse panel structure from uCode
  uCodes <- raw_df$uCode
  panel_ids <- str_remove(uCodes, "_\\d{4}$")
  time_ids <- as.integer(str_extract(uCodes, "\\d{4}$"))

  # Indicator columns (everything except uCode)
  ind_cols <- setdiff(names(raw_df), "uCode")

  # Initialise imputation log
  log_rows <- list()

  # =========================================================================
  # PASS 1: Linear interpolation / extrapolation per country time series
  # =========================================================================
  message("Pass 1: Linear interpolation/extrapolation...")

  n_interp <- 0
  unique_panels <- unique(panel_ids)

  for (ind in ind_cols) {
    vals <- raw_df[[ind]]
    new_vals <- vals

    for (pid in unique_panels) {
      idx <- which(panel_ids == pid)
      times <- time_ids[idx]
      v <- vals[idx]

      non_na <- !is.na(v)
      na_pos <- is.na(v)

      if (sum(non_na) >= 2 && any(na_pos)) {
        # rule = 2: extrapolate using boundary values at series ends
        interp <- approx(times[non_na], v[non_na], xout = times, rule = 2)

        # Only fill NAs, keep originals
        new_vals[idx] <- ifelse(na_pos, interp$y, v)

        # Log each imputed cell
        imputed_idx <- idx[na_pos]
        for (ii in imputed_idx) {
          n_interp <- n_interp + 1
          log_rows[[length(log_rows) + 1]] <- tibble(
            uCode = uCodes[ii],
            indicator = ind,
            method = "linear_interp",
            imputed_value = new_vals[ii]
          )
        }
      }
    }

    imputed_df[[ind]] <- new_vals
  }

  message(paste("  Imputed", n_interp, "values via linear interpolation/extrapolation"))

  # =========================================================================
  # PASS 2: EM algorithm for remaining NAs
  # =========================================================================
  # Identify columns with remaining NAs (skip all-NA columns like
  # infrastructure placeholders — EM cannot impute those)
  remaining_na_per_col <- sapply(ind_cols, function(ic) sum(is.na(imputed_df[[ic]])))
  n_rows <- nrow(imputed_df)
  em_eligible <- names(remaining_na_per_col[remaining_na_per_col > 0 &
                                             remaining_na_per_col < n_rows])
  remaining_na <- sum(remaining_na_per_col[em_eligible])

  if (remaining_na > 0) {
    message(paste("Pass 2: EM imputation for", remaining_na, "remaining NAs",
                  "across", length(em_eligible), "indicators..."))

    em_success <- FALSE

    if (requireNamespace("Amelia", quietly = TRUE)) {
      # Prepare numeric matrix — only columns with partial NAs plus
      # complete columns (Amelia needs variance in each column)
      usable_cols <- names(remaining_na_per_col[remaining_na_per_col < n_rows])
      em_input <- as.data.frame(imputed_df[, usable_cols, drop = FALSE])

      # empri: empirical prior (ridge regularization) — needed because
      # the number of indicators can exceed what 54 observations support.
      # Setting empri = 0.5 * nrow adds moderate regularization.
      empri_val <- max(1, round(0.5 * nrow(em_input)))

      amelia_result <- tryCatch({
        Amelia::amelia(em_input, m = 1, p2s = 0, empri = empri_val)
      }, error = function(e) {
        warning(paste("Amelia EM failed:", e$message, "- falling back to group median"))
        NULL
      })

      # Check Amelia return code: $code == 1 means success
      if (!is.null(amelia_result) &&
          !is.null(amelia_result$code) &&
          amelia_result$code == 1) {
        em_output <- as_tibble(amelia_result$imputations[[1]])
        n_em <- 0

        for (ind in em_eligible) {
          was_na <- is.na(imputed_df[[ind]])
          now_filled <- !is.na(em_output[[ind]])
          em_filled <- was_na & now_filled

          if (any(em_filled)) {
            imputed_df[[ind]][em_filled] <- em_output[[ind]][em_filled]

            for (ii in which(em_filled)) {
              n_em <- n_em + 1
              log_rows[[length(log_rows) + 1]] <- tibble(
                uCode = uCodes[ii],
                indicator = ind,
                method = "EM",
                imputed_value = em_output[[ind]][ii]
              )
            }
          }
        }

        message(paste("  Imputed", n_em, "values via EM algorithm"))
        em_success <- TRUE
      } else {
        if (!is.null(amelia_result) && !is.null(amelia_result$code)) {
          message(paste("  Amelia returned error code", amelia_result$code,
                        "- falling back to group median"))
        }
      }
    } else {
      message("  Amelia package not installed — falling back to group median")
    }

    # Fallback: year-group median for anything EM could not fill
    if (!em_success) {
      fb <- impute_year_median(imputed_df, em_eligible, uCodes, time_ids)
      imputed_df <- fb$data
      log_rows <- c(log_rows, fb$log_rows)
    }
  } else {
    message("Pass 2: No remaining NAs (in partially-observed indicators) — skipping")
  }

  # =========================================================================
  # Store results
  # =========================================================================
  coin$Data$Imputed <- imputed_df

  # Build and store imputation log
  if (length(log_rows) > 0) {
    imputation_log <- bind_rows(log_rows)
  } else {
    imputation_log <- tibble(
      uCode = character(), indicator = character(),
      method = character(), imputed_value = numeric()
    )
  }
  coin$Analysis$Imputation <- imputation_log

  # Final summary
  total_cells <- length(ind_cols) * nrow(raw_df)
  total_imputed <- nrow(imputation_log)
  pct_imputed <- round(total_imputed / total_cells * 100, 1)
  still_na <- sum(is.na(as.matrix(imputed_df[, ind_cols])))

  message(paste("\n✓ Imputation complete"))
  message(paste("  Total indicator cells:", total_cells))
  message(paste("  Imputed:", total_imputed, "(", pct_imputed, "%)"))
  message(paste("  Remaining NAs:", still_na,
                "(all-NA indicators, e.g. infrastructure placeholders)"))
  message("=======================================================\n")

  return(coin)
}


#' Year-Group Median Imputation (Fallback)
#'
#' For each indicator with remaining NAs, replaces missing values with
#' the median of other countries in the same year.
#'
#' @param imputed_df Data frame being imputed
#' @param ind_cols Indicator columns to impute
#' @param uCodes Character vector of unit codes
#' @param time_ids Integer vector of years per row
#' @return List with $data (imputed data frame) and $log_rows (list of tibbles)
#' @keywords internal
impute_year_median <- function(imputed_df, ind_cols, uCodes, time_ids) {

  log_rows <- list()
  n_med <- 0

  for (ind in ind_cols) {
    na_idx <- which(is.na(imputed_df[[ind]]))

    for (ii in na_idx) {
      yr <- time_ids[ii]
      same_year <- which(time_ids == yr)
      year_vals <- imputed_df[[ind]][same_year]
      med_val <- median(year_vals, na.rm = TRUE)

      if (!is.na(med_val)) {
        imputed_df[[ind]][ii] <- med_val
        n_med <- n_med + 1
        log_rows[[length(log_rows) + 1]] <- tibble(
          uCode = uCodes[ii],
          indicator = ind,
          method = "year_median",
          imputed_value = med_val
        )
      }
    }
  }

  message(paste("  Imputed", n_med, "values via year-group median (fallback)"))
  list(data = imputed_df, log_rows = log_rows)
}


#' Get Imputation Summary
#'
#' Diagnostic function to inspect which values were imputed and by what method.
#'
#' @param coin COINr coin object (after impute_gcceii)
#' @return Tibble summarising imputation by indicator and method
#' @export
get_imputation_summary <- function(coin) {

  if (is.null(coin$Analysis$Imputation) || nrow(coin$Analysis$Imputation) == 0) {
    message("No imputation log found. Run impute_gcceii() first.")
    return(tibble())
  }

  log <- coin$Analysis$Imputation

  # Per-indicator summary
  summary <- log %>%
    mutate(
      Country = str_remove(uCode, "_\\d{4}$"),
      Year = as.integer(str_extract(uCode, "\\d{4}$"))
    ) %>%
    group_by(indicator, method) %>%
    summarize(
      n_imputed = n(),
      countries = paste(sort(unique(Country)), collapse = ", "),
      years = paste(sort(unique(Year)), collapse = ", "),
      .groups = "drop"
    ) %>%
    arrange(indicator, method)

  return(summary)
}

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  GCCEII HELPER FUNCTIONS MODULE LOADED
=======================================================

Statistical functions:
  - calculate_cv()           : Coefficient of variation
  - calculate_yoy_growth()   : Year-over-year growth
  - calculate_hhi()          : Herfindahl-Hirschman Index
  - calculate_zscore()       : Z-score standardization

Normalization (reference - use COINr instead):
  - normalize_minmax()       : Min-max to 0-100
  - normalize_zscore()       : Z-score to 0-100
  - cv_to_convergence_score(): CV to convergence score

Data helpers:
  - safe_year_filter()       : Safe year filtering with fallback
  - get_indicator_value()    : Extract single indicator value
  - extract_indicator_wide() : Long to wide format

Imputation:
  - impute_gcceii()          : Two-pass imputation (linear + EM)
  - get_imputation_summary() : Inspect imputed cells by method

Export:
  - export_methodology_xlsx(): Full methodology workbook

Validation:
  - check_gcc_complete()     : Check all 6 countries present
  - get_missing_countries()  : Get list of missing countries
  - validate_range()         : Check value ranges
  - summarize_data_quality() : Data quality summary

Aggregation:
  - gdp_weighted_mean()      : GDP-weighted average
  - population_weighted_mean(): Population-weighted average

=======================================================
")
