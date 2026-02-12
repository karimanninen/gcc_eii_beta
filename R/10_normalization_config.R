# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - NORMALIZATION CONFIGURATION
# =============================================================================
#
# Version: 2.0 (with Winsorization)
# Date: December 2024
# 
# Purpose:
#   Define indicator-specific normalization strategy to handle:
#   - Extreme outliers (e.g., ind_3_aviation with 2047x range)
#   - Size-sensitive indicators (e.g., FDI/GDP varies with country size)
#   - Zero-crossing distributions (e.g., GDP growth, inflation)
#   - Skewed distributions
#
# =============================================================================
# METHODOLOGY DECISIONS
# =============================================================================
#
# Based on distribution analysis of 54 country-year observations:
#
# 1. WINSORIZATION (5th-95th percentile)
#    - Applied FIRST to cap extreme values before normalization
#    - Preserves scale and ordering while reducing outlier influence
#    - Used for indicators with range ratio > 15x
#    - Better than rank normalization: maintains relative distances
#
# 2. Z-SCORE NORMALIZATION
#    - Applied to indicators that cross zero (can't use min-max)
#    - Measures deviation from GCC mean (convergence perspective)
#    - Rescaled to 0-100 after calculation for comparability
#    - Used for OCA criteria: inflation, GDP growth, M2 growth
#
# 3. MIN-MAX NORMALIZATION (0-100)
#    - Default for well-behaved distributions (CV < 50%)
#    - Pooled across all years for time-series comparability
#    - Applied to bounded ratios and percentages
#
# Distribution Analysis Results (see analyze_distributions()):
# ┌─────────────────────┬────────┬─────────────┬──────────────────────┐
# │ Indicator           │ CV (%) │ Range Ratio │ Treatment            │
# ├─────────────────────┼────────┼─────────────┼──────────────────────┤
# │ ind_3_aviation      │ 94.6   │ 2047x       │ Winsorize + Min-max  │
# │ ind_unemployment    │ 84.6   │ 62x         │ Winsorize + Min-max  │
# │ ind_69_labor        │ 97.8   │ 47x         │ Winsorize + Min-max  │
# │ ind_39_banking      │ 96.7   │ 34x         │ Winsorize + Min-max  │
# │ ind_71_student      │ 80.4   │ 33x         │ Winsorize + Min-max  │
# │ ind_52              │ 93.0   │ 31x         │ Winsorize + Min-max  │
# │ ind_55              │ 87.9   │ 32x         │ Winsorize + Min-max  │
# │ ind_51              │ 83.8   │ 25x         │ Winsorize + Min-max  │
# │ ind_31_fdi          │ 86.2   │ 19x         │ Winsorize + Min-max  │
# │ ind_gdp_growth      │ 206    │ crosses 0   │ Z-score              │
# │ ind_inflation       │ 136    │ crosses 0   │ Z-score              │
# │ ind_m2_growth       │ 77.7   │ crosses 0   │ Z-score              │
# │ Others              │ <50    │ <10x        │ Min-max              │
# └─────────────────────┴────────┴─────────────┴──────────────────────┘
#
# =============================================================================

library(tidyverse)
library(COINr)

# =============================================================================
# NORMALIZATION CONFIGURATION
# =============================================================================

NORM_CONFIG <- list(
  
  # =========================================================================
  # WINSORIZE FIRST (cap extreme values at 5th/95th percentile)
  # =========================================================================
  # Rationale: These indicators have range ratios > 15x, meaning the maximum
  # is more than 15 times the minimum. Winsorization caps extreme values
  # while preserving the relative ordering and scale.
  #
  # Alternative considered: Rank normalization
  # Why rejected: Rank loses magnitude information; a country improving from
  # 10 to 50 would show same change as 50 to 51 if ranks don't change.
  # -------------------------------------------------------------------------
  
  winsorize = c(
    # Extreme outliers (range > 50x)
    "ind_3_aviation",      # Range 2047x - Bahrain tourism vs others
    "ind_unemployment",    # Range 62x - Saudi high vs Qatar low
    
    # Size-sensitive mobility (range > 30x)
    "ind_69_labor",        # Range 47x - Bahrain high mobility per capita
    "ind_71_student",      # Range 33x - Small countries higher per capita
    
    # Size-sensitive financial (range > 15x)
    "ind_39_banking",      # Range 34x - Bahrain financial hub effect
    "ind_31_fdi",          # Range 19x - Bahrain high due to Saudi inflows
    
    # Trade intensity (range > 20x)
    # Small countries naturally trade more relative to GDP
    "ind_51",              # Range 25x - Intra-GCC trade intensity
    "ind_52",              # Range 31x - Services trade share
    "ind_55"               # Range 32x - Non-oil trade intensity
  ),
  
  # Winsorization percentile thresholds
  winsor_lower = 0.05,     # Floor at 5th percentile
  winsor_upper = 0.95,     # Cap at 95th percentile
  
  # =========================================================================
  # Z-SCORE NORMALIZATION (zero-crossing, OCA criteria)
  # =========================================================================
  # Rationale: These indicators can be negative, making min-max problematic.
  # Z-score measures deviation from GCC mean, which is conceptually
  # appropriate for Optimum Currency Area (OCA) convergence criteria.
  #
  # For integration, countries closer to GCC average = more synchronized.
  # After z-score, we rescale to 0-100 for comparability.
  # -------------------------------------------------------------------------
  
  zscore = c(
    "ind_gdp_growth",      # CV 206%, range -8.7 to +12% - OCA criterion
    "ind_inflation",       # CV 136%, range -2.6 to +5% - OCA criterion
    "ind_m2_growth",       # CV 78%, range -3.2 to +15% - monetary sync
    "ind_fiscal_balance"   # Crosses zero (surplus/deficit as % of revenues)
  ),
  
  # =========================================================================
  # STANDARD MIN-MAX (well-behaved distributions)
  # =========================================================================
  # Rationale: These indicators have CV < 50% and range ratio < 10x,
  # indicating relatively well-behaved distributions suitable for
  # simple min-max normalization.
  #
  # Pooled normalization: Using min/max across all years (2015-2023)
  # ensures scores are comparable over time.
  # -------------------------------------------------------------------------
  
  # =========================================================================
  # GOALPOST NORMALIZATION (fixed reference points, no data-driven rescaling)
  # =========================================================================
  # Rationale: These indicators are already on a meaningful 0-100 scale
  # (e.g., percentages). Data-driven min-max would stretch a narrow range
  # to 0-100, exaggerating small differences. Instead, we use fixed
  # goalposts [0, 100] so the raw percentage IS the score.
  # -------------------------------------------------------------------------

  goalpost = c(
    "ind_44_stock"         # Stock market openness (composite % already 0-100)
  ),

  # =========================================================================
  # BOUNDED MIN-MAX (data-driven min/max, but compressed output range)
  # =========================================================================
  # Rationale: Some indicators are structurally constrained - the metric
  # can never reach high values due to external limits (e.g., only ~29 GCC
  # banks exist, so banks-per-million will always be low for large countries).
  # A floor above 0 avoids over-penalizing countries for structural factors
  # outside their control, while still preserving relative ordering.
  # -------------------------------------------------------------------------

  bounded_minmax = list(
    ind_39_banking = c(50, 100)  # Floor at 50: few distinct GCC banks limits per-capita metric
  ),

  minmax = c(
    # Trade (well-behaved, CV < 45%)
    "ind_56",              # CV 27% - Services % of total trade
    "ind_63",              # CV 41% - Intermediate goods share
    "ind_64",              # CV 13% - Trade diversification

    # Financial (bounded ratios)
    "ind_bank_depth",      # CV 37% - Banking assets % GDP
    
    # Labor
    "ind_72_tourism",      # CV 42% - Intra-GCC tourism share
    "ind_lfpr",            # CV 16% - Labor force participation
    
    # Infrastructure
    "ind_elec_pc",         # CV 26% - Electricity per capita
    
    # Sustainability
    "ind_non_oil_share",   # CV 15% - Non-oil GDP share
    "ind_oil_share",       # CV 38% - Oil dependency
    "ind_manufacturing_share", # CV 36% - Manufacturing share
    "ind_nonoil_rev_share",   # Non-oil revenue share (%)

    # Convergence (already 0-100 distance scores)
    "ind_conv_non_oil",       # CV 8% - Already normalized
    "ind_conv_manufacturing", # CV 26% - Already normalized
    "ind_conv_oil",           # CV 25% - Already normalized
    "ind_conv_income",        # CV 30% - Already normalized (ICP years only)
    "ind_conv_price",         # CV 4% - Already normalized (ICP years only)
    "ind_conv_fiscal",        # Fiscal balance convergence
    "ind_conv_interest"       # Interest rate convergence (policy rates)
  )
)

# =============================================================================
# STEP 1: WINSORIZATION FUNCTION (Manual Percentile-based)
# =============================================================================

#' Apply Percentile-based Winsorization
#'
#' Caps values at specified percentiles to reduce outlier influence.
#' Creates a Treated dataset while preserving original Raw data.
#'
#' @param coin COINr coin object with Raw data
#' @param config Normalization configuration (default: NORM_CONFIG)
#' @return Coin object with Treated dataset (Raw preserved for comparison)
#' @export
apply_winsorization <- function(coin, config = NORM_CONFIG) {

  message("Applying Winsorization (5th-95th percentile)...")

  # Check if any indicators need winsorization
  if (length(config$winsorize) == 0) {
    message("  No indicators to winsorize")
    return(coin)
  }

  # Source from Imputed dataset if available, else Raw
  if ("Imputed" %in% names(coin$Data)) {
    source_dset <- "Imputed"
    message("  Using Imputed data as source")
  } else {
    source_dset <- "Raw"
  }
  treated_data <- coin$Data[[source_dset]]
  
  # Track changes
  n_capped <- 0
  
  for (ind in config$winsorize) {
    if (ind %in% names(treated_data)) {
      vals <- treated_data[[ind]]
      
      if (!all(is.na(vals))) {
        # Calculate percentile thresholds
        lower_bound <- quantile(vals, config$winsor_lower, na.rm = TRUE)
        upper_bound <- quantile(vals, config$winsor_upper, na.rm = TRUE)
        
        # Count values to be capped
        n_lower <- sum(vals < lower_bound, na.rm = TRUE)
        n_upper <- sum(vals > upper_bound, na.rm = TRUE)
        
        # Apply winsorization
        vals_winsorized <- pmax(pmin(vals, upper_bound), lower_bound)
        
        # Update treated data
        treated_data[[ind]] <- vals_winsorized
        
        if (n_lower + n_upper > 0) {
          message(paste("  ", ind, "-> capped", n_lower, "low,", n_upper, "high values",
                        "(range:", round(lower_bound, 2), "-", round(upper_bound, 2), ")"))
          n_capped <- n_capped + n_lower + n_upper
        } else {
          message(paste("  ", ind, "-> no values outside 5-95% range"))
        }
      }
    }
  }
  
  # Store treated data (Raw is preserved for comparison)
  coin$Data$Treated <- treated_data
  
  message(paste("✓ Winsorized", length(config$winsorize), "indicators,", 
                n_capped, "values capped"))
  
  return(coin)
}

# =============================================================================
# GOALPOST NORMALIZATION FUNCTION
# =============================================================================

#' Goalpost Normalization (fixed reference points)
#'
#' Normalizes using fixed goalposts rather than data-driven min/max.
#' Ideal for indicators already on a meaningful scale (e.g., percentages
#' where 0 = none and 100 = full). Unlike min-max, this does not stretch
#' a narrow data range to 0-100, preserving the original scale's meaning.
#'
#' @param x Numeric vector of raw values
#' @param gposts Numeric vector of length 2: c(lower_goalpost, upper_goalpost).
#'   Default c(0, 100) for percentage indicators.
#' @param l_u Numeric vector of length 2: c(lower_output, upper_output).
#'   Default c(0, 100).
#' @return Normalized numeric vector, capped at l_u bounds
#' @export
n_goalpost <- function(x, gposts = c(0, 100), l_u = c(0, 100)) {
  result <- (x - gposts[1]) / (gposts[2] - gposts[1]) * (l_u[2] - l_u[1]) + l_u[1]
  result <- pmax(pmin(result, l_u[2]), l_u[1])
  return(result)
}

# =============================================================================
# STEP 2: NORMALIZATION FUNCTION
# =============================================================================

#' Apply Custom Normalization Strategy
#'
#' Applies different normalization methods based on indicator properties:
#' - Z-score for zero-crossing indicators (OCA criteria)
#' - Min-max (0-100) for all others
#'
#' @param coin COINr coin object (with Treated or Raw data)
#' @param config Normalization configuration (default: NORM_CONFIG)
#' @return Coin object with Normalised dataset
#' @export
apply_custom_normalisation <- function(coin, config = NORM_CONFIG) {
  
  message("Applying normalization...")
  
  # Determine source dataset (Treated if exists, else Raw)
  if ("Treated" %in% names(coin$Data)) {
    source_dset <- "Treated"
    message("  Using Treated (winsorized) data as source")
  } else {
    source_dset <- "Raw"
    message("  Using Raw data as source")
  }
  
  # Build individual specifications
  indiv_specs <- list()

  # Z-score for zero-crossing indicators
  for (ind in config$zscore) {
    if (ind %in% names(coin$Data[[source_dset]])) {
      indiv_specs[[ind]] <- list(f_n = "n_zscore")
      message(paste("  ", ind, "-> z-score (OCA criterion)"))
    }
  }

  # Goalpost normalization for indicators already on a meaningful 0-100 scale
  for (ind in config$goalpost) {
    if (ind %in% names(coin$Data[[source_dset]])) {
      indiv_specs[[ind]] <- list(
        f_n = "n_goalpost",
        f_n_para = list(gposts = c(0, 100), l_u = c(0, 100))
      )
      message(paste("  ", ind, "-> goalpost [0,100] (meaningful % scale)"))
    }
  }

  # Bounded min-max for structurally constrained indicators
  for (ind in names(config$bounded_minmax)) {
    if (ind %in% names(coin$Data[[source_dset]])) {
      bounds <- config$bounded_minmax[[ind]]
      indiv_specs[[ind]] <- list(
        f_n = "n_minmax",
        f_n_para = list(l_u = bounds)
      )
      message(paste("  ", ind, "-> min-max [", bounds[1], ",", bounds[2], "] (bounded)"))
    }
  }

  # Apply normalization
  # Global default: min-max 0-100
  # Individual: z-score, goalpost, or bounded min-max for specified indicators
  coin <- Normalise(
    coin,
    dset = source_dset,
    global_specs = list(
      f_n = "n_minmax",
      f_n_para = list(l_u = c(0, 100))
    ),
    indiv_specs = indiv_specs
  )
  
  message("✓ Normalization complete")
  
  return(coin)
}

# =============================================================================
# STEP 3: RESCALE Z-SCORES TO 0-100
# =============================================================================

#' Rescale Z-scores to 0-100 Range
#'
#' Z-scores are unbounded (-inf to +inf). This function rescales them
#' to 0-100 using the observed min/max for comparability with other indicators.
#'
#' Note: After rescaling, the interpretation changes from "standard deviations
#' from mean" to "relative position within observed range". This is acceptable
#' for aggregation purposes.
#'
#' @param coin Coin object with Normalised data
#' @param config Normalization config (default: NORM_CONFIG)
#' @return Coin object with rescaled z-scores
#' @export
rescale_zscores <- function(coin, config = NORM_CONFIG) {
  
  if (length(config$zscore) == 0) {
    return(coin)
  }
  
  message("Rescaling z-scores to 0-100...")
  
  # Get normalised data
  norm_data <- coin$Data$Normalised
  
  for (ind in config$zscore) {
    if (ind %in% names(norm_data)) {
      vals <- norm_data[[ind]]
      if (!all(is.na(vals))) {
        min_val <- min(vals, na.rm = TRUE)
        max_val <- max(vals, na.rm = TRUE)
        if (max_val > min_val) {
          norm_data[[ind]] <- (vals - min_val) / (max_val - min_val) * 100
          message(paste("  ", ind, "-> rescaled to 0-100"))
        }
      }
    }
  }
  
  coin$Data$Normalised <- norm_data
  message("✓ Z-score rescaling complete")
  
  return(coin)
}

# =============================================================================
# FULL PIPELINE FUNCTION
# =============================================================================

#' Run Full Custom Normalization Pipeline
#'
#' Applies the complete normalization strategy:
#' 1. Winsorization (5th-95th percentile) for extreme outliers
#' 2. Z-score normalization for zero-crossing indicators
#' 3. Min-max normalization (0-100) for standard indicators
#' 4. Rescale z-scores to 0-100 for comparability
#'
#' @param coin Coin object with Raw data
#' @param config Normalization configuration (default: NORM_CONFIG)
#' @return Coin object with Normalised data
#' @export
run_normalization_pipeline <- function(coin, config = NORM_CONFIG) {
  
  message("\n=======================================================")
  message("  CUSTOM NORMALIZATION PIPELINE")
  message("=======================================================\n")
  
  # -------------------------------------------------------------------------
  # Step 1: Winsorize extreme values
  # -------------------------------------------------------------------------
  coin <- apply_winsorization(coin, config)
  
  # -------------------------------------------------------------------------
  # Step 2: Apply normalization (z-score or min-max)
  # -------------------------------------------------------------------------
  message("")
  coin <- apply_custom_normalisation(coin, config)
  
  # -------------------------------------------------------------------------
  # Step 3: Rescale z-scores to 0-100
  # -------------------------------------------------------------------------
  message("")
  coin <- rescale_zscores(coin, config)
  
  # -------------------------------------------------------------------------
  # Summary
  # -------------------------------------------------------------------------
  message("\n=======================================================")
  message("  NORMALIZATION SUMMARY")
  message("=======================================================")
  message(paste("  Winsorized (5-95%): ", length(config$winsorize), "indicators"))
  message(paste("  Z-score (rescaled): ", length(config$zscore), "indicators"))
  message(paste("  Goalpost (fixed):   ", length(config$goalpost), "indicators"))
  message(paste("  Bounded min-max:    ", length(config$bounded_minmax), "indicators"))
  message(paste("  Min-max (0-100):    ", length(config$minmax), "indicators"))
  message("=======================================================\n")
  
  return(coin)
}

# =============================================================================
# DIAGNOSTIC FUNCTIONS
# =============================================================================

#' Analyze Indicator Distributions
#'
#' Calculates key statistics to identify problematic distributions.
#' Use this to verify normalization choices or identify new issues.
#'
#' @param coin Coin object with Raw data
#' @param config Normalization configuration (default: NORM_CONFIG)
#' @return Tibble with distribution statistics and assigned method
#' @export
analyze_distributions <- function(coin, config = NORM_CONFIG) {
  
  raw_data <- coin$Data$Raw
  
  stats <- raw_data %>%
    select(-uCode) %>%
    pivot_longer(everything(), names_to = "indicator", values_to = "value") %>%
    filter(!is.na(value)) %>%
    group_by(indicator) %>%
    summarize(
      n = n(),
      min = min(value),
      max = max(value),
      mean = mean(value),
      median = median(value),
      sd = sd(value),
      cv = sd / abs(mean) * 100,
      range_ratio = if_else(min > 0 & min != max, max / min, NA_real_),
      skew_proxy = if_else(sd > 0, (mean - median) / sd, NA_real_),
      .groups = "drop"
    ) %>%
    mutate(
      across(where(is.numeric), ~round(.x, 2)),
      # Assign normalization method
      winsorized = indicator %in% config$winsorize,
      norm_method = case_when(
        indicator %in% config$zscore ~ "z-score",
        indicator %in% config$goalpost ~ "goalpost",
        indicator %in% names(config$bounded_minmax) ~ "bounded-minmax",
        TRUE ~ "min-max"
      ),
      # Flag potential issues
      flag = case_when(
        !winsorized & range_ratio > 15 ~ "⚠️ Consider winsorize",
        cv > 100 & !indicator %in% config$zscore ~ "⚠️ High CV",
        TRUE ~ "✓"
      )
    ) %>%
    arrange(desc(cv))
  
  return(stats)
}

#' Compare Before/After Normalization
#'
#' Shows raw vs normalized values for a specific indicator.
#' Useful for verifying normalization effects.
#'
#' @param coin Coin object with Raw and Normalised data
#' @param indicator Indicator code to compare
#' @param year_filter Optional year to filter (default: all years)
#' @return Comparison tibble
#' @export
compare_normalization <- function(coin, indicator, year_filter = NULL) {
  
  # Get raw data
  raw <- coin$Data$Raw %>%
    select(uCode, raw = all_of(indicator))
  
  # Get treated data if exists
  if ("Treated" %in% names(coin$Data) && indicator %in% names(coin$Data$Treated)) {
    treated <- coin$Data$Treated %>%
      select(uCode, treated = all_of(indicator))
  } else {
    treated <- tibble(uCode = raw$uCode, treated = NA_real_)
  }
  
  # Get normalized data
  norm <- coin$Data$Normalised %>%
    select(uCode, normalized = all_of(indicator))
  
  # Combine
  comparison <- raw %>%
    left_join(treated, by = "uCode") %>%
    left_join(norm, by = "uCode") %>%
    mutate(
      Country = str_remove(uCode, "_\\d{4}$"),
      Year = as.integer(str_extract(uCode, "\\d{4}$"))
    ) %>%
    select(Country, Year, raw, treated, normalized)
  
  # Filter by year if specified
  if (!is.null(year_filter)) {
    comparison <- comparison %>% filter(Year == year_filter)
  }
  
  comparison %>% arrange(Year, desc(normalized))
}

#' Show Winsorization Impact
#'
#' Displays which values were capped by winsorization.
#' Compares Raw (original) vs Treated (winsorized) data.
#'
#' @param coin Coin object with Raw and Treated data
#' @param indicator Indicator to check
#' @return Tibble showing capped values
#' @export
show_winsorization_impact <- function(coin, indicator) {
  
  if (!"Treated" %in% names(coin$Data)) {
    message("No Treated dataset found - run apply_winsorization() first")
    return(NULL)
  }
  
  if (!indicator %in% names(coin$Data$Raw)) {
    message(paste("Indicator", indicator, "not found in data"))
    return(NULL)
  }
  
  raw <- coin$Data$Raw[[indicator]]
  treated <- coin$Data$Treated[[indicator]]
  
  # Find values that changed
  changed <- !is.na(raw) & !is.na(treated) & (abs(raw - treated) > 1e-10)
  
  if (!any(changed)) {
    message(paste("No values were capped for", indicator))
    return(tibble())
  }
  
  impact <- tibble(
    uCode = coin$Data$Raw$uCode,
    raw = raw,
    treated = treated
  ) %>%
    filter(abs(raw - treated) > 1e-10) %>%
    mutate(
      Country = str_remove(uCode, "_\\d{4}$"),
      Year = as.integer(str_extract(uCode, "\\d{4}$")),
      change = treated - raw,
      pct_change = round((treated - raw) / raw * 100, 1),
      direction = if_else(change > 0, "floored", "capped")
    ) %>%
    select(Country, Year, raw, treated, change, pct_change, direction) %>%
    arrange(desc(abs(pct_change)))
  
  message(paste("✓", nrow(impact), "values capped for", indicator))
  
  return(impact)
}

# =============================================================================
# DOCUMENTATION FUNCTIONS
# =============================================================================

#' Print Normalization Strategy Summary
#'
#' Displays a formatted summary of the normalization choices.
#'
#' @param config Normalization configuration (default: NORM_CONFIG)
#' @export
print_normalization_summary <- function(config = NORM_CONFIG) {
  
  cat("\n")
  cat("=======================================================\n")
  cat("  GCC EII NORMALIZATION STRATEGY\n")
  cat("=======================================================\n\n")
  
  cat("STEP 1: WINSORIZATION (5th-95th percentile)\n")
  cat("Purpose: Cap extreme values while preserving scale\n")
  cat("Indicators:", length(config$winsorize), "\n")
  for (ind in config$winsorize) {
    cat(paste("  -", ind, "\n"))
  }
  
  cat("\nSTEP 2: Z-SCORE NORMALIZATION\n")
  cat("Purpose: Handle zero-crossing indicators (OCA criteria)\n")
  cat("Indicators:", length(config$zscore), "\n")
  for (ind in config$zscore) {
    cat(paste("  -", ind, "\n"))
  }
  
  cat("\nGOALPOST NORMALIZATION (fixed 0-100 reference)\n")
  cat("Purpose: Preserve raw percentage scale for bounded indicators\n")
  cat("Indicators:", length(config$goalpost), "\n")
  for (ind in config$goalpost) {
    cat(paste("  -", ind, "\n"))
  }

  cat("\nBOUNDED MIN-MAX (compressed output range)\n")
  cat("Purpose: Floor above 0 for structurally constrained indicators\n")
  cat("Indicators:", length(config$bounded_minmax), "\n")
  for (ind in names(config$bounded_minmax)) {
    bounds <- config$bounded_minmax[[ind]]
    cat(paste("  -", ind, "-> [", bounds[1], ",", bounds[2], "]\n"))
  }

  cat("\nSTEP 3: MIN-MAX NORMALIZATION (0-100)\n")
  cat("Purpose: Standard scaling for well-behaved distributions\n")
  cat("Indicators:", length(config$minmax), "\n")
  for (ind in config$minmax) {
    cat(paste("  -", ind, "\n"))
  }
  
  cat("\n=======================================================\n\n")
}

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  NORMALIZATION CONFIG MODULE LOADED (v2.0)
=======================================================

Pipeline function:
  - run_normalization_pipeline() : Full 3-step normalization

Individual steps:
  - apply_winsorization()        : Cap extreme values (5-95%)
  - apply_custom_normalisation() : Z-score or min-max
  - rescale_zscores()            : Rescale z-scores to 0-100

Diagnostics:
  - analyze_distributions()       : Check indicator distributions
  - compare_normalization()       : Before/after comparison
  - show_winsorization_impact()   : See capped values
  - print_normalization_summary() : Display strategy

Configuration:
  - NORM_CONFIG$winsorize      : 9 indicators  (range > 15x)
  - NORM_CONFIG$zscore         : 3 indicators  (zero-crossing)
  - NORM_CONFIG$goalpost       : 1 indicator   (fixed 0-100 scale)
  - NORM_CONFIG$bounded_minmax : 1 indicator   (compressed range)
  - NORM_CONFIG$minmax         : 15 indicators (standard)

Methodology:
  1. Winsorize at 5th/95th percentile (reduces outlier impact)
  2. Z-score for OCA criteria (deviation from GCC mean)
  3. Goalpost for meaningful percentages (fixed 0-100 bounds)
  4. Bounded min-max for structurally constrained indicators
  5. Min-max 0-100 for all others (pooled across years)
  6. Rescale z-scores to 0-100 (for aggregation)

=======================================================
")
