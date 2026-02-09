# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - COINr METADATA
# =============================================================================
#
# Purpose:
#   Build the iMeta (indicator metadata) structure required by COINr.
#   This defines the hierarchical structure, weights, and properties
#   of all indicators and aggregates.
#
# Structure:
#   Level 1: Individual Indicators (40+ in POC)
#   Level 2: Dimensions (6)
#   Level 3: Overall Index (1)
#
# =============================================================================

library(tidyverse)

# =============================================================================
# INDICATOR METADATA DEFINITION
# =============================================================================

#' Build iMeta for GCCEII
#'
#' Creates the complete indicator metadata table for COINr.
#' Based on the GCC Economic Integration Indicators Framework.
#'
#' @param version Framework version: "poc" (40 indicators) or "full" (90 indicators)
#' @return Tibble in COINr iMeta format
#' @export
build_iMeta <- function(version = "poc") {
  
  message(paste("Building iMeta for version:", version))
  
  # =========================================================================
  # LEVEL 1: INDIVIDUAL INDICATORS
  # =========================================================================
  
  indicators <- tribble(
    ~iCode, ~iName, ~Parent, ~Weight, ~Direction,
    
    # -----------------------------------------------------------------------
    # TRADE INTEGRATION DIMENSION
    # -----------------------------------------------------------------------
    "ind_51", "Intra-GCC Trade Intensity", "Trade", 1, 1,
    "ind_52", "Services Trade Share", "Trade", 1, 1,
    "ind_55", "Non-oil Trade Intensity", "Trade", 1, 1,
    "ind_56", "Services % of Total Trade", "Trade", 1, 1,
    "ind_63", "Intermediate Goods Share", "Trade", 1, 1,
    "ind_64", "Trade Diversification", "Trade", 1, 1,
    
    # -----------------------------------------------------------------------
    # FINANCIAL INTEGRATION DIMENSION
    # -----------------------------------------------------------------------
    "ind_inflation", "Inflation Rate", "Financial", 1, -1,
    "ind_m2_growth", "M2 Money Supply Growth", "Financial", 1, 1,
    "ind_gdp_growth", "Real GDP Growth", "Financial", 1, 1,
    "ind_39_banking", "GCC Banking Penetration", "Financial", 1, 1,
    "ind_44_stock", "Stock Market Openness", "Financial", 1, 1,
    "ind_31_fdi", "Intra-GCC FDI", "Financial", 1, 1,
    "ind_bank_depth", "Banking Depth", "Financial", 1, 1,
    
    # -----------------------------------------------------------------------
    # LABOR & MOBILITY DIMENSION
    # -----------------------------------------------------------------------
    "ind_69_labor", "GCC Labor Mobility", "Labor", 1, 1,
    "ind_71_student", "Student Mobility", "Labor", 1, 1,
    "ind_72_tourism", "Intra-GCC Tourism Share", "Labor", 1, 1,
    "ind_lfpr", "Labor Force Participation", "Labor", 1, 1,
    "ind_unemployment", "Unemployment Rate", "Labor", 1, -1,
    
    # -----------------------------------------------------------------------
    # INFRASTRUCTURE DIMENSION
    # -----------------------------------------------------------------------
    "ind_3_aviation", "Aviation Connectivity", "Infrastructure", 1, 1,
    "ind_elec_pc", "Electricity Per Capita", "Infrastructure", 1, 1,
    
    # -----------------------------------------------------------------------
    # SUSTAINABILITY DIMENSION
    # -----------------------------------------------------------------------
    "ind_non_oil_share", "Non-oil GDP Share", "Sustainability", 1, 1,
    "ind_oil_share", "Oil Dependency", "Sustainability", 1, -1,
    "ind_manufacturing_share", "Manufacturing Share", "Sustainability", 1, 1,
    
    # -----------------------------------------------------------------------
    # CONVERGENCE DIMENSION (Country-specific distance from GCC average)
    # -----------------------------------------------------------------------
    "ind_conv_non_oil", "Non-oil Convergence", "Convergence", 1, 1,
    "ind_conv_manufacturing", "Manufacturing Convergence", "Convergence", 1, 1,
    "ind_conv_oil", "Oil Dependency Convergence", "Convergence", 1, 1,
    "ind_conv_income", "Real Income Convergence", "Convergence", 1, 1,
    "ind_conv_price", "Price Level Convergence", "Convergence", 1, 1,
  )
  
  # Add Level and Type for indicators
  indicators <- indicators %>%
    mutate(
      Level = 1,
      Type = "Indicator"
    )
  
  # =========================================================================
  # LEVEL 2: DIMENSIONS
  # =========================================================================
  
  dimensions <- tribble(
    ~iCode, ~iName, ~Parent, ~Weight, ~Direction, ~Level, ~Type,
    "Trade", "Trade Integration", "Index", 0.20, 1, 2, "Aggregate",
    "Financial", "Financial Integration", "Index", 0.20, 1, 2, "Aggregate",
    "Labor", "Labor & Mobility", "Index", 0.20, 1, 2, "Aggregate",
    "Infrastructure", "Infrastructure", "Index", 0.20, 1, 2, "Aggregate",
    "Sustainability", "Sustainability", "Index", 0.10, 1, 2, "Aggregate",
    "Convergence", "Convergence", "Index", 0.10, 1, 2, "Aggregate"
  )
  
  
  
  # =========================================================================
  # LEVEL 3: OVERALL INDEX
  # =========================================================================
  
  index <- tribble(
    ~iCode, ~iName, ~Parent, ~Weight, ~Direction, ~Level, ~Type,
    "Index", "GCC Economic Integration Index", NA, 1, 1, 3, "Aggregate"
  )
  
  # =========================================================================
  # COMBINE ALL
  # =========================================================================
  
  iMeta <- bind_rows(indicators, dimensions, index) %>%
    select(iCode, iName, Level, Parent, Weight, Direction, Type)
  
  message(paste("  Created iMeta with", nrow(iMeta), "rows"))
  message(paste("  - Level 1 (Indicators):", sum(iMeta$Level == 1)))
  message(paste("  - Level 2 (Dimensions):", sum(iMeta$Level == 2)))
  message(paste("  - Level 3 (Index):", sum(iMeta$Level == 3)))
  
  return(iMeta)
}

# =============================================================================
# UNIT METADATA (Optional)
# =============================================================================

#' Build Unit Metadata
#'
#' Creates the uMeta (unit metadata) table for COINr.
#' For GCCEII, units are countries.
#'
#' @return Tibble in COINr uMeta format
#' @export
build_uMeta <- function() {
  
  tribble(
    ~uCode, ~uName, ~Region, ~GDP_Weight,
    "BHR", "Bahrain", "GCC", 0.02,
    "KWT", "Kuwait", "GCC", 0.08,
    "OMN", "Oman", "GCC", 0.05,
    "QAT", "Qatar", "GCC", 0.11,
    "SAU", "Saudi Arabia", "GCC", 0.55,
    "ARE", "UAE", "GCC", 0.19
  )
}

# =============================================================================
# INDICATOR CODE MAPPING
# =============================================================================

#' Map Raw Column Names to iMeta Codes
#'
#' Maps the column names from extract_raw_indicators() to the iCode
#' values used in iMeta. This ensures consistency between data and metadata.
#'
#' @return Named vector for column renaming
#' @export
get_indicator_code_mapping <- function() {
  c(
    # Trade
    "ind_51_trade_intensity" = "ind_51",
    "ind_52_services_share" = "ind_52",
    "ind_55_non_oil_trade" = "ind_55",
    "ind_56_services_pct" = "ind_56",
    "ind_63_intermediate_share" = "ind_63",
    "ind_64_diversification" = "ind_64",
    
    # Financial
    "ind_inflation" = "ind_inflation",
    "ind_m2_growth" = "ind_m2_growth",
    "ind_gdp_growth" = "ind_gdp_growth",
    "ind_39_banking" = "ind_39_banking",
    "ind_44_stock" = "ind_44_stock",
    "ind_31_fdi" = "ind_31_fdi",
    "ind_bank_depth" = "ind_bank_depth",
    
    # Labor
    "ind_69_labor" = "ind_69_labor",
    "ind_71_student" = "ind_71_student",
    "ind_72_tourism" = "ind_72_tourism",
    "ind_lfpr" = "ind_lfpr",
    "ind_unemployment" = "ind_unemployment",
    
    # Infrastructure
    "ind_3_aviation" = "ind_3_aviation",
    "ind_elec_pc" = "ind_elec_pc",
    
    # Sustainability
    "ind_non_oil_share" = "ind_non_oil_share",
    "ind_oil_share" = "ind_oil_share",
    "ind_manufacturing_share" = "ind_manufacturing_share",
    
    # Convergence
    "ind_ppp_gdp_pc" = "ind_ppp_gdp_pc",
    "ind_price_level" = "ind_price_level"
  )
}

# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

#' Validate iData Against iMeta
#'
#' Checks that iData contains all indicators specified in iMeta
#' and reports any missing or extra columns.
#'
#' @param iData Data tibble from extract_raw_indicators()
#' @param iMeta Metadata tibble from build_iMeta()
#' @return List with validation results
#' @export
validate_iData_iMeta <- function(iData, iMeta) {
  
  # Get indicator codes from iMeta
  expected_indicators <- iMeta %>%
    filter(Level == 1) %>%
    pull(iCode)
  
  # Get data columns (excluding uCode, uName, Year)
  data_columns <- setdiff(names(iData), c("uCode", "uName", "Year"))
  
  # Check for missing indicators
  missing <- setdiff(expected_indicators, data_columns)
  
  # Check for extra columns
  extra <- setdiff(data_columns, expected_indicators)
  
  # Summary
  result <- list(
    valid = length(missing) == 0,
    expected = length(expected_indicators),
    found = length(intersect(expected_indicators, data_columns)),
    missing = missing,
    extra = extra
  )
  
  if (!result$valid) {
    warning(paste("iData missing", length(missing), "indicators:",
                  paste(missing, collapse = ", ")))
  }
  
  if (length(extra) > 0) {
    message(paste("iData has", length(extra), "extra columns:",
                  paste(extra, collapse = ", ")))
  }
  
  return(result)
}

#' Check iMeta Structure
#'
#' Validates that iMeta has correct structure for COINr
#'
#' @param iMeta Metadata tibble
#' @return Logical TRUE if valid
#' @export
validate_iMeta <- function(iMeta) {
  
  required_cols <- c("iCode", "iName", "Level", "Parent", "Type")
  
  # Check required columns
  if (!all(required_cols %in% names(iMeta))) {
    missing <- setdiff(required_cols, names(iMeta))
    stop(paste("iMeta missing required columns:", paste(missing, collapse = ", ")))
  }
  
 # Check that all parents exist (except for top level)
  parents <- iMeta$Parent[!is.na(iMeta$Parent)]
  codes <- iMeta$iCode
  
  missing_parents <- setdiff(parents, codes)
  if (length(missing_parents) > 0) {
    stop(paste("iMeta has undefined parents:", paste(missing_parents, collapse = ", ")))
  }
  
  # Check that top level has no parent
  top_level <- max(iMeta$Level)
  top_items <- iMeta %>% filter(Level == top_level)
  if (any(!is.na(top_items$Parent))) {
    warning("Top-level items should have NA as Parent")
  }
  
  message("✓ iMeta structure is valid")
  return(TRUE)
}

# =============================================================================
# WEIGHT ADJUSTMENT FUNCTIONS
# =============================================================================

#' Adjust Dimension Weights
#'
#' Allows modifying dimension weights for sensitivity analysis
#'
#' @param iMeta Current iMeta tibble
#' @param new_weights Named vector with dimension weights (must sum to 1)
#' @return Updated iMeta
#' @export
set_dimension_weights <- function(iMeta, new_weights) {
  
  # Validate weights sum to 1
  if (abs(sum(new_weights) - 1) > 0.001) {
    stop("Dimension weights must sum to 1")
  }
  
  # Update weights
  for (dim_name in names(new_weights)) {
    iMeta <- iMeta %>%
      mutate(Weight = if_else(iCode == dim_name, new_weights[[dim_name]], Weight))
  }
  
  return(iMeta)
}

#' Get Current Dimension Weights
#'
#' @param iMeta Metadata tibble
#' @return Named vector of dimension weights
#' @export
get_dimension_weights <- function(iMeta) {
  
  weights <- iMeta %>%
    filter(Level == 2) %>%
    select(iCode, Weight)
  
  setNames(weights$Weight, weights$iCode)
}

# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  COINr METADATA MODULE LOADED
=======================================================

Main functions:
  - build_iMeta()            : Create indicator metadata
  - build_uMeta()            : Create unit (country) metadata
  - get_indicator_code_mapping() : Column name to iCode mapping

Validation:
  - validate_iData_iMeta()   : Check data against metadata
  - validate_iMeta()         : Check metadata structure

Weight adjustment:
  - set_dimension_weights()  : Modify dimension weights
  - get_dimension_weights()  : Get current weights

Current dimension weights:
  - Trade: 20%
  - Financial: 20%
  - Labor: 15%
  - Infrastructure: 20%
  - Sustainability: 10%
  - Convergence: 15%

=======================================================
")
