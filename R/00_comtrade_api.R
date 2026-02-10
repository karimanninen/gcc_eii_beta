# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - COMTRADE API DATA EXTRACTION
# =============================================================================
#
# Purpose:
#   Fetch bilateral intra-GCC trade data from the UN Comtrade API and save
#   as RDS files ready for the GCCEII pipeline.
#
#   This module is a DATA ACQUISITION tool, run manually before the main
#   build pipeline. It produces comtrade_data.rds and comtrade_data_hs.rds
#   in data-raw/, which 01_data_loading.R then reads.
#
# Usage:
#   source("R/00_comtrade_api.R")
#   fetch_gcc_comtrade(output_dir = "data-raw")
#
# Requirements:
#   - install.packages("comtradr")
#   - A UN Comtrade API key, set via:
#       Sys.setenv(COMTRADE_API_KEY = "your-key-here")
#     or passed as the api_key argument
#
# Output Contract (consumed by 03_indicators_trade.R):
#   $total_trade:     year, reporter_name, partner_name, flow_direction,
#                     trade_value_usd, trade_value_million_usd
#   $bec_trade:       same + bec_category, hs_chapter
#   $non_oil_trade:   same as total_trade (oil HS chapters excluded)
#   $intra_gcc_summary, $bec_summary, $non_oil_summary: pre-aggregated
#   $metadata:        provenance info
#
# =============================================================================

library(tidyverse)

# =============================================================================
# CONFIGURATION
# =============================================================================

#' GCC ISO3 codes and standard names
#' @keywords internal
COMTRADE_GCC <- list(
  iso3 = c("BHR", "KWT", "OMN", "QAT", "SAU", "ARE"),
  names = c(
    "BHR" = "Bahrain",
    "KWT" = "Kuwait",
    "OMN" = "Oman",
    "QAT" = "Qatar",
    "SAU" = "Saudi Arabia",
    "ARE" = "UAE"
  )
)

#' HS chapters classified as oil/mineral fuels (excluded from non-oil trade)
#' @keywords internal
OIL_HS_CHAPTERS <- c("27")

#' Key HS chapters to download for BEC classification
#' Covers all 97 HS chapters grouped by BEC category
#' @keywords internal
ALL_HS_CHAPTERS <- sprintf("%02d", 1:97)

# =============================================================================
# MAIN EXTRACTION FUNCTION
# =============================================================================

#' Fetch GCC Bilateral Trade Data from UN Comtrade API
#'
#' Downloads intra-GCC bilateral trade data and saves as RDS files
#' compatible with the GCCEII pipeline.
#'
#' @param start_year First year to download (default: 2015)
#' @param end_year Last year to download (default: 2024)
#' @param output_dir Directory to save RDS files (default: "data-raw")
#' @param api_key Comtrade API key. If NULL, reads COMTRADE_API_KEY env var.
#' @param detailed_hs Download individual HS chapters for actual BEC/non-oil
#'   classification (TRUE, slower) or use estimates (FALSE, fast).
#' @param rate_limit_sec Seconds to pause between API calls (default: 1.5)
#'
#' @return List with total_trade, bec_trade, non_oil_trade, summaries, metadata
#' @export
fetch_gcc_comtrade <- function(start_year = 2015,
                               end_year = 2024,
                               output_dir = "data-raw",
                               api_key = NULL,
                               detailed_hs = TRUE,
                               rate_limit_sec = 1.5) {

  if (!requireNamespace("comtradr", quietly = TRUE)) {
    stop("Package 'comtradr' is required. Install with: install.packages('comtradr')")
  }

  message("=======================================================")
  message("  GCC COMTRADE DATA EXTRACTION")
  message("=======================================================\n")

  # --- API key setup ---
  api_key <- api_key %||% Sys.getenv("COMTRADE_API_KEY", unset = "")
  if (api_key == "") {
    stop(
      "No Comtrade API key found.\n",
      "Set it with: Sys.setenv(COMTRADE_API_KEY = 'your-key')\n",
      "Or pass it as the api_key argument."
    )
  }
  comtradr::set_primary_comtrade_key(api_key)
  message("\u2713 API key set\n")

  gcc_iso3 <- COMTRADE_GCC$iso3
  gcc_names <- COMTRADE_GCC$names

  # =========================================================================
  # STEP 1: Total Trade (cmdCode = TOTAL)
  # =========================================================================

  message("STEP 1: Downloading total bilateral trade...\n")

  total_trade_raw <- fetch_by_reporter(
    gcc_iso3, gcc_names,
    commodity_code = "TOTAL",
    start_year = start_year,
    end_year = end_year,
    rate_limit_sec = rate_limit_sec
  )

  total_trade <- standardize_trade_df(total_trade_raw, gcc_names)
  message(paste("\u2713 Total trade:", nrow(total_trade), "records\n"))

  # =========================================================================
  # STEP 2: HS Chapter-level Data (for BEC mapping + non-oil)
  # =========================================================================

  bec_trade <- NULL
  non_oil_trade <- NULL
  bec_method <- "estimated"

  if (detailed_hs) {
    message("STEP 2: Downloading HS chapter-level data...\n")
    message("  (This takes several minutes due to API rate limits)\n")

    hs_trade_raw <- fetch_hs_chapters(
      gcc_iso3, gcc_names,
      hs_chapters = ALL_HS_CHAPTERS,
      start_year = start_year,
      end_year = end_year,
      rate_limit_sec = rate_limit_sec
    )

    if (!is.null(hs_trade_raw) && nrow(hs_trade_raw) > 0) {
      hs_trade <- standardize_trade_df(hs_trade_raw, gcc_names)

      # Identify HS chapter from commodity code
      hs_trade <- hs_trade %>%
        mutate(hs_chapter = str_sub(as.character(commodity_code), 1, 2))

      # --- BEC classification ---
      hs_bec_map <- create_hs_bec_mapping()
      bec_trade <- hs_trade %>%
        left_join(hs_bec_map, by = c("hs_chapter" = "hs2")) %>%
        filter(!is.na(bec_category))

      bec_method <- "actual"
      message(paste("\u2713 BEC trade:", nrow(bec_trade), "records\n"))

      # --- Non-oil trade (exclude HS 27) ---
      non_oil_trade <- hs_trade %>%
        filter(!hs_chapter %in% OIL_HS_CHAPTERS)

      message(paste("\u2713 Non-oil trade:", nrow(non_oil_trade), "records\n"))
    } else {
      message("\u26a0 No HS chapter data returned from API\n")
    }
  } else {
    message("STEP 2: Skipping detailed HS download (detailed_hs = FALSE)\n")
  }

  # --- Fallback: estimate BEC from total trade if no HS data ---
  if (is.null(bec_trade) || nrow(bec_trade) == 0) {
    message("  Creating BEC estimates from total trade (historical shares)...\n")
    bec_trade <- estimate_bec_from_total(total_trade)
    bec_method <- "estimated"
  }

  # --- Fallback: estimate non-oil from total trade if no HS data ---
  if (is.null(non_oil_trade) || nrow(non_oil_trade) == 0) {
    message("  Creating non-oil estimates from total trade (oil share estimates)...\n")
    non_oil_trade <- estimate_non_oil_from_total(total_trade)
  }

  # =========================================================================
  # STEP 3: Build summaries
  # =========================================================================

  message("STEP 3: Building summaries...\n")

  intra_gcc_summary <- total_trade %>%
    group_by(year, reporter_name, flow_direction) %>%
    summarize(
      total_trade = sum(trade_value_million_usd, na.rm = TRUE),
      n_partners = n_distinct(partner_name),
      .groups = "drop"
    )

  bec_summary <- bec_trade %>%
    group_by(year, reporter_name, flow_direction, bec_category) %>%
    summarize(
      total_trade = sum(trade_value_million_usd, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    pivot_wider(
      names_from = bec_category,
      values_from = total_trade,
      values_fill = 0
    )

  non_oil_summary <- non_oil_trade %>%
    group_by(year, reporter_name, flow_direction) %>%
    summarize(
      total_non_oil = sum(trade_value_million_usd, na.rm = TRUE),
      .groups = "drop"
    )

  # =========================================================================
  # STEP 4: Package results
  # =========================================================================

  result <- list(
    total_trade = total_trade,
    bec_trade = bec_trade,
    non_oil_trade = non_oil_trade,
    intra_gcc_summary = intra_gcc_summary,
    bec_summary = bec_summary,
    non_oil_summary = non_oil_summary,
    metadata = list(
      bec_method = bec_method,
      non_oil_method = if (detailed_hs && bec_method == "actual") "actual" else "estimated",
      extraction_time = Sys.time(),
      start_year = start_year,
      end_year = end_year,
      detailed_hs = detailed_hs
    )
  )

  # =========================================================================
  # STEP 5: Save RDS files
  # =========================================================================

  message("STEP 4: Saving RDS files...\n")

  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  # Save aggregate (same structure whether actual or estimated)
  saveRDS(result, file.path(output_dir, "comtrade_data.rds"))
  message(paste("  \u2713 Saved", file.path(output_dir, "comtrade_data.rds")))

  # Save HS-level (identical structure when detailed_hs = TRUE;
  # when FALSE the same estimated data is in both files for compatibility)
  saveRDS(result, file.path(output_dir, "comtrade_data_hs.rds"))
  message(paste("  \u2713 Saved", file.path(output_dir, "comtrade_data_hs.rds")))

  # =========================================================================
  # Summary
  # =========================================================================

  message("\n=======================================================")
  message("  EXTRACTION COMPLETE")
  message("=======================================================\n")
  message(paste("  Total trade records:  ", nrow(total_trade)))
  message(paste("  BEC trade records:    ", nrow(bec_trade)))
  message(paste("  Non-oil records:      ", nrow(non_oil_trade)))
  message(paste("  BEC method:           ", bec_method))
  message(paste("  Years:                ", start_year, "-", end_year))

  countries_found <- sort(unique(total_trade$reporter_name))
  message(paste("  Countries with data:  ", paste(countries_found, collapse = ", ")))

  years_found <- sort(unique(total_trade$year))
  message(paste("  Years with data:      ", paste(years_found, collapse = ", ")))
  message("")

  return(invisible(result))
}


# =============================================================================
# API CALL HELPERS
# =============================================================================

#' Download total or single-commodity trade for all GCC reporters
#'
#' @param gcc_iso3 Character vector of ISO3 codes
#' @param gcc_names Named vector mapping ISO3 -> country name
#' @param commodity_code Commodity code (e.g. "TOTAL" or "01")
#' @param start_year Start year
#' @param end_year End year
#' @param rate_limit_sec Pause between API calls
#' @return Raw data frame from comtradr (may have varying column names)
#' @keywords internal
fetch_by_reporter <- function(gcc_iso3, gcc_names,
                              commodity_code = "TOTAL",
                              start_year, end_year,
                              rate_limit_sec = 1.5) {

  results <- list()

  for (reporter in gcc_iso3) {
    message(paste("  Fetching:", gcc_names[reporter]))

    partners <- setdiff(gcc_iso3, reporter)

    tryCatch({
      trade <- comtradr::ct_get_data(
        type = "goods",
        frequency = "A",
        commodity_classification = "HS",
        commodity_code = commodity_code,
        flow_direction = c("import", "export"),
        reporter = reporter,
        partner = partners,
        start_date = as.character(start_year),
        end_date = as.character(end_year),
        process = TRUE,
        tidy_cols = TRUE,
        verbose = FALSE
      )

      if (!is.null(trade) && nrow(trade) > 0) {
        results[[reporter]] <- trade
      }

      Sys.sleep(rate_limit_sec)

    }, error = function(e) {
      message(paste("    \u26a0 Error for", gcc_names[reporter], ":", e$message))
    })
  }

  if (length(results) == 0) return(NULL)

  bind_rows(results)
}


#' Download HS chapter-level trade for all GCC reporters
#'
#' Iterates over reporters and HS chapters with rate limiting.
#'
#' @param gcc_iso3 Character vector of ISO3 codes
#' @param gcc_names Named vector mapping ISO3 -> country name
#' @param hs_chapters Character vector of 2-digit HS chapter codes
#' @param start_year Start year
#' @param end_year End year
#' @param rate_limit_sec Pause between API calls
#' @return Raw data frame from comtradr
#' @keywords internal
fetch_hs_chapters <- function(gcc_iso3, gcc_names,
                              hs_chapters = ALL_HS_CHAPTERS,
                              start_year, end_year,
                              rate_limit_sec = 1.5) {

  results <- list()
  total_calls <- length(gcc_iso3) * length(hs_chapters)
  call_count <- 0

  for (reporter in gcc_iso3) {
    message(paste("  Fetching HS chapters for:", gcc_names[reporter]))
    partners <- setdiff(gcc_iso3, reporter)

    for (hs_ch in hs_chapters) {
      call_count <- call_count + 1

      tryCatch({
        hs_data <- comtradr::ct_get_data(
          type = "goods",
          frequency = "A",
          commodity_classification = "HS",
          commodity_code = hs_ch,
          flow_direction = c("import", "export"),
          reporter = reporter,
          partner = partners,
          start_date = as.character(start_year),
          end_date = as.character(end_year),
          process = TRUE,
          tidy_cols = TRUE,
          verbose = FALSE
        )

        if (!is.null(hs_data) && nrow(hs_data) > 0) {
          results[[paste0(reporter, "_", hs_ch)]] <- hs_data
        }

        Sys.sleep(rate_limit_sec)

      }, error = function(e) {
        # Silently skip "no data" errors (common for many HS chapters)
        if (!grepl("no data", tolower(e$message))) {
          message(paste("    \u26a0 HS", hs_ch, ":", e$message))
        }
      })
    }

    message(paste("    Progress:", call_count, "/", total_calls))
  }

  if (length(results) == 0) return(NULL)

  bind_rows(results)
}


# =============================================================================
# COLUMN STANDARDIZATION
# =============================================================================

#' Standardize a raw comtradr data frame to the GCCEII column contract
#'
#' Handles the varying column names that different comtradr versions produce.
#'
#' @param raw_df Raw data frame from comtradr
#' @param gcc_names Named vector mapping ISO3 -> country name
#' @return Standardized tibble with the required columns
#' @keywords internal
standardize_trade_df <- function(raw_df, gcc_names) {

  if (is.null(raw_df) || nrow(raw_df) == 0) {
    return(tibble(
      year = integer(), reporter_name = character(),
      partner_name = character(), flow_direction = character(),
      trade_value_usd = numeric(), trade_value_million_usd = numeric()
    ))
  }

  df <- raw_df
  cols <- names(df)

  # --- year ---
  if ("period" %in% cols) {
    df <- df %>% mutate(year = as.integer(period))
  } else if ("ref_year" %in% cols) {
    df <- df %>% mutate(year = as.integer(ref_year))
  }

  # --- reporter_name / partner_name (use ISO mapping for consistency) ---
  if ("reporter_iso" %in% cols) {
    df <- df %>% mutate(reporter_name = gcc_names[reporter_iso])
  } else if ("reporter_desc" %in% cols) {
    df <- df %>% mutate(reporter_name = reporter_desc)
  }

  if ("partner_iso" %in% cols) {
    df <- df %>% mutate(partner_name = gcc_names[partner_iso])
  } else if ("partner_desc" %in% cols) {
    df <- df %>% mutate(partner_name = partner_desc)
  }

  # --- flow_direction ---
  flow_col <- intersect(
    c("flow_direction", "flow_desc", "flow_code", "flow", "trade_flow", "trade_flow_code"),
    cols
  )
  if (length(flow_col) > 0) {
    flow_col <- flow_col[1]
    df <- df %>%
      rename(flow_direction = !!sym(flow_col)) %>%
      mutate(
        flow_direction = case_when(
          tolower(flow_direction) %in% c("export", "exports", "x", "2") ~ "export",
          tolower(flow_direction) %in% c("import", "imports", "m", "1") ~ "import",
          tolower(flow_direction) %in% c("re-export", "re-exports", "3") ~ "re-export",
          tolower(flow_direction) %in% c("re-import", "re-imports", "4") ~ "re-import",
          TRUE ~ tolower(as.character(flow_direction))
        )
      )
  }

  # --- trade value ---
  value_col <- intersect(
    c("primary_value", "trade_value_usd", "trade_value", "value",
      "fobvalue", "cifvalue", "fob_value_usd", "cif_value_usd"),
    cols
  )
  if (length(value_col) > 0) {
    df <- df %>%
      rename(trade_value_usd = !!sym(value_col[1])) %>%
      mutate(trade_value_million_usd = trade_value_usd / 1e6)
  }

  # --- commodity_code (for HS-level data) ---
  cmd_col <- intersect(
    c("cmd_code", "commodity_code", "cmdcode", "commodity"),
    cols
  )
  if (length(cmd_col) > 0) {
    df <- df %>% rename(commodity_code = !!sym(cmd_col[1]))
  }

  # --- Select only the columns the pipeline needs ---
  keep <- intersect(
    c("year", "reporter_name", "partner_name", "flow_direction",
      "trade_value_usd", "trade_value_million_usd", "commodity_code"),
    names(df)
  )

  df <- df %>%
    select(all_of(keep)) %>%
    filter(!is.na(trade_value_usd))

  return(df)
}


# =============================================================================
# BEC AND NON-OIL ESTIMATION FALLBACKS
# =============================================================================

#' Estimate BEC breakdown from total trade using historical GCC shares
#'
#' Used when detailed HS data is unavailable.
#'
#' @param total_trade Standardized total trade tibble
#' @return Tibble with bec_category column added
#' @keywords internal
estimate_bec_from_total <- function(total_trade) {

  gcc_bec_shares <- tribble(
    ~reporter_name, ~primary_share, ~intermediate_share, ~final_share,
    "Bahrain",      0.25, 0.35, 0.40,
    "Kuwait",       0.45, 0.30, 0.25,
    "Oman",         0.50, 0.28, 0.22,
    "Qatar",        0.55, 0.25, 0.20,
    "Saudi Arabia", 0.48, 0.30, 0.22,
    "UAE",          0.30, 0.35, 0.35
  )

  total_trade %>%
    left_join(gcc_bec_shares, by = "reporter_name") %>%
    pivot_longer(
      cols = ends_with("_share"),
      names_to = "bec_category",
      values_to = "share"
    ) %>%
    mutate(
      bec_category = str_remove(bec_category, "_share"),
      trade_value_usd = trade_value_usd * share,
      trade_value_million_usd = trade_value_million_usd * share
    ) %>%
    select(-share)
}


#' Estimate non-oil trade by subtracting estimated oil shares
#'
#' Used when detailed HS data is unavailable.
#'
#' @param total_trade Standardized total trade tibble
#' @return Tibble with estimated non-oil values
#' @keywords internal
estimate_non_oil_from_total <- function(total_trade) {

  gcc_oil_shares <- tribble(
    ~reporter_name, ~oil_share_exports, ~oil_share_imports,
    "Bahrain",      0.45, 0.15,
    "Kuwait",       0.70, 0.12,
    "Oman",         0.65, 0.14,
    "Qatar",        0.68, 0.10,
    "Saudi Arabia", 0.72, 0.13,
    "UAE",          0.42, 0.11
  )

  total_trade %>%
    left_join(gcc_oil_shares, by = "reporter_name") %>%
    mutate(
      oil_share = if_else(flow_direction == "export",
                          oil_share_exports,
                          oil_share_imports),
      trade_value_usd = trade_value_usd * (1 - oil_share),
      trade_value_million_usd = trade_value_million_usd * (1 - oil_share)
    ) %>%
    select(-oil_share, -oil_share_exports, -oil_share_imports)
}


# =============================================================================
# HS TO BEC MAPPING
# =============================================================================

#' Create HS 2-digit to BEC Category Mapping
#'
#' Maps HS chapters to three broad BEC (Broad Economic Categories):
#' primary, intermediate, final.
#'
#' @return Tibble with hs2 and bec_category columns
#' @export
create_hs_bec_mapping <- function() {
  tribble(
    ~hs2, ~bec_category,
    # Primary goods
    "01", "primary", "02", "primary", "03", "primary", "04", "primary",
    "05", "primary", "06", "primary", "07", "primary", "08", "primary",
    "09", "primary", "10", "primary", "11", "primary", "12", "primary",
    "13", "primary", "14", "primary", "25", "primary", "26", "primary",
    "27", "primary",
    # Intermediate goods
    "15", "intermediate", "28", "intermediate", "29", "intermediate",
    "30", "intermediate", "31", "intermediate", "32", "intermediate",
    "33", "intermediate", "34", "intermediate", "35", "intermediate",
    "36", "intermediate", "37", "intermediate", "38", "intermediate",
    "39", "intermediate", "40", "intermediate", "47", "intermediate",
    "48", "intermediate", "49", "intermediate", "54", "intermediate",
    "55", "intermediate", "56", "intermediate", "72", "intermediate",
    "73", "intermediate", "74", "intermediate", "75", "intermediate",
    "76", "intermediate", "78", "intermediate", "79", "intermediate",
    "80", "intermediate", "81", "intermediate", "82", "intermediate",
    "83", "intermediate",
    # Final goods
    "16", "final", "17", "final", "18", "final", "19", "final",
    "20", "final", "21", "final", "22", "final", "23", "final",
    "24", "final", "41", "final", "42", "final", "43", "final",
    "50", "final", "51", "final", "52", "final", "53", "final",
    "57", "final", "58", "final", "59", "final", "60", "final",
    "61", "final", "62", "final", "63", "final", "64", "final",
    "65", "final", "66", "final", "67", "final", "68", "final",
    "69", "final", "70", "final", "71", "final", "84", "final",
    "85", "final", "86", "final", "87", "final", "88", "final",
    "89", "final", "90", "final", "91", "final", "92", "final",
    "93", "final", "94", "final", "95", "final", "96", "final",
    "97", "final"
  )
}


# =============================================================================
# MODULE LOAD MESSAGE
# =============================================================================

message("
=======================================================
  COMTRADE API MODULE LOADED
=======================================================

Main function:
  - fetch_gcc_comtrade()       : Download & save Comtrade data

Configuration:
  - Set API key:  Sys.setenv(COMTRADE_API_KEY = 'your-key')
  - Default:      detailed_hs = TRUE (actual HS chapter data)
  - Output:       data-raw/comtrade_data.rds
                  data-raw/comtrade_data_hs.rds

Usage:
  source('R/00_comtrade_api.R')
  fetch_gcc_comtrade(start_year = 2015, end_year = 2024)

=======================================================
")
