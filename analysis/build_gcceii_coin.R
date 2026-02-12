# =============================================================================
# GCC ECONOMIC INTEGRATION INDEX - BUILD COIN (ANALYSIS SCRIPT)
# =============================================================================
#
# This script demonstrates the complete workflow for building the GCC EII
# using COINr with POOLED NORMALIZATION for time series comparability.
#
# Prerequisites:
#   1. Place data files in data-raw/ folder
#   2. Install COINr: install.packages("COINr")
#   3. Set working directory to gcceii folder
#
# =============================================================================

# Load required packages
library(tidyverse)
library(COINr)

# =============================================================================
# STEP 0: SOURCE ALL MODULES
# =============================================================================

message("Step 0: Loading GCCEII modules...")

source("R/01_data_loading.R")
source("R/02_helpers.R")
source("R/03_indicators_trade.R")
source("R/04_indicators_financial.R")
source("R/05_indicators_labor.R")
source("R/06_indicators_infrastructure.R")
source("R/07_indicators_sustainability.R")
source("R/08_indicators_convergence.R")
source("R/09_coinr_metadata.R")
source("R/10_normalization_config.R")

message("✓ All modules loaded\n")

# =============================================================================
# STEP 1: LOAD DATA
# =============================================================================

message("Step 1: Loading data...")

data_list <- load_gcc_data(data_dir = "data-raw")

message("✓ Data loaded\n")

# =============================================================================
# STEP 2: EXTRACT RAW INDICATORS (for each year)
# =============================================================================

message("Step 2: Extracting raw indicators...")

years <- 2015:2024
all_raw_data <- list()

for (yr in years) {
  message(paste("  Processing year:", yr))
  all_raw_data[[as.character(yr)]] <- extract_raw_indicators(data_list, yr)
}

# Combine into single iData
iData <- bind_rows(all_raw_data)

message(paste("✓ Total records:", nrow(iData)))
message(paste("  Indicators:", ncol(iData) - 3, "\n"))

# =============================================================================
# STEP 3: BUILD iMeta (Indicator Metadata)
# =============================================================================

message("Step 3: Building indicator metadata...")

iMeta <- build_iMeta(version = "poc")
validate_iMeta(iMeta)

message("✓ Metadata built\n")

# =============================================================================
# STEP 4: PREPARE iData WITH UNIQUE uCode
# =============================================================================

message("Step 4: Preparing data with unique identifiers...")

# Rename columns to match iMeta codes
col_mapping <- get_indicator_code_mapping()
iData_renamed <- iData

for (old_name in names(col_mapping)) {
  if (old_name %in% names(iData_renamed)) {
    names(iData_renamed)[names(iData_renamed) == old_name] <- col_mapping[[old_name]]
  }
}

# Make uCode unique by appending year (for pooled normalization)
iData_panel <- iData_renamed %>%
  mutate(
    uCode = paste0(uCode, "_", Year)  # e.g., "BHR_2023"
  ) %>%
  select(-Year)  # ADD THIS LINE - Remove Year column, it's encoded in uCode

message(paste("  Unique units:", n_distinct(iData_panel$uCode)))
message("✓ Data prepared\n")

# =============================================================================
# STEP 5: BUILD SINGLE COIN WITH ALL YEARS
# =============================================================================

message("Step 5: Building coin object...")

gcceii_coin <- new_coin(
  iData = iData_panel,
  iMeta = iMeta,
  level_names = c("Indicator", "Dimension", "Index")
)

message("✓ Coin built\n")

# =============================================================================
# STEP 5a: PRE-IMPUTATION (custom extrapolation for specific indicators)
# =============================================================================
# ind_71_student has nationality-level data only for 2016-2021. Raw_Data
# contains NA for 2015, 2022-2023. This step linearly extrapolates from the
# nearest two observed years before general imputation runs.
# =============================================================================

message("Step 5a: Pre-imputing specific indicators...")

gcceii_coin <- pre_impute_student_mobility(gcceii_coin)

message("✓ Pre-imputation complete\n")

# =============================================================================
# STEP 5b: IMPUTE MISSING DATA
# =============================================================================
# Two-pass strategy:
#   Pass 1: Linear interpolation/extrapolation within each country's time
#           series (fills 1-2 year gaps)
#   Pass 2: EM algorithm (Amelia) for remaining NAs (fills indicators where
#           a country is completely missing). Falls back to year-group median
#           if Amelia is not installed.
#
# Imputed values are logged in coin$Analysis$Imputation.
# Use get_imputation_summary(coin) to inspect what was imputed.
# =============================================================================

message("Step 5b: Imputing missing data...")

gcceii_coin <- impute_gcceii(gcceii_coin)

# =============================================================================
# STEP 6: NORMALIZE INDICATORS (Custom Strategy)
# =============================================================================

message("Step 6: Normalizing indicators (custom strategy)...")

# source("R/10_normalization_config.R")
gcceii_coin <- run_normalization_pipeline(gcceii_coin)

message("✓ Normalization complete")

# =============================================================================
# STEP 7: AGGREGATE TO DIMENSIONS AND INDEX
# =============================================================================

message("Step 7: Aggregating to dimensions and index...")

gcceii_coin <- Aggregate(
  gcceii_coin,
  dset = "Normalised",
  f_ag = "a_amean"
)

message("✓ Aggregation complete\n")

# =============================================================================
# STEP 8: EXTRACT RESULTS
# =============================================================================

message("Step 8: Extracting results...")

results <- get_results(gcceii_coin, dset = "Aggregated", tab_type = "Full") %>%
  # Extract year and country back from uCode
  mutate(
    Year = as.integer(str_extract(uCode, "\\d{4}$")),
    Country = str_remove(uCode, "_\\d{4}$")
  ) %>%
  # Replace COINr's pooled rank with within-year rank
  select(-Rank) %>%
  group_by(Year) %>%
  mutate(Rank = rank(-Index, ties.method = "min")) %>%
  ungroup() %>%
  arrange(Year, Rank)

# Append GDP-weighted GCC aggregate (no rank for GCC)
results <- append_gcc_aggregate(results)

# Latest year summary
latest_year <- max(results$Year)

latest_results <- results %>%
  filter(Year == latest_year) %>%
  select(Country, Rank, Trade, Financial, Labor,
         Infrastructure, Sustainability, Convergence, Index) %>%
  arrange(Country == "GCC", Rank)

message("\n=======================================================")
message(paste("  GCC EII RESULTS -", latest_year))
message("=======================================================\n")
print(latest_results)


# Time series summary (GCC weighted average)
message("\n=======================================================")
message("  GCC WEIGHTED INDEX BY YEAR")
message("=======================================================\n")
gcc_trend <- results %>%
  filter(Country == "GCC") %>%
  select(Year, GCC_Index = Index)
print(gcc_trend)

# =============================================================================
# STEP 9: EXPORT RESULTS
# =============================================================================

message("\nStep 9: Exporting results...")

# Create output directory
if (!dir.exists("output")) dir.create("output")

# Export to CSV
write_csv(results, "output/gcceii_results_all_years.csv")
write_csv(latest_results, paste0("output/gcceii_results_", latest_year, ".csv"))

# Save workspace for dashboard
save(
  gcceii_coin,
  iData,
  iData_panel,
  iMeta,
  results,
  gcc_trend,
  file = "output/gcceii_coin_workspace.RData"
)

# Export methodology XLSX (all data stages from coin object)
export_methodology_xlsx(
  coin = gcceii_coin,
  iMeta = iMeta,
  results = results,
  gcc_trend = gcc_trend,
  output_path = "output/GCCEII_Full_Methodology.xlsx"
)

# -------------------------------------------------------------------------
# STEP 9a: DASHBOARD EXPORT ADAPTER
# -------------------------------------------------------------------------
# Translates gcc_eii_beta column schema → GCCEII dashboard schema
# Dashboard repo: https://github.com/karimanninen/GCCEII
# One-way file interface: gcc_eii_beta produces, dashboard consumes

time_series_complete <- results %>%
  transmute(
    country              = Country,
    year                 = Year,
    overall_index        = Index,
    trade_score          = Trade,
    financial_score      = Financial,
    labor_score          = Labor,
    infrastructure_score = Infrastructure,
    sustainability_score = Sustainability,
    convergence_score    = Convergence,
    integration_level    = case_when(
      Index >= 60 ~ "Good",
      Index >= 40 ~ "Moderate",
      TRUE        ~ "Weak"
    ),
    method = if_else(Country == "GCC", "gdp", NA_character_)
  )

write_csv(time_series_complete, "output/time_series_complete.csv")
save(time_series_complete, file = "output/gcc_integration_workspace.RData")
message("✓ Dashboard exports: time_series_complete.csv + gcc_integration_workspace.RData")

message("✓ Results exported to output/\n")

# =============================================================================
# STEP 10: QUICK VISUALIZATIONS
# =============================================================================

message("Step 10: Creating visualizations...")

# Time trend plot
trend_plot <- ggplot(results, aes(x = Year, y = Index, color = Country)) +

  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  labs(
    title = "GCC Economic Integration Index (2015-2023)",
    subtitle = "Pooled normalization for time comparability",
    y = "Integration Index",
    color = "Country"
  ) +
  theme_minimal() +
  scale_x_continuous(breaks = years)

# Save plot
ggsave("output/gcceii_trend.png", trend_plot, width = 10, height = 6, dpi = 150)
message("✓ Trend plot saved to output/gcceii_trend.png\n")

# =============================================================================
# STEP 10a: PCA WEIGHT ESTIMATION
# =============================================================================
# Estimate data-driven indicator weights using PCA on the Normalised dataset.
# Two approaches:
#   1. Per-dimension PCA: PC1 squared loadings within each dimension
#   2. Whole-index PCA: Retain components for >=80% variance, derive weights
# Results are informational - compare against equal/expert weights.
# =============================================================================

message("Step 10a: Estimating PCA-based indicator weights...")

pca_weights <- estimate_pca_weights(gcceii_coin, dset = "Normalised", var_threshold = 0.80)

# Export PCA weight comparison tables
write_csv(pca_weights$by_dimension, "output/gcceii_pca_weights_by_dimension.csv")
write_csv(pca_weights$whole_index, "output/gcceii_pca_weights_whole_index.csv")
write_csv(pca_weights$dimension_weights, "output/gcceii_pca_dimension_weights.csv")

message("✓ PCA weights exported to output/")

# --- Re-aggregate with PCA weights to build comparison table ---
message("  Re-aggregating with PCA weights for comparison...")

# Save original weights
original_weights <- gcceii_coin$Meta$Ind %>%
  select(iCode, Weight) %>%
  deframe()

# Helper: swap weights, re-aggregate, extract per-country-year Index + dimensions
reaggregate_with_weights <- function(coin, ind_weights, dim_weights) {
  # Set indicator-level weights
  for (ind in names(ind_weights)) {
    idx <- which(coin$Meta$Ind$iCode == ind)
    if (length(idx) == 1) coin$Meta$Ind$Weight[idx] <- ind_weights[ind]
  }
  # Set dimension-level weights
  for (dim_name in names(dim_weights)) {
    idx <- which(coin$Meta$Ind$iCode == dim_name)
    if (length(idx) == 1) coin$Meta$Ind$Weight[idx] <- dim_weights[dim_name]
  }
  coin <- Aggregate(coin, dset = "Normalised", f_ag = "a_amean")
  res <- get_results(coin, dset = "Aggregated", tab_type = "Full") %>%
    mutate(
      Year = as.integer(str_extract(uCode, "\\d{4}$")),
      Country = str_remove(uCode, "_\\d{4}$")
    )
  res <- append_gcc_aggregate(res)
  return(res)
}

# --- Approach 1: Per-dimension PCA weights (dimension weights unchanged) ---
pca_dim_ind_weights <- pca_weights$by_dimension %>%
  select(iCode, pca_dim_weight) %>%
  deframe()

# Keep original dimension weights
orig_dim_weights <- gcceii_coin$Meta$Ind %>%
  filter(Type == "Aggregate", Level == 2) %>%
  select(iCode, Weight) %>%
  deframe()

res_pca_dim <- reaggregate_with_weights(gcceii_coin, pca_dim_ind_weights, orig_dim_weights)

# --- Approach 2: Whole-index PCA weights (both indicator and dimension) ---
pca_whole_ind_weights <- pca_weights$whole_index %>%
  select(iCode, pca_whole_weight) %>%
  deframe()

pca_whole_dim_weights <- pca_weights$dimension_weights %>%
  select(dimension, pca_whole_weight) %>%
  deframe()

res_pca_whole <- reaggregate_with_weights(gcceii_coin, pca_whole_ind_weights, pca_whole_dim_weights)

# --- Restore original weights ---
for (ic in names(original_weights)) {
  idx <- which(gcceii_coin$Meta$Ind$iCode == ic)
  if (length(idx) == 1) gcceii_coin$Meta$Ind$Weight[idx] <- original_weights[ic]
}
gcceii_coin <- Aggregate(gcceii_coin, dset = "Normalised", f_ag = "a_amean")

# --- Build comparison table ---
comparison <- results %>%
  select(Country, Year,
         Trade_orig = Trade, Financial_orig = Financial,
         Labor_orig = Labor, Infrastructure_orig = Infrastructure,
         Sustainability_orig = Sustainability, Convergence_orig = Convergence,
         Index_orig = Index) %>%
  left_join(
    res_pca_dim %>% select(Country, Year, Index_pca_dim = Index,
                           Trade_pca_dim = Trade, Financial_pca_dim = Financial,
                           Labor_pca_dim = Labor, Infrastructure_pca_dim = Infrastructure,
                           Sustainability_pca_dim = Sustainability, Convergence_pca_dim = Convergence),
    by = c("Country", "Year")
  ) %>%
  left_join(
    res_pca_whole %>% select(Country, Year, Index_pca_whole = Index,
                             Trade_pca_whole = Trade, Financial_pca_whole = Financial,
                             Labor_pca_whole = Labor, Infrastructure_pca_whole = Infrastructure,
                             Sustainability_pca_whole = Sustainability, Convergence_pca_whole = Convergence),
    by = c("Country", "Year")
  ) %>%
  arrange(Year, desc(Index_orig))

write_csv(comparison, "output/gcceii_index_comparison.csv")

# Print latest year summary
message("\n--- Index comparison (", latest_year, ") ---")
comp_latest <- comparison %>%
  filter(Year == latest_year) %>%
  select(Country, Index_orig, Index_pca_dim, Index_pca_whole) %>%
  arrange(desc(Index_orig))

for (i in seq_len(nrow(comp_latest))) {
  r <- comp_latest[i, ]
  message(sprintf("  %-8s  Original: %5.1f   PCA-dim: %5.1f   PCA-whole: %5.1f",
                  r$Country, r$Index_orig, r$Index_pca_dim, r$Index_pca_whole))
}

message("\n✓ Comparison table exported to output/gcceii_index_comparison.csv\n")

# =============================================================================
# SUMMARY
# =============================================================================

message("=======================================================")
message("  GCC EII BUILD COMPLETE")
message("=======================================================")
message(paste("Years:", min(results$Year), "-", max(results$Year)))
message(paste("Countries:", n_distinct(results$Country)))
message(paste("Indicators:", sum(iMeta$Type == "Indicator")))
message(paste("Dimensions:", sum(iMeta$Level == 2)))
message(paste("Normalization: Pooled (min-max across all years)"))
message("=======================================================")
message("\nOutputs saved to:")
message("  - output/gcceii_results_all_years.csv")
message(paste0("  - output/gcceii_results_", latest_year, ".csv"))
message("  - output/gcceii_trend.png")
message("  - output/gcceii_coin_workspace.RData")
message("  - output/gcceii_pca_weights_by_dimension.csv")
message("  - output/gcceii_pca_weights_whole_index.csv")
message("  - output/gcceii_pca_dimension_weights.csv")
message("  - output/gcceii_index_comparison.csv")
message("  - output/gcceii_sensitivity_ranks.csv")
message("  - output/gcceii_sensitivity_gcc_detail.csv")
message("  - output/gcceii_sensitivity_gcc_summary.csv")
message("=======================================================")

# =============================================================================
# OPTIONAL: SENSITIVITY ANALYSIS
# =============================================================================
# COINr's get_sensitivity() requires all pipeline steps in coin$Log so it can
# regenerate the coin with varied parameters. Our custom steps (imputation,
# winsorization, z-score rescaling) are not in the Log, so we run a manual
# sensitivity analysis instead: re-normalise and re-aggregate from the Treated
# dataset using different method combinations.
# =============================================================================

message("\nRunning sensitivity analysis...")

sa_norm_methods <- c("n_minmax", "n_rank", "n_zscore")
sa_agg_methods  <- c("a_amean", "a_gmean")
sa_N <- 100

# Source dataset: Treated exists after winsorization
sa_source <- if ("Treated" %in% names(gcceii_coin$Data)) "Treated" else
             if ("Imputed" %in% names(gcceii_coin$Data)) "Imputed" else "Raw"

sa_results_list <- vector("list", sa_N)
sa_errors <- character(0)

for (sa_i in seq_len(sa_N)) {
  sa_norm <- sample(sa_norm_methods, 1)
  sa_agg  <- sample(sa_agg_methods, 1)

  # Build method-appropriate normalization specs
  # n_rank and n_zscore don't accept l_u; only n_minmax uses it.
  # Use l_u = c(1, 100) for minmax to avoid zeros (which break a_gmean).
  sa_specs <- switch(sa_norm,
    n_minmax = list(f_n = "n_minmax", f_n_para = list(l_u = c(1, 100))),
    n_rank   = list(f_n = "n_rank"),
    n_zscore = list(f_n = "n_zscore"),
    list(f_n = sa_norm)
  )

  sa_coin <- tryCatch({
    suppressMessages({
      c_tmp <- Normalise(gcceii_coin, dset = sa_source,
                         global_specs = sa_specs,
                         write_to = "SA_Normalised")
      Aggregate(c_tmp, dset = "SA_Normalised", f_ag = sa_agg)
    })
  }, error = function(e) {
    sa_errors <<- c(sa_errors, paste0(sa_norm, "+", sa_agg, ": ", e$message))
    NULL
  })

  if (!is.null(sa_coin)) {
    sa_res <- get_results(sa_coin, dset = "Aggregated", tab_type = "Full")
    sa_results_list[[sa_i]] <- sa_res %>%
      select(uCode, Index) %>%
      mutate(iteration = sa_i, norm = sa_norm, agg = sa_agg)
  }
}

sa_all <- bind_rows(sa_results_list)
sa_n_ok <- sum(!sapply(sa_results_list, is.null))

# Report method breakdown
if (sa_n_ok > 0) {
  sa_method_counts <- sa_all %>%
    distinct(iteration, norm, agg) %>%
    count(norm, agg, name = "n_replications")
  message(paste("\n  Method combinations that succeeded:"))
  for (r in seq_len(nrow(sa_method_counts))) {
    message(paste("   ", sa_method_counts$norm[r], "+",
                  sa_method_counts$agg[r], ":",
                  sa_method_counts$n_replications[r], "replications"))
  }
}

# Report unique errors
if (length(sa_errors) > 0) {
  unique_errors <- unique(sa_errors)
  message(paste("\n  Failed combinations (", length(sa_errors), "total ):"))
  for (ue in unique_errors) {
    message(paste("   ", ue))
  }
}

# Rank statistics per unit
sa_rank_summary <- sa_all %>%
  group_by(iteration) %>%
  mutate(rank = rank(-Index, ties.method = "average")) %>%
  ungroup() %>%
  group_by(uCode) %>%
  summarize(
    mean_index = round(mean(Index, na.rm = TRUE), 2),
    sd_index   = round(sd(Index, na.rm = TRUE), 2),
    mean_rank  = round(mean(rank), 2),
    sd_rank    = round(sd(rank), 2),
    min_rank   = min(rank),
    max_rank   = max(rank),
    .groups = "drop"
  ) %>%
  arrange(mean_rank)

message(paste("\n✓ Sensitivity analysis complete (", sa_n_ok, "/", sa_N,
              "replications succeeded )"))
message("\nRank stability across normalization/aggregation choices:")
print(as.data.frame(sa_rank_summary))

# Save SA results
write_csv(sa_rank_summary, "output/gcceii_sensitivity_ranks.csv")
message("  Saved to output/gcceii_sensitivity_ranks.csv")

# =============================================================================
# SENSITIVITY ANALYSIS EXTENSION: WEIGHTING × NORMALIZATION × AGGREGATION
# =============================================================================
# Extends the analysis above by adding 3 weighting schemes (original, PCA-dim,
# PCA-whole) as a third dimension. Focuses on the GCC aggregate index to show
# how the overall integration measure varies across all methodological choices.
# =============================================================================

message("\nRunning extended sensitivity analysis (with PCA weights)...")

# Define the 3 weight schemes (using PCA weights computed in Step 10a)
sa_weight_schemes <- list(
  original = list(
    ind = gcceii_coin$Meta$Ind %>%
      filter(Type == "Indicator") %>% select(iCode, Weight) %>% deframe(),
    dim = gcceii_coin$Meta$Ind %>%
      filter(Type == "Aggregate", Level == 2) %>% select(iCode, Weight) %>% deframe()
  ),
  pca_dim = list(
    ind = pca_weights$by_dimension %>% select(iCode, pca_dim_weight) %>% deframe(),
    dim = orig_dim_weights
  ),
  pca_whole = list(
    ind = pca_weights$whole_index %>% select(iCode, pca_whole_weight) %>% deframe(),
    dim = pca_weights$dimension_weights %>% select(dimension, pca_whole_weight) %>% deframe()
  )
)

# Extended SA uses only bounded normalization methods (z-score excluded -
# its unbounded scale is not comparable for GCC aggregate level analysis)
sa_ext_norm_methods <- c("n_minmax", "n_rank")

sa_ext_N <- 150
sa_ext_results <- vector("list", sa_ext_N)
sa_ext_errors <- character(0)

for (sa_i in seq_len(sa_ext_N)) {
  sa_norm   <- sample(sa_ext_norm_methods, 1)
  sa_agg    <- sample(sa_agg_methods, 1)
  sa_wt_name <- sample(names(sa_weight_schemes), 1)
  sa_wt <- sa_weight_schemes[[sa_wt_name]]

  sa_specs <- switch(sa_norm,
    n_minmax = list(f_n = "n_minmax", f_n_para = list(l_u = c(1, 100))),
    n_rank   = list(f_n = "n_rank"),
    n_zscore = list(f_n = "n_zscore"),
    list(f_n = sa_norm)
  )

  sa_res <- tryCatch({
    # Temporarily set weights
    sa_coin <- gcceii_coin
    for (ind in names(sa_wt$ind)) {
      idx <- which(sa_coin$Meta$Ind$iCode == ind)
      if (length(idx) == 1) sa_coin$Meta$Ind$Weight[idx] <- sa_wt$ind[ind]
    }
    for (d in names(sa_wt$dim)) {
      idx <- which(sa_coin$Meta$Ind$iCode == d)
      if (length(idx) == 1) sa_coin$Meta$Ind$Weight[idx] <- sa_wt$dim[d]
    }

    suppressMessages({
      sa_coin <- Normalise(sa_coin, dset = sa_source,
                           global_specs = sa_specs,
                           write_to = "SA_Normalised")
      sa_coin <- Aggregate(sa_coin, dset = "SA_Normalised", f_ag = sa_agg)
    })

    res <- get_results(sa_coin, dset = "Aggregated", tab_type = "Full") %>%
      mutate(
        Year = as.integer(str_extract(uCode, "\\d{4}$")),
        Country = str_remove(uCode, "_\\d{4}$")
      )
    res <- append_gcc_aggregate(res)

    # Return GCC aggregate rows + dimension scores
    res %>%
      filter(Country == "GCC") %>%
      select(Country, Year, Trade, Financial, Labor,
             Infrastructure, Sustainability, Convergence, Index) %>%
      mutate(iteration = sa_i, norm = sa_norm, agg = sa_agg, weights = sa_wt_name)
  }, error = function(e) {
    sa_ext_errors <<- c(sa_ext_errors,
      paste0(sa_norm, "+", sa_agg, "+", sa_wt_name, ": ", e$message))
    NULL
  })

  sa_ext_results[[sa_i]] <- sa_res
}

sa_ext_all <- bind_rows(sa_ext_results)
sa_ext_ok <- sum(!sapply(sa_ext_results, is.null))

# --- GCC Aggregate sensitivity summary ---
message(paste("\n✓ Extended SA complete (", sa_ext_ok, "/", sa_ext_N,
              "replications succeeded )"))

# Method breakdown
sa_ext_counts <- sa_ext_all %>%
  distinct(iteration, norm, agg, weights) %>%
  count(norm, agg, weights, name = "n")
message("\n  Method combinations (norm + agg + weights):")
for (r in seq_len(nrow(sa_ext_counts))) {
  rc <- sa_ext_counts[r, ]
  message(sprintf("    %-10s + %-8s + %-12s : %d replications",
                  rc$norm, rc$agg, rc$weights, rc$n))
}

if (length(sa_ext_errors) > 0) {
  message(paste("\n  Failed:", length(sa_ext_errors), "total"))
}

# Summarise GCC Index by year across all methodological variations
sa_gcc_summary <- sa_ext_all %>%
  group_by(Year) %>%
  summarize(
    mean_index = round(mean(Index, na.rm = TRUE), 2),
    sd_index   = round(sd(Index, na.rm = TRUE), 2),
    min_index  = round(min(Index, na.rm = TRUE), 2),
    max_index  = round(max(Index, na.rm = TRUE), 2),
    .groups = "drop"
  ) %>%
  arrange(Year)

message("\nGCC Aggregate Index - sensitivity range by year:")
message(sprintf("  %-6s  %8s  %8s  %8s  %8s",
                "Year", "Mean", "SD", "Min", "Max"))
for (i in seq_len(nrow(sa_gcc_summary))) {
  r <- sa_gcc_summary[i, ]
  message(sprintf("  %-6d  %8.2f  %8.2f  %8.2f  %8.2f",
                  r$Year, r$mean_index, r$sd_index, r$min_index, r$max_index))
}

# Dimension-level sensitivity for GCC (latest year)
sa_gcc_dim <- sa_ext_all %>%
  filter(Year == latest_year) %>%
  pivot_longer(cols = c(Trade, Financial, Labor, Infrastructure,
                        Sustainability, Convergence, Index),
               names_to = "Dimension", values_to = "Score") %>%
  group_by(Dimension) %>%
  summarize(
    mean_score = round(mean(Score, na.rm = TRUE), 2),
    sd_score   = round(sd(Score, na.rm = TRUE), 2),
    min_score  = round(min(Score, na.rm = TRUE), 2),
    max_score  = round(max(Score, na.rm = TRUE), 2),
    .groups = "drop"
  )

message(paste0("\nGCC Dimension Scores (", latest_year, ") - sensitivity range:"))
message(sprintf("  %-18s  %8s  %8s  %8s  %8s",
                "Dimension", "Mean", "SD", "Min", "Max"))
for (i in seq_len(nrow(sa_gcc_dim))) {
  r <- sa_gcc_dim[i, ]
  message(sprintf("  %-18s  %8.2f  %8.2f  %8.2f  %8.2f",
                  r$Dimension, r$mean_score, r$sd_score, r$min_score, r$max_score))
}

# Impact of each methodological choice on GCC Index (latest year)
sa_gcc_latest <- sa_ext_all %>% filter(Year == latest_year)

sa_impact <- bind_rows(
  sa_gcc_latest %>% group_by(choice = norm) %>%
    summarize(mean_idx = mean(Index, na.rm = TRUE), .groups = "drop") %>%
    mutate(factor = "Normalization"),
  sa_gcc_latest %>% group_by(choice = agg) %>%
    summarize(mean_idx = mean(Index, na.rm = TRUE), .groups = "drop") %>%
    mutate(factor = "Aggregation"),
  sa_gcc_latest %>% group_by(choice = weights) %>%
    summarize(mean_idx = mean(Index, na.rm = TRUE), .groups = "drop") %>%
    mutate(factor = "Weighting")
)

message(paste0("\nImpact of methodological choices on GCC Index (", latest_year, "):"))
for (f in unique(sa_impact$factor)) {
  message(paste0("  ", f, ":"))
  sub <- sa_impact %>% filter(factor == f) %>% arrange(desc(mean_idx))
  for (i in seq_len(nrow(sub))) {
    message(sprintf("    %-14s  %.2f", sub$choice[i], sub$mean_idx[i]))
  }
}

# Export
write_csv(sa_ext_all, "output/gcceii_sensitivity_gcc_detail.csv")
write_csv(sa_gcc_summary, "output/gcceii_sensitivity_gcc_summary.csv")
message("\n  Saved to output/gcceii_sensitivity_gcc_detail.csv")
message("  Saved to output/gcceii_sensitivity_gcc_summary.csv")
