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

years <- 2015:2023
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
  arrange(Year, desc(Index))

# Latest year summary
latest_year <- max(results$Year)

latest_results <- results %>%
  filter(Year == latest_year) %>%
  select(Country, Trade, Financial, Labor, 
         Infrastructure, Sustainability, Convergence, Index) %>%
  arrange(desc(Index))

message("\n=======================================================")
message(paste("  GCC EII RESULTS -", latest_year))
message("=======================================================\n")
print(latest_results)


# Time series summary
message("\n=======================================================")
message("  GCC AVERAGE INDEX BY YEAR")
message("=======================================================\n")
gcc_trend <- results %>%
  group_by(Year) %>%
  summarize(
    GCC_Index = mean(Index, na.rm = TRUE),
    .groups = "drop"
  )
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
message("=======================================================")

# =============================================================================
# OPTIONAL: SENSITIVITY ANALYSIS
# =============================================================================

# Uncomment to run sensitivity analysis (takes a few minutes)

message("\nRunning sensitivity analysis...")
 
sa_results <- get_sensitivity(
  gcceii_coin,
  SA_specs = list(
    Normalisation = c("n_minmax", "n_rank", "n_zscore"),
    Aggregation = c("a_amean", "a_gmean")
  ),
  N = 100,
  SA_type = "UA"
)

message("✓ Sensitivity analysis complete")
