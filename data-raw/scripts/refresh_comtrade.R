# =============================================================================
# REFRESH COMTRADE DATA
# =============================================================================
#
# Run this script interactively to pull fresh trade data from the
# UN Comtrade API and update the RDS files in data-raw/.
#
# Prerequisites:
#   1. install.packages("comtradr")
#   2. Set your API key (get one at https://comtradeplus.un.org/):
#      Sys.setenv(COMTRADE_API_KEY = "your-key-here")
#   3. Set working directory to the project root (gcc_eii_beta/)
#
# The script downloads all 97 HS chapters for actual BEC classification
# and non-oil trade filtering. This takes ~15-30 minutes due to API
# rate limits (6 reporters x 97 chapters x 1.5s pause = ~15 min minimum).
#
# After running, the main pipeline (analysis/build_gcceii_coin.R) will
# automatically pick up the refreshed data via load_comtrade_data().
#
# =============================================================================

# Load the Comtrade API module
source("R/00_comtrade_api.R")

# --- Set your API key here if not already in environment ---
# Sys.setenv(COMTRADE_API_KEY = "your-key-here")

# --- Fetch and save ---
fetch_gcc_comtrade(
  start_year = 2015,
  end_year   = 2024,
  output_dir = "data-raw",
  detailed_hs = TRUE   # Download actual HS chapters (slower but accurate)
)

message("\nDone! The pipeline will use the updated data on next build.")
message("Run: source('analysis/build_gcceii_coin.R')")
