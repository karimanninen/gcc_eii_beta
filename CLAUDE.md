# CLAUDE.md - GCCEII Project Context

> This file provides context for Claude Code sessions working on the GCC Economic Integration Index.

## Project Overview

**Repository:** `gcc_eii_beta`  
**Purpose:** R package for calculating the GCC Economic Integration Index (GCCEII) using the COINr framework  
**Organisation:** GCC-Stat, Economic Statistics Department  
**Status:** Proof of Concept → Production Migration

The GCCEII is a composite indicator measuring economic integration across the six Gulf Cooperation Council countries (UAE, Bahrain, Saudi Arabia, Oman, Qatar, Kuwait) from 2015-2023. It supports Vision 2030 transformation goals and positions GCC-Stat as the authoritative source for regional integration measurement.

---

## Technical Stack

| Component | Technology | Notes |
|-----------|------------|-------|
| Language | R 4.x | tidyverse style throughout |
| Composite Indicators | COINr package | JRC/OECD methodology |
| Data Manipulation | dplyr, tidyr, stringr | Pipes preferred |
| Visualisation | ggplot2, plotly | Interactive dashboards |
| Dashboards | Shiny + shinydashboard | Future: deployed on Posit Connect |
| Data Sources | CSV (current), SDMX (future) | GCC-Stat warehouse + external APIs |
| Version Control | Git/GitHub | Commit working code frequently |

### Key Packages
```r
library(tidyverse)
library(COINr)        # Composite indicator construction
library(readxl)       # Excel framework file
library(jsonlite)     # API responses
library(httr)         # API calls (Comtrade)
```

---

## Repository Structure

```
gcc_eii_beta/
├── R/                              # Package functions (source these)
│   ├── 01_data_loading.R          # ✅ Data extraction → tidy datasets
│   ├── 02_helpers.R               # ✅ Pure utility functions
│   ├── 03_indicators_trade.R      # ✅ Trade indicator calculations
│   ├── 04_indicators_financial.R  # ✅ Financial indicator calculations
│   ├── 05_indicators_labor.R      # ✅ Labor indicator calculations
│   ├── 06_indicators_infrastructure.R # ⚠️ Infrastructure (4 placeholders)
│   ├── 07_indicators_sustainability.R # ✅ Sustainability indicators
│   ├── 08_indicators_convergence.R    # ✅ Convergence indicators
│   ├── 09_coinr_metadata.R        # ✅ iMeta builder for COINr
│   └── 10_normalization_config.R  # ✅ Normalization pipeline
├── analysis/                       # Analysis scripts (not part of package)
│   └── build_gcceii_coin.R        # ✅ Main COINr workflow (working)
├── data-raw/                       # Source data files (CSV)
├── output/                         # Generated results
├── DESCRIPTION                     # 🔲 Package metadata
├── NAMESPACE                       # 🔲 Exports
├── CLAUDE.md                       # This file
└── README.md                       # Project documentation
```

**Legend:** ✅ Complete | 🔲 To be created | ⚠️ Needs revision

**Completion Status:** ~95% (28 indicators operational, 4 infrastructure placeholders remaining)

---

## Data Sources

### GCC-Stat Internal (data-raw/)
| File | Content | Key Variables |
|------|---------|---------------|
| `DF_ES_NA.csv` | National Accounts | GDP, GNI, expenditure components |
| `DF_Common_Market_Tables.csv` | Common Market indicators | Various integration metrics |
| `DF_ES_CPI.csv` | Consumer Price Index | Inflation rates |
| `DF_ES_MF.csv` | Monetary & Financial | Interest rates, money supply |
| `DF_GEETS_TUR.csv` | Tourism | Arrivals, departures, receipts |
| `DF_PSS_LAB.csv` | Labor Force | Employment, participation |
| `DF_PSS_DEM_POP.csv` | Population | Total, by nationality |
| `DF_GEETS_ENR.csv` | Energy | Production, consumption |

### External Data (data-raw/)
| File | Source | Content |
|------|--------|---------|
| `comtrade_data.rds` | UN Comtrade API | Aggregate trade flows |
| `comtrade_data_hs.rds` | UN Comtrade API | HS-level trade detail |
| `GCC FDI flows.csv` | UNCTAD/National sources | FDI inflows/outflows |
| `ICP_data.csv` | World Bank ICP | Price level indices |

### Country Codes
```r
# Standard GCC country names (use these consistently)
gcc_countries <- c("United Arab Emirates", "Bahrain", "Saudi Arabia", 
                   "Oman", "Qatar", "Kuwait")

# ISO3 codes
gcc_iso3 <- c("ARE", "BHR", "SAU", "OMN", "QAT", "KWT")
```

---

## Index Framework

### Dimensions and Weights
| Dimension | Weight | Code | Key Focus |
|-----------|--------|------|-----------|
| Trade Integration | 20% | `Trade` | Intra-GCC trade, non-oil diversification |
| Financial Integration | 20% | `Financial` | OCA criteria, banking, capital markets |
| Labor & Mobility | 20% | `Labor` | Worker mobility, students, tourism |
| Infrastructure | 20% | `Infrastructure` | Aviation, energy, digital connectivity |
| Sustainability | 10% | `Sustainability` | Non-oil GDP, manufacturing, diversification |
| Convergence | 10% | `Convergence` | Cross-country CV for key variables |

### Hierarchy Structure
```
Level 3: Index (GCCEII)
    └── Level 2: Dimensions (Trade, Financial, Labor, Infrastructure, Sustainability, Convergence)
        └── Level 1: Indicators (28 current, expanding to 90)
```

### Special Indicator Types

**Standard Indicators:** Higher value = better integration
- Use `Direction = 1` in iMeta
- Examples: trade intensity, FDI share, connectivity index

**Negative Indicators:** Lower value = better integration  
- Use `Direction = -1` in iMeta
- Examples: inflation differential, price dispersion

**Convergence Indicators:** Measure regional similarity using CV
- Same score for all countries in a given year
- Formula: `score = 100 - CV` (capped at 0-100)
- Examples: GDP per capita convergence, inflation convergence

---

## COINr Workflow

### Actual Pipeline (build_gcceii_coin.R)
```r
# Step 0: Source all modules (R/01-10)
# Step 1: Load raw data
data_list <- load_gcc_data(data_dir = "data-raw")

# Step 2: Extract raw indicators for all years (2015-2023 pooled panel)
iData_panel <- map(years, ~extract_raw_indicators(data_list, .x)) %>% bind_rows()

# Step 3: Build metadata
iMeta <- build_iMeta(version = "poc")

# Step 4: Prepare data with unique uCodes (e.g. "BHR_2023")
# Step 5: Construct coin
coin <- new_coin(iData, iMeta, level_names = c("Indicator", "Dimension", "Index"))

# Step 5a: Pre-impute specific indicators (e.g. ind_71_student extrapolation)
coin <- pre_impute_student_mobility(coin)

# Step 5b: Impute missing data (linear interpolation + EM)
coin <- impute_gcceii(coin)

# Step 6: Normalize (custom multi-strategy pipeline)
#   - Winsorize 9 indicators at 5th/95th percentile
#   - Z-score for OCA criteria (ind_gdp_growth, ind_inflation, ind_m2_growth)
#   - Goalpost for meaningful percentages (ind_44_stock)
#   - Bounded min-max for structurally constrained indicators (ind_39_banking)
#   - Min-max 0-100 for all others (pooled across years)
#   - Rescale z-scores to 0-100
coin <- run_normalization_pipeline(coin)

# Step 7: Aggregate (arithmetic weighted mean)
coin <- Aggregate(coin, dset = "Normalised", f_ag = "a_amean")

# Step 8: Extract results with within-year rankings + GCC weighted aggregate
results <- get_results(coin, ...) %>% append_gcc_aggregate()

# Step 9: Export CSV, XLSX, workspace
# Step 10: Visualizations
# Step 10a: PCA weight estimation + comparison table
# Sensitivity analysis: normalization × aggregation × weighting
```

---

## Coding Standards

### General Principles
1. **Indicator functions return RAW values only** - COINr handles normalization
2. **Use tidyverse style** - pipes, tibbles, snake_case
3. **Document all functions** - roxygen2 format
4. **Fail gracefully** - handle missing data, return informative errors
5. **Test incrementally** - run functions as you build them

### Function Template
```r
#' Calculate [Indicator Name]
#'
#' @description
#' Extracts raw [indicator] values for all GCC countries.
#'
#' @param data_list List from load_gcc_data()
#' @param year_filter Year to extract (default: 2023)
#'
#' @return Tibble with columns: country, year, indicator_code, raw_value
#'
#' @examples
#' data_list <- load_gcc_data()
#' result <- calculate_indicator_name(data_list, 2023)
#'
#' @export
calculate_indicator_name <- function(data_list, year_filter = 2023) {
  
  # Input validation
  stopifnot("national_accounts" %in% names(data_list))
  
  # Extract and process
  result <- data_list$national_accounts %>%
    filter(year == year_filter) %>%
    # ... processing logic ...
    select(country, year, raw_value) %>%
    mutate(indicator_code = "indicator_name")
  
  # Validate output
  if (nrow(result) < 6) {
    warning(paste("Only", nrow(result), "countries returned for indicator_name"))
  }
  
  return(result)
}
```

### Naming Conventions
| Type | Convention | Example |
|------|------------|---------|
| Functions | `verb_noun()` | `calculate_trade_intensity()` |
| Indicator codes | `snake_case` | `intra_gcc_trade_share` |
| Data frames | Descriptive noun | `trade_indicators`, `country_index` |
| Constants | UPPER_SNAKE | `GCC_COUNTRIES` |

### Output Format for Indicator Functions
All indicator extraction functions should return this structure:
```r
# Standard output format
tibble(
  country = character(),        # Full country name (standardized)
  year = integer(),             # 4-digit year
  indicator_code = character(), # Snake_case identifier
  raw_value = numeric(),        # Unprocessed value
  unit = character()            # Optional: %, USD millions, index, etc.
)
```

---

## Current Priorities

### Immediate (This Week)
1. Complete infrastructure placeholders in `06_indicators_infrastructure.R`:
   - `calc_railway_raw()` - GCC Railway data
   - `calc_port_raw()` - UNCTAD LSCI/port data
   - `calc_gccia_raw()` - GCCIA power grid data
   - `calc_digital_raw()` - ITU ICT data
2. Replace hardcoded estimates in trade indicators with real data sources
3. Run full compilation test end-to-end

### Short-term (Next 2 Weeks)
4. Create `DESCRIPTION` file - package metadata and dependencies
5. Generate `NAMESPACE` - export key functions using roxygen2
6. Add roxygen2 documentation to exported functions
7. Create unit tests with `testthat`

### Medium-term (This Month)
8. Set up GitHub Actions CI for R CMD check
9. Implement SDMX data loader (replace placeholder)
10. Create Shiny dashboard for results visualization
11. Document methodology in vignette

### Completed ✅
- All indicator modules (01-05, 07-10) fully implemented
- COINr workflow with pooled normalization (2015-2023 panel)
- Custom normalization pipeline (winsorize, z-score, goalpost, bounded min-max)
- Two-pass imputation (linear interpolation + EM)
- Pre-imputation for ind_71_student (linear extrapolation outside 2016-2021)
- Fixed ind_44_stock data filter bug and goalpost normalization
- Fixed ind_39_banking double-counting and bounded min-max [50,100]
- Fixed ind_71_student UAE data (nationality breakdowns instead of KN_TTL)
- GCC GDP-weighted aggregate index for all outputs
- Within-year country rankings (1-6 per year)
- PCA weight estimation (per-dimension + whole-index)
- PCA-weighted aggregate comparison table
- Sensitivity analysis: normalization × aggregation (country-level)
- Extended sensitivity analysis: normalization × aggregation × weighting (GCC aggregate)
- Full methodology XLSX export

---

## Common Tasks

### Load and Test Data
```r
source("R/01_data_loading.R")
source("R/02_helpers.R")
data_list <- load_gcc_data(data_dir = "data-raw")
str(data_list)  # Check structure
```

### Test an Indicator Function
```r
source("R/03_indicators_trade.R")
trade_raw <- calculate_trade_indicators(data_list, 2023)
print(trade_raw)
```

### Build and Inspect Coin
```r
library(COINr)
# Run the full build script
source("analysis/build_gcceii_coin.R")
# Or inspect the saved workspace
load("output/gcceii_coin_workspace.RData")
get_results(gcceii_coin, dset = "Aggregated")
```

### Check Distributions
```r
# After building coin
get_stats(coin, dset = "Raw", out2 = "df") %>%
  select(iCode, Mean, Median, Skew, Kurt) %>%
  filter(abs(Skew) > 2 | Kurt > 3.5)  # Flag problematic indicators
```

### Run Diagnostics
```r
# Correlation analysis
get_corr(coin, dset = "Normalised", cor_thresh = 0.9)

# Data availability
get_data_avail(coin, dset = "Raw")
```

---

## Troubleshooting

### Common Issues

**"Column not found" errors**
- Check column names in source data: `names(data_list$national_accounts)`
- GCC-Stat CSVs may have varying column names between files

**Missing countries in output**
- Check country name standardization: `unique(df$country)`
- Use `standardize_countries()` from `01_data_loading.R`

**COINr errors about directions**
- Ensure iMeta has `Direction` column with 1 or -1 for all indicators
- Aggregates should have `Direction = NA`

**Geometric mean fails**
- Check for zero or negative values in normalized data
- Use `n_minmax` with `l_u = c(1, 100)` instead of `c(0, 100)`

### Useful Debugging
```r
# Check what's in the coin
coin$Data$Raw %>% head()
coin$Meta$Ind %>% select(iCode, Parent, Level, Weight, Direction)

# Trace a specific indicator
get_data(coin, dset = "Raw", iCodes = "trade_intensity")
```

---

## References

- [COINr Documentation](https://bluefoxr.github.io/COINr/)
- [COINr Book (v0.6)](https://bluefoxr.github.io/COINrDoc/) - Detailed methodology
- [JRC/OECD Handbook on Composite Indicators](https://publications.jrc.ec.europa.eu/repository/handle/JRC47008)
- GCC-Stat Indicator Framework (Excel file in data-raw/)

---

## Contact

**Project Lead:** Economic Statistics Department, GCC-Stat  
**Repository:** https://github.com/karimanninen/gcc_eii_beta

---

## Known Issues

### Infrastructure Data Gaps
Four infrastructure indicators return `NA_real_` due to missing external data:
- Railway connectivity (needs GCC Railway Committee data)
- Port connectivity (needs UNCTAD LSCI data)
- GCCIA power grid (needs GCCIA annual reports)
- Digital connectivity (needs ITU ICT Index data)

### Hardcoded Estimates in Trade Module
Some trade indicators use fallback estimates when data unavailable:
- `ind_52` (services trade) - fixed estimates
- `ind_56` (services percentage) - hardcoded percentages
- `ind_63`, `ind_64` - fallback values

These are documented in the code and should be replaced with real data sources.

---

*Last updated: February 2026*
