---
editor_options: 
  markdown: 
    wrap: 72
---

# GCCEII - GCC Economic Integration Index

## Overview

R package for calculating the GCC Economic Integration Index using the
COINr framework for composite indicator construction.

## Package Structure

```
gcc_eii_beta/
├── R/                              # Package functions
│   ├── 01_data_loading.R          # ✅ Data extraction → tidy datasets
│   ├── 02_helpers.R               # ✅ Pure utility functions
│   ├── 03_indicators_trade.R      # ✅ Trade indicator calculations
│   ├── 04_indicators_financial.R  # ✅ Financial indicator calculations
│   ├── 05_indicators_labor.R      # ✅ Labor indicator calculations
│   ├── 06_indicators_infrastructure.R # ⚠️ Infrastructure (4 placeholders)
│   ├── 07_indicators_sustainability.R # ✅ Sustainability indicator calculations
│   ├── 08_indicators_convergence.R    # ✅ Convergence indicator calculations
│   ├── 09_coinr_metadata.R        # ✅ iMeta builder for COINr
│   └── 10_normalization_config.R  # ✅ Normalization pipeline
├── analysis/                       # Analysis scripts (not part of package)
│   └── build_gcceii_coin.R        # ✅ Main COINr workflow (working)
├── inst/
│   └── shiny/
│       └── app.R                  # Dashboard (future)
├── data-raw/                       # Source data files (CSV)
├── output/                         # Generated results
├── DESCRIPTION                     # 🔲 Package metadata
├── NAMESPACE                       # 🔲 Exports
└── README.md                       # ✅ This file
```

Legend: ✅ Complete | ⚠️ Partial | 🔲 To be created

**Status:** 94.7% complete (90/95 functions implemented)

------------------------------------------------------------------------

## COINr Migration Plan

### Stage 1: Foundation (Data + Helpers + Raw Indicators) ✅ COMPLETE

**Goal**: Create a clean data pipeline that outputs **raw indicator
values** (unnormalized)

**Files**:
- `01_data_loading.R` ✅ - Load all source data, standardize country names
- `02_helpers.R` ✅ - Pure utility functions (CV, GDP extraction, etc.)
- `03_indicators_trade.R` ✅ - Extract raw trade values from Comtrade
- `04_indicators_financial.R` ✅ - Extract raw financial/monetary values
- `05_indicators_labor.R` ✅ - Extract raw labor/mobility values
- `06_indicators_infrastructure.R` ⚠️ - 4 placeholder functions need external data
- `07_indicators_sustainability.R` ✅ - Extract raw sustainability values
- `08_indicators_convergence.R` ✅ - Extract raw convergence values

**Key Change**: Indicator functions return **raw values** (e.g.,
`inflation_rate = 2.3%`), NOT normalized scores. COINr will handle
normalization.

------------------------------------------------------------------------

### Stage 2: COINr Integration (Metadata + Coin Construction) ✅ COMPLETE

**Goal**: Build the coin object with proper metadata

**Files**:
- `09_coinr_metadata.R` ✅ - Build iMeta and validate against iData
- `10_normalization_config.R` ✅ - Normalization pipeline with custom strategies
- `analysis/build_gcceii_coin.R` ✅ - Full workflow with sensitivity analysis

**Deliverables**:

``` r
# iMeta structure (90 indicators + aggregates)
iMeta <- tibble(
  iCode = c("ind_51", "ind_52", ..., "Trade", "Financial", ..., "Index"),
  iName = c("Trade Intensity", "Services Share", ...),
  Level = c(1, 1, ..., 2, 2, ..., 4),
  Parent = c("Trade", "Trade", ..., "Index", "Index", ..., NA),
  Weight = c(1, 1, ..., 0.20, 0.20, ..., NA),
  Direction = c(1, 1, ..., NA, NA, ..., NA),
  Type = c("Indicator", "Indicator", ..., "Aggregate", "Aggregate", ..., "Aggregate")
)

# Coin construction
coin <- new_coin(iData, iMeta, level_names = c("Indicator", "Category", "Dimension", "Index"))
coin <- Normalise(coin, dset = "Raw", global_specs = list(f_n = "n_minmax"))
coin <- Aggregate(coin, dset = "Normalised", f_ag = "a_amean")
```

------------------------------------------------------------------------

### Stage 3: Dashboard + Export (Outputs) 🔲 IN PROGRESS

**Goal**: Generate all outputs from the coin object

**Completed**:
- Sensitivity analysis (enabled in build script)
- CSV export of results
- PNG visualization export
- RData workspace export

**Remaining**:
- Shiny dashboard
- Excel export with formatting
- DESCRIPTION/NAMESPACE for package installation

------------------------------------------------------------------------

## Indicator Framework

### Dimensions and Weights

| Dimension | Weight | Key Indicators |
|----|----|----|
| Trade Integration | 20% | Trade intensity, non-oil trade, services, BEC composition |
| Financial Integration | 20% | OCA readiness, banking, stock markets, FDI |
| Labor & Mobility | 20% | Labor mobility, students, tourism |
| Infrastructure | 20% | Aviation, energy, digital |
| Sustainability | 10% | Non-oil share, manufacturing, diversification |
| Convergence | 10% | Cross-dimension CV indicators |

### Special Handling: Convergence Indicators

Convergence indicators use **Coefficient of Variation (CV)** across
countries: - Same score for all countries in a given year - Lower CV =
higher convergence = higher score - Formula:
`convergence_score = 100 - CV`

These will be handled via custom aggregation in COINr.

------------------------------------------------------------------------

## Data Sources

### GCC-Stat Internal (CSV → future SDMX)

-   `DF_Common_Market_Tables.csv` - Common Market indicators
-   `DF_ES_NA.csv` - National Accounts
-   `DF_GEETS_TUR.csv` - Tourism
-   `DF_ES_CPI.csv` - CPI/Inflation
-   `DF_PSS_LAB.csv` - Labor Force
-   `DF_ES_MF.csv` - Monetary & Financial
-   `DF_GEETS_ENR.csv` - Energy
-   `DF_PSS_DEM_POP.csv` - Population

### External

-   `comtrade_data.rds` - UN Comtrade aggregate trade
-   `comtrade_data_hs.rds` - UN Comtrade HS-level trade
-   `GCC FDI flows.csv` - FDI data
-   `ICP_data.csv` - World Bank ICP

------------------------------------------------------------------------

## Usage

### Current (Development)

``` r
# Source the modules
source("R/01_data_loading.R")
source("R/02_helpers.R")

# Load data
data_list <- load_gcc_data(data_dir = "path/to/data")

# Extract raw indicators
raw_2023 <- extract_raw_indicators(data_list, year_filter = 2023)
```

### Future (Package)

``` r
library(gcceii)
library(COINr)

# Build coin for a year
coin_2023 <- build_gcceii_coin(year = 2023)

# Get results
results <- get_results(coin_2023, dset = "Aggregated")

# Sensitivity analysis
sa_results <- SA_estimate(coin_2023)
```

------------------------------------------------------------------------

## Development Notes

### Key Functions Migrated

| Original Function | New Location | Change |
|----|----|----|
| `load_gcc_data()` | `01_data_loading.R` | Added ISO3 codes, SDMX placeholders |
| `standardize_countries()` | `01_data_loading.R` | Added ISO3 codes |
| `get_gdp()` | `01_data_loading.R` | Enhanced validation |
| `calculate_cv()` | `02_helpers.R` | No change |
| `normalize_minmax()` | `02_helpers.R` | Kept for reference (COINr replaces) |
| `calculate_*()` | `03-07_indicators_*.R` | Return RAW values only |

### COINr Functions to Use

| COINr Function      | Purpose                              |
|---------------------|--------------------------------------|
| `new_coin()`        | Create coin object                   |
| `Normalise()`       | Min-max, rank, z-score normalization |
| `Aggregate()`       | Weighted averages, geometric means   |
| `get_data()`        | Extract processed data               |
| `get_results()`     | Get aggregated scores                |
| `SA_estimate()`     | Sensitivity analysis                 |
| `plot_corr()`       | Correlation matrices                 |
| `plot_bar()`        | Country comparison charts            |
| `export_to_excel()` | Professional Excel export            |

------------------------------------------------------------------------

## Claude Code Benefits

Using GitHub + Claude Code for this project enables:

1.  **Bulk refactoring** - Transform 25+ indicator functions to return
    raw values
2.  **Terminal-based testing** - Run R scripts and debug interactively
3.  **Package structure** - Generate DESCRIPTION, NAMESPACE, roxygen2
    docs
4.  **CI/CD setup** - GitHub Actions for automated testing
5.  **Version control** - Track methodology changes across development

------------------------------------------------------------------------

## Contact

Economic Statistics Department, GCC-Stat

------------------------------------------------------------------------

## License

Proprietary - GCC-Stat Internal Use
