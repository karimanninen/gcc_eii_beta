# =============================================================================
# GCC EII - FUSION REGISTRY CONFIGURATION
# =============================================================================
#
# Connection settings for the MARSA Dissemination Warehouse (PostgreSQL).
# Source this file before using R/11_fusion_registry.R.
#
# CREDENTIAL SETUP:
# -----------------
# The database password is read from the environment variable
# GCCSTAT_DB_PASSWORD. NEVER hardcode the password in this file.
#
# Option 1 (recommended): Add to your .Renviron file
#   Create or edit the file:  ~/.Renviron  (user-level)
#   Or for this project only: gcc_eii_beta/.Renviron
#
#   Add this line:
#     GCCSTAT_DB_PASSWORD=your_actual_password_here
#
#   Then restart R (the .Renviron is read at startup).
#
# Option 2: Set in your R session before sourcing this file
#   Sys.setenv(GCCSTAT_DB_PASSWORD = "your_actual_password_here")
#
# Option 3: Set in your system environment (Linux/Mac)
#   export GCCSTAT_DB_PASSWORD="your_actual_password_here"
#
# To verify it's set:
#   Sys.getenv("GCCSTAT_DB_PASSWORD")   # should NOT be ""
#
# =============================================================================

# --- Database Connection -----------------------------------------------------

db_config <- list(
  host     = "warehouse.marsa.gccstat.org",
  port     = 5432,
  dbname   = "dissemination",
  user     = "readonlyuser",
  password = Sys.getenv("GCCSTAT_DB_PASSWORD", "")
)

# --- Extraction Settings -----------------------------------------------------

extraction_config <- list(
  start_year = 2015,
  end_year   = 2023,

  # Country name mapping: Fusion labels -> GCC standard names
  # Used by dplyr::recode() via !!!  (splice operator)
  country_standardize = c(
    "United Arab Emirates"     = "UAE",
    "The United Arab Emirates" = "UAE",
    "Emirates"                 = "UAE",
    "Kingdom of Saudi Arabia"  = "Saudi Arabia",
    "Kingdom of Bahrain"       = "Bahrain",
    "State of Kuwait"          = "Kuwait",
    "Sultanate of Oman"        = "Oman",
    "State of Qatar"           = "Qatar"
  )
)

# --- Dataflow Table Mappings -------------------------------------------------
# Maps internal dataset names to actual table/view names in the MARSA warehouse.
# These are the dissemination views created by the Fusion Registry.

dataflow_tables <- list(
  national_accounts = "DF_ES_NA",
  cpi               = "DF_ES_CPI",
  monetary          = "DF_ES_MF",
  labor             = "DF_PSS_LAB",
  population        = "DF_PSS_DEM_POP",
  energy            = "DF_GEETS_ENR",
  tourism           = "DF_GEETS_TUR",
  common_market     = "DF_Common_Market_Tables"
)

# --- Validation --------------------------------------------------------------

message("Fusion Registry config loaded:")
message("  Host: ", db_config$host, ":", db_config$port)
message("  Database: ", db_config$dbname)
message("  User: ", db_config$user)
message("  Password: ", if (nchar(db_config$password) > 0) "SET" else "NOT SET")
message("  Year range: ", extraction_config$start_year, "-",
        extraction_config$end_year)
message("  Dataflows: ", paste(names(dataflow_tables), collapse = ", "))

if (nchar(db_config$password) == 0) {
  warning(
    "GCCSTAT_DB_PASSWORD is not set!\n",
    "  Add to .Renviron:  GCCSTAT_DB_PASSWORD=your_password\n",
    "  Or run:  Sys.setenv(GCCSTAT_DB_PASSWORD = 'your_password')"
  )
}
