# =============================================================================
# GCC EII - FUSION REGISTRY CONFIGURATION
# =============================================================================
#
# Connection settings for the MARSA Dissemination Warehouse (PostgreSQL).
# Source this file before using R/11_fusion_registry.R.
#
# CREDENTIAL SETUP:
# -----------------
# All connection parameters can be overridden via environment variables
# (prefix: MARSA_). The password MUST be set via environment variable.
# NEVER hardcode the password in this file.
#
# Option 1 (recommended): Add to your .Renviron file
#   Create or edit the file:  ~/.Renviron  (user-level)
#   Or for this project only: gcc_eii_beta/.Renviron
#
#   Add these lines:
#     MARSA_PASSWORD=your_actual_password_here
#     MARSA_USER=your_username
#
#   Then restart R (the .Renviron is read at startup).
#
# Option 2: Set in your R session before sourcing this file
#   Sys.setenv(MARSA_PASSWORD = "your_actual_password_here")
#
# Option 3: Set in your system environment (Linux/Mac)
#   export MARSA_PASSWORD="your_actual_password_here"
#
# To verify it's set:
#   Sys.getenv("MARSA_PASSWORD")   # should NOT be ""
#
# Available environment variables (all optional except MARSA_PASSWORD):
#   MARSA_HOST     - Database host (default: warehouse.marsa.gccstat.org)
#   MARSA_PORT     - Database port (default: 5434)
#   MARSA_DB       - Database name (default: prod-diss-warehouse)
#   MARSA_USER     - Database user (default: k.manninen)
#   MARSA_PASSWORD - Database password (REQUIRED, no default)
#
# =============================================================================

# --- Database Connection -----------------------------------------------------

db_config <- list(
  host     = Sys.getenv("MARSA_HOST",     "warehouse.marsa.gccstat.org"),
  port     = as.integer(Sys.getenv("MARSA_PORT", "5434")),
  dbname   = Sys.getenv("MARSA_DB",       "prod-diss-warehouse"),
  user     = Sys.getenv("MARSA_USER",     "k.manninen"),
  password = Sys.getenv("MARSA_PASSWORD", "")
)

# Common Market lives on the "final" warehouse instance (port 5433)
# Uses its own env vars so credentials can differ from the diss warehouse.
# Falls back to the diss warehouse values if not set.
db_config_final <- list(
  host     = Sys.getenv("MARSA_HOST_FINAL",     Sys.getenv("MARSA_HOST",     "warehouse.marsa.gccstat.org")),
  port     = as.integer(Sys.getenv("MARSA_PORT_FINAL", "5433")),
  dbname   = Sys.getenv("MARSA_DB_FINAL",       "prod-final-warehouse"),
  user     = Sys.getenv("MARSA_USER_FINAL",     Sys.getenv("MARSA_USER",     "k.manninen")),
  password = Sys.getenv("MARSA_PASSWORD_FINAL", Sys.getenv("MARSA_PASSWORD", ""))
)

# --- Extraction Settings -----------------------------------------------------

extraction_config <- list(
  start_year = 2010,
  end_year   = 2024,

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
# Naming pattern: {DATAFLOW_ID}_V{VERSION}_{AGENCY}_DF

dataflow_tables <- list(
  national_accounts = "DF_ES_NA_V1.0_GCCSTAT.ES_DF",
  cpi               = "DF_ES_CPI_V1.0_GCCSTAT.ES_DF",
  monetary          = "DF_ES_MF_V1.0_GCCSTAT.ES_DF",
  labor             = "DF_PSS_LAB_V1.0_GCCSTAT.PSS_DF",
  population        = "DF_PSS_DEM_POP_V1.0_GCCSTAT.PSS_DF",
  energy            = "DF_GEETS_ENR_V1.0_GCCSTAT.GEETS_DF",
  tourism           = "DF_GEETS_TUR_V1.0_GCCSTAT.GEETS_DF",
  common_market     = "DF_Common_Market_Tables_V1.0_GCCSTAT.CM_DF"
)

# --- Column Mappings ---------------------------------------------------------
# Maps internal field names to actual PostgreSQL column names in each table.
# English label columns end with "_en"; code columns use codelist prefixes.
# Used by R/11_fusion_registry.R to build SQL SELECT statements.

column_mappings <- list(
  national_accounts = list(
    country    = "COUNTRY_en",
    frequency  = "FREQUENCY_en",
    price_type = "PRICES TYPES_en",
    indicator  = "INDICATOR_en",
    unit       = "UNIT_en"
  ),

  cpi = list(
    country   = "COUNTRY_en",
    frequency = "FREQUENCY_en",
    indicator = "INDICATOR_en",
    unit      = "UNIT_en"
  ),

  monetary = list(
    country   = "COUNTRY_en",
    frequency = "FREQUENCY_en",
    indicator = "INDICATOR_en",
    unit      = "UNIT_en"
  ),

  labor = list(
    country        = "COUNTRY_en",
    indicator      = "INDICATOR_en",
    labour_aspect  = "LABOUR_ASPECT_en",
    nationality    = "NATIONALITY_en",
    unit           = "UNIT_en",
    gender         = "GENDER_en",
    frequency      = "FREQUENCY_en"
  ),

  population = list(
    country     = "COUNTRY_en",
    nationality = "NATIONALITY_en",
    gender      = "GENDER_en",
    age         = "AGE_en",
    frequency   = "FREQUENCY_en"
  ),

  energy = list(
    country   = "COUNTRY_en",
    product   = "PRODUCT_en",
    flow      = "FLOW_en",
    indicator = "INDICATOR_en",
    unit      = "UNIT_en"
  ),

  tourism = list(
    country   = "COUNTRY_en",
    partner   = "PARTNER_COUNTRY_en",
    indicator = "INDICATOR_en",
    frequency = "FREQUENCY_en"
  ),

  common_market = list(
    country_code   = "CL_COM_AREA_GEO_FLAT",
    country        = "COUNTRY_en",
    track          = "CL_COM_TTRACK",
    track_name     = "TRACK_en",
    indicator_code = "CL_COM_INDICATOR",
    indicator      = "INDICATOR_en",
    citizen_code   = "CL_COM_CITIZEN",
    citizen        = "CITIZEN_en",
    sex_code       = "CL_COM_SEX",
    sex            = "SEX_en"
  )
)

# --- Validation --------------------------------------------------------------

message("Fusion Registry config loaded:")
message("  Host: ", db_config$host, ":", db_config$port)
message("  Database (diss): ", db_config$dbname)
message("  Database (final): ", db_config_final$dbname)
message("  User: ", db_config$user)
message("  Password: ", if (nchar(db_config$password) > 0) "SET" else "NOT SET")
message("  Year range: ", extraction_config$start_year, "-",
        extraction_config$end_year)
message("  Dataflows: ", paste(names(dataflow_tables), collapse = ", "))

if (nchar(db_config$password) == 0) {
  warning(
    "MARSA_PASSWORD is not set!\n",
    "  Add to .Renviron:  MARSA_PASSWORD=your_password\n",
    "  Or run:  Sys.setenv(MARSA_PASSWORD = 'your_password')"
  )
}
