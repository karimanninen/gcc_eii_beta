# Tests for COINr metadata functions (09_coinr_metadata.R)

test_that("build_iMeta returns valid structure", {
  iMeta <- build_iMeta(version = "poc")

  # Check required columns exist
  required_cols <- c("iCode", "iName", "Level", "Parent", "Type", "Direction", "Weight")
  expect_true(all(required_cols %in% names(iMeta)))

  # Check it's a tibble/data.frame
expect_true(is.data.frame(iMeta))

  # Check there are indicators and aggregates
  expect_true(any(iMeta$Type == "Indicator"))
  expect_true(any(iMeta$Type == "Aggregate"))
})

test_that("build_iMeta includes all dimensions", {
  iMeta <- build_iMeta(version = "poc")

  dimensions <- c("Trade", "Financial", "Labor", "Infrastructure",
                  "Sustainability", "Convergence")

  # All dimensions should be present
  expect_true(all(dimensions %in% iMeta$iCode))
})

test_that("validate_iMeta catches missing columns", {
  # Invalid iMeta missing required columns
  invalid_iMeta <- data.frame(
    iCode = c("ind_1", "ind_2"),
    iName = c("Indicator 1", "Indicator 2")
  )

  expect_error(validate_iMeta(invalid_iMeta))
})

test_that("get_indicator_code_mapping returns named list", {
  mapping <- get_indicator_code_mapping()

  expect_type(mapping, "list")
  expect_true(length(mapping) > 0)

  # All values should be character
  expect_true(all(sapply(mapping, is.character)))
})

test_that("dimension weights sum to 1", {
  weights <- get_dimension_weights()

  # Weights should sum to 1 (or 100 if percentages)
  weight_sum <- sum(unlist(weights))
  expect_true(weight_sum == 1 || weight_sum == 100)
})
