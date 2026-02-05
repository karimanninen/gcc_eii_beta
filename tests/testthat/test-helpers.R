# Tests for helper functions (02_helpers.R)

test_that("calculate_cv returns correct coefficient of variation", {
  # Test with known values
  values <- c(10, 20, 30, 40, 50)
  expected_cv <- (sd(values) / mean(values)) * 100

  expect_equal(calculate_cv(values), expected_cv)
})

test_that("calculate_cv handles NA values", {
  values_with_na <- c(10, 20, NA, 40, 50)

  # Should handle NA by removing them

  result <- calculate_cv(values_with_na)
  expect_true(!is.na(result))
})

test_that("calculate_cv returns NA for empty or all-NA input", {
  expect_true(is.na(calculate_cv(c())))
  expect_true(is.na(calculate_cv(c(NA, NA, NA))))
})

test_that("normalize_minmax scales values to 0-100", {
  values <- c(10, 20, 30, 40, 50)
  normalized <- normalize_minmax(values)

  expect_equal(min(normalized), 0)
  expect_equal(max(normalized), 100)
  expect_length(normalized, length(values))
})

test_that("cv_to_convergence_score inverts CV correctly", {
  # Low CV = high convergence
  expect_equal(cv_to_convergence_score(10), 90)
  expect_equal(cv_to_convergence_score(0), 100)


  # CV > 100 should be capped at 0
expect_equal(cv_to_convergence_score(150), 0)
})

test_that("check_gcc_complete identifies missing countries", {
  gcc_countries <- c("Bahrain", "Kuwait", "Oman", "Qatar", "Saudi Arabia", "UAE")

  # Complete set
  complete_df <- data.frame(country = gcc_countries)
  expect_true(check_gcc_complete(complete_df$country))

  # Missing one country
  incomplete_df <- data.frame(country = gcc_countries[-1])
  expect_false(check_gcc_complete(incomplete_df$country))
})
