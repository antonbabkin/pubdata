test_that("glue_meta works", {
  preglue <- list(
    mask = "test_{year}_{result}",
    desc = "Test result is {result} in year {year}",
    vector = c("raw_{year}", "literal"),
    nested = list(
      result = "result is {result}",
      year = "year is {year}",
      numbers = 1:4,
      pi = 3.14
    )
  )

  postglue <- list(
    mask = "test_{year}_{result}",
    desc = "Test result is pass in year 2024",
    vector = c("raw_2024", "literal"),
    nested = list(
      result = "result is pass",
      year = "year is 2024",
      numbers = 1:4,
      pi = 3.14
    ),
    keys = list(
      year = "2024",
      result = "pass"
    )
  )

  expect_equal(glue_meta("test_2024_pass", preglue), postglue)
})
