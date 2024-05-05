test_that("invalid data key stops with error", {
  expect_error(bea_io_meta("invalid_data_key"), "invalid_data_key is not a valid data key")
})


test_that("meta() calls produce no errors for all data keys", {
  for (key in bea_io_ls()) {
    expect_no_error(bea_io_meta(key))
  }
})


test_that("get() calls produce no errors for all data keys", {
  for (key in bea_io_ls()) {
    expect_no_error(bea_io_get(key))
  }
})


test_that("every table column is in schema", {
  for (key in bea_io_ls()) {
    m <- bea_io_meta(key)
    if (m$type == "raw") next
    t <- bea_io_get(key)
    expect_equal(names(m$schema), names(t))
  }
})


for (key in bea_io_ls("_sup_")) {
  test_that(glue::glue("total supply equals total use in {key}"), {
    # supply table rows are all commodities + total
    total_supply <- bea_io_get(key) |>
      dplyr::filter(col_name == "Total product supply (purchaser prices)") |>
      dplyr::pull(value) |>
      tidyr::replace_na(0)
    # use table rows are all commodities, total and extras
    total_use <- bea_io_get(sub("_sup_", "_use_", key)) |>
      dplyr::filter(col_name == "Total use of products") |>
      dplyr::pull(value) %>%
      `[`(1:length(total_supply)) |> # only keep commodities and total, ignore extras
      tidyr::replace_na(0)
    # absolute deviation less than small number - rounding error
    expect_true(all(abs(total_supply - total_use) <= 2))
    # expect_equal is more informative, but can not set absolute tolerance
    # expect_equal(total_supply, total_use, tolerance = 1e-5)
  })
}
