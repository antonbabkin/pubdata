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


for (key in bea_io_ls("curr_sup*")) {
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
    expect_equal(total_supply, total_use, tolerance = 1e-7)
  })
}
