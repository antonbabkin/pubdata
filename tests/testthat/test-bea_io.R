
#' Test if two vectors are approximately equal
#' Absolute and relative tolerance explicitly separated, unlike testthat::expect_equal()
#' Downside: will not report specific values that fail
expect_close <- function(x, y, rel_tol = 0.001, abs_tol = Inf) {
  abs_dif <- abs(x - y)
  rel_dif <- ifelse(x == 0, 0, abs_dif / abs((x + y) / 2))
  expect_equal(abs_dif <= abs_tol & rel_dif < rel_tol, rep(TRUE, length(x)))
}

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
    expect_close(total_supply, total_use, abs_tol = 2, rel_tol = Inf)
    # expect_equal is more informative, but can not set absolute tolerance
    # expect_equal(total_supply, total_use, tolerance = 1e-5)
  })
}


for (key in bea_io_ls("_sup_")) {
  test_that(glue::glue("core matrix row and column sums equal respective table totals in {key}"), {
    tab <- bea_io_get(key)

    # commodity (row) totals
    com_tot <- tab |>
      dplyr::filter(col_name == "Total Commodity Output") |>
      dplyr::select(row_code, tot = value)
    com <- tab |>
      dplyr::filter(core_matrix) |>
      dplyr::summarize(sum = sum(value, na.rm = TRUE), .by = "row_code") |>
      dplyr::left_join(com_tot, by = "row_code") |>
      tidyr::replace_na(list(tot = 0, sum = 0))
    # x <- dplyr::mutate(com, abs_dif = abs(sum - tot), rel_dif = ifelse(tot == 0, 0, abs_dif / tot))
    expect_close(com$sum, com$tot, abs_tol = 12, rel_tol = Inf)

    # industry (column) totals
    ind_tot <- tab |>
      dplyr::filter(row_name == "Total industry supply") |>
      dplyr::select(col_code, tot = value)
    ind <- tab |>
      dplyr::filter(core_matrix) |>
      dplyr::summarize(sum = sum(value, na.rm = TRUE), .by = "col_code") |>
      dplyr::left_join(ind_tot, by = "col_code") |>
      tidyr::replace_na(list(tot = 0, sum = 0))
    # x <- dplyr::mutate(ind, abs_dif = abs(sum - tot), rel_dif = ifelse(tot == 0, 0, abs_dif / tot))
    expect_close(ind$sum, ind$tot, abs_tol = 10, rel_tol = Inf)
  })
}


test_that("naics lists expand correctly", {
  input <- c("1111", "1111, 1112", "1111-5", "1111-3, 211-4")
  output <- lapply(input, expand_naics_lists) |>
    as.character()
  expected <- c("1111", "1111,1112", "1111,1112,1113,1114,1115", "1111,1112,1113,211,212,213,214")
  expect_equal(output, expected)
})


for (key in bea_io_ls("_naics")) {
  test_that(glue::glue("naics tables pass basic tests in {key}"), {
    d <- bea_io_get(key)
    # title never missing
    expect_equal(is.na(d$title), rep(FALSE, nrow(d)))
    # no naics code for non-detail rows
    x <- d[is.na(d$detail), ]$naics
    expect_equal(is.na(x), rep(TRUE, length(x)))
  })
}

