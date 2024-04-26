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
