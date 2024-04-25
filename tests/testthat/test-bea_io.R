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
