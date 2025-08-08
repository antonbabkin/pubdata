

for (key in ls("bea_fa")) {

  m <- NULL
  test_that(glue::glue("{key}: meta() gives no error"), {
    expect_no_error({ m <<- meta("bea_fa", key, print = FALSE) })
  })

  if (m$type == "raw") {
    next
  }

  tab <- NULL
  test_that(glue::glue("{key}: get() gives no error"), {
    expect_no_error({ tab <<- get("bea_fa", key) })
  })

  test_that(glue::glue("{key}: columns cosistent with schema"), {
    expect_equal(names(m$schema), names(tab))
  })
}
