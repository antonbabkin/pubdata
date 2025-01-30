

for (key in ls("naics")) {

  m <- NULL
  test_that(glue::glue("{key}: meta() gives no error"), {
    expect_no_error({ m <<- meta("naics", key, print = FALSE) })
  })

  if (m$type == "raw") {
    next
  }

  tab <- NULL
  test_that(glue::glue("{key}: get() gives no error"), {
    expect_no_error({ tab <<- get("naics", key) })
  })

  test_that(glue::glue("{key}: columns cosistent with schema"), {
    expect_equal(names(m$schema), names(tab))
  })
}
