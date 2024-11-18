
#' Test if two vectors are approximately equal
#' Absolute and relative tolerance explicitly separated, unlike testthat::expect_equal()
#' Downside: will not report specific values that fail
expect_close <- function(x, y, rel_tol = 0.001, abs_tol = Inf) {
  abs_dif <- abs(x - y)
  rel_dif <- ifelse(x == 0, 0, abs_dif / abs((x + y) / 2))
  expect_equal(abs_dif <= abs_tol & rel_dif < rel_tol, rep(TRUE, length(x)), ignore_attr = "names")
}

test_that("invalid data key stops with error", {
  expect_error(bea_io_meta("invalid_data_key"), "invalid_data_key is not a valid data key")
})


test_that("naics lists expand correctly", {
  input <- c("1111", "1111, 1112", "1111-5", "1111-3, 211-4")
  output <- lapply(input, expand_naics_lists) |>
    as.character()
  expected <- c("1111", "1111,1112", "1111,1112,1113,1114,1115", "1111,1112,1113,211,212,213,214")
  expect_equal(output, expected)
})



for (key in bea_io_ls()) {

  m <- NULL
  test_that(glue::glue("{key}: meta() gives no error"), {
    expect_no_error({ m <<- bea_io_meta(key) })
  })

  if (m$type == "raw") {
    next
  }

  tab <- NULL
  test_that(glue::glue("{key}: get() gives no error"), {
    expect_no_error({ tab <<- bea_io_get(key) })
  })

  test_that(glue::glue("{key}: columns cosistent with schema"), {
    expect_equal(names(m$schema), names(tab))
  })

  if (!is.null(m$keys$table) && m$keys$table %in% c(
    "mak-bef", "mak-aft", "use-bef-pro", "use-bef-pur", "use-aft-pro", "use-aft-pur",
    "sup", "use", "imp-aft", "imp-bef"
    )) {
    mat <- tab %>%
      dplyr::filter(core_matrix) |>
      tidyr::pivot_wider(id_cols = "row_name", names_from = "col_name") |>
      tibble::column_to_rownames("row_name") |>
      as.matrix()

    test_that(glue::glue("{key}: core matrix row sums equal respective table totals"), {
      row_sum <- rowSums(mat, na.rm = TRUE)
      coln <- dplyr::case_when(
        m$keys$table %in% c("mak-bef", "mak-aft") ~ "Total Industry Output",
        m$keys$table == "sup" ~ "Total Commodity Output",
        m$keys$table %in% c("use-bef-pro", "use-bef-pur", "use-aft-pro", "use-aft-pur", "use", "imp-aft", "imp-bef") ~ "Total Intermediate"
      )
      row_tot <- tab |>
        dplyr::filter(col_name == coln) |>
        dplyr::pull(value, name = "row_name") |>
        tidyr::replace_na(0)
      row_tot <- row_tot[names(row_sum)]
      expect_close(row_sum, row_tot, abs_tol = 35, rel_tol = Inf)
    })

    if (!(m$keys$table %in% c("imp-bef", "imp-aft"))) {
      test_that(glue::glue("{key}: core matrix column sums equal respective table totals"), {
        col_sum <- colSums(mat, na.rm = TRUE)
        rown <- dplyr::case_when(
          m$keys$table %in% c("mak-bef", "mak-aft") ~ "Total Commodity Output",
          m$keys$table == "sup" ~ "Total industry supply",
          grepl("_su_use_det_", key) ~ "Total intermediate inputs",
          m$keys$table %in% c("use-bef-pro", "use-bef-pur", "use-aft-pro", "use-aft-pur", "use") ~ "Total Intermediate"
        )
        col_tot <- tab |>
          dplyr::filter(row_name == rown) |>
          dplyr::pull(value, name = "col_name") |>
          tidyr::replace_na(0)
        col_tot <- col_tot[names(col_sum)]
        expect_close(col_tot, col_sum, abs_tol = 35, rel_tol = Inf)
      })
    }
  }

  # Supply and Use tables commodity totals match
  if (!is.null(m$keys$table) && m$keys$table == "sup") {
    test_that(glue::glue("{key}: total supply equals total use"), {
      # supply table rows are all commodities + total
      total_supply <- tab |>
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
    })
  }

  # Make and Use tables commodity totals match
  if (!is.null(m$keys$table) && m$keys$table %in% c("mak-bef", "mak-aft")) {
    test_that(glue::glue("{key}: total make equals total use"), {
      # make table columns are commodities
      com_names <- tab |>
        dplyr::filter(core_matrix) |>
        dplyr::distinct(col_name) |>
        dplyr::pull()
      total_make <- tab |>
        dplyr::filter(row_name == "Total Commodity Output", col_name %in% com_names) |>
        dplyr::pull(value) |>
        tidyr::replace_na(0)
      # use key from make key, e.g. "2022_mu_mak-bef_sec_2021" -> "2022_mu_use-bef-pro_sec_2021"
      use_key <- sub("_mak-(...)_", "_use-\\1-pro_", key)
      tab2 <-bea_io_get(use_key)
      # use table rows are commodities
      com_names2 <- tab2 |>
        dplyr::filter(core_matrix) |>
        dplyr::distinct(row_name) |>
        dplyr::pull()
      total_use <- tab2 |>
        dplyr::filter(col_name == "Total Commodity Output", row_name %in% com_names2) |>
        dplyr::pull(value) %>%
        tidyr::replace_na(0)
      # absolute deviation less than small number - rounding error
      expect_close(total_make, total_use, abs_tol = 2, rel_tol = Inf)
    })
  }


  if (grepl("_naics", key)) {
    test_that(glue::glue("{key}: naics tables pass basic tests"), {
      d <- bea_io_get(key)
      # title never missing
      expect_equal(is.na(d$title), rep(FALSE, nrow(d)))
      # no naics code for non-detail rows
      x <- d[is.na(d$detail), ]$naics
      expect_equal(is.na(x), rep(TRUE, length(x)))
    })
  }
}




