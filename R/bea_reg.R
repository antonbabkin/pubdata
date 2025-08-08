#' Data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
bea_reg_get <- function(key) {
  this_meta <- meta("bea_reg", key, print = FALSE)

  if (this_meta$type == "raw") {
    path <- pubdata_path("bea_reg", this_meta$path)
    utils::download.file(this_meta$url, mkdir(path))
    stopifnot(file.exists(path))
    return(path)
  }

  raw <- get("bea_reg", this_meta$depends)
  unzipped_file <- utils::unzip(raw, this_meta$read$file, exdir = tempdir())
  on.exit(unlink(unzipped_file))
  logger::log_debug("unzipped to {unzipped_file}")

  df <- bea_reg_read(unzipped_file, this_meta)
  df
}


#' Read table
bea_reg_read <- function(path, meta) {
  # list (new_name = old_name)
  col_renames <- meta$schema |>
    purrr::keep(\(x) "orig" %in% names(x)) |>
    purrr::map(\(x) x[["orig"]])
  # codes used for missing values
  missing_values <- c("(D)", "(NA)")

  df <- readr::read_csv(
    path,
    col_types = readr::cols(
      "Region" = "i",
      "LineCode" = "i",
      .default = "c"
    ),
    n_max = meta$read$rows
  ) |>
    dplyr::rename(!!!col_renames) |>
    # remove footnote symbols
    # TODO remove trailing "*" in geo_name such as "Prince of Wales-Outer Ketchikan Census Area, AK*"
    dplyr::mutate(
      geo_name = stringr::str_remove(geo_name, " \\*$"),
      ind_desc = stringr::str_remove(ind_desc, " 2/$"),
      ind_desc = stringr::str_remove(ind_desc, " 3/$")
    ) |>
    tidyr::pivot_longer("2001":meta$keys$year, names_to = "year") |>
    dplyr::mutate(
      year = as.integer(year),
      value_f = dplyr::if_else(value %in% missing_values, value, NA),
      value = as.double(dplyr::if_else(is.na(value_f), value, NA)),
      ind_code = dplyr::na_if(ind_code, "...")
    )

  df
}

