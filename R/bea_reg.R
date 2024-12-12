# BEA Regional Economic Accounts

#' List data keys
#'
#' @param pattern grep pattern to filter with
#'
#' @return Vector of keys that match pattern.
#' @export
#'
#' @examples
#' bea_reg_ls("raw")
bea_reg_ls <- function(pattern = ".") {
  meta_path <- system.file("extdata/bea_reg/meta.yml", package = "pubdata")
  full_meta <- yaml::read_yaml(meta_path)
  all_keys <- names(full_meta$data)
  grep(pattern, all_keys, value = TRUE)
}


#' Metadata for a data object
#'
#' @param key String key of the specific data object
#'
#' @return Metadata as a list.
#' @export
#' @examples
#' # use as a list
#' bea_reg_meta("raw_2022_cagdp2")
#'
#' # print in a compact format
#' bea_reg_meta("raw_2022_cagdp2") |>
#'   yaml::as.yaml() |>
#'   cat()
#'
bea_reg_meta <- function(key) {
  meta_path <- system.file("extdata/bea_reg/meta.yml", package = "pubdata")
  full_meta <- yaml::read_yaml(meta_path)

  if (!(key %in% names(full_meta$data))) {
    stop(key, " is not a valid data key")
  }

  glue_meta(key, full_meta$data[[key]])
}


#' Data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
#' @export
bea_reg_get <- function(key) {
  meta <- bea_reg_meta(key)
  path <- pubdata_path("bea_reg", meta$path)

  if (file.exists(path)) {
    if (meta$type == "raw") {
      return(path)
    } else if (meta$type == "table") {
      logger::log_debug("{key} read from {path}")
      return(arrow::read_parquet(path))
    }
  }

  if (meta$type == "raw") {
    utils::download.file(meta$url, mkdir(path))
    stopifnot(file.exists(path))
    return(path)
  }

  raw <- bea_reg_get(meta$depends)
  unzipped_file <- utils::unzip(raw, meta$read$file, exdir = tempdir())
  on.exit(unlink(unzipped_file))
  logger::log_debug("unzipped to {unzipped_file}")

  df <- bea_reg_read(unzipped_file, meta)

  arrow::write_parquet(df, mkdir(path))
  logger::log_debug("{key} saved to {path}")
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

