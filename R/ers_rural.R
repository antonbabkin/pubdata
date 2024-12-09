#' List data keys in the ERS Rurality collection
#'
#' @param pattern grep pattern to filter with
#'
#' @return Vector of keys that match pattern.
#' @export
#'
#' @examples
#' ers_rural_ls("raw")
ers_rural_ls <- function(pattern = ".") {
  meta_path <- system.file("extdata/ers_rural/meta.yml", package = "pubdata")
  full_meta <- yaml::read_yaml(meta_path)
  all_keys <- names(full_meta$data)
  grep(pattern, all_keys, value = TRUE)
}


#' ERS Rurality metadata for a data object
#'
#' @param key String key of the specific data object
#'
#' @return Metadata as a list.
#' @export
#' @examples
#' # use as a list
#' ers_rural_meta("ruc_2023")
#'
#' # print in a compact format
#' ers_rural_meta("ruc_2023") |>
#'   yaml::as.yaml() |>
#'   cat()
#'
ers_rural_meta <- function(key) {
  meta_path <- system.file("extdata/ers_rural/meta.yml", package = "pubdata")
  full_meta <- yaml::read_yaml(meta_path)

  if (!(key %in% names(full_meta$data))) {
    stop(key, " is not a valid data key")
  }

  glue_meta(key, full_meta$data[[key]])
}


#' ERS Rurality data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
#' @export
ers_rural_get <- function(key) {
  meta <- ers_rural_meta(key)
  path <- pubdata_path("ers_rural", meta$path)

  if (file.exists(path)) {
    if (meta$type == "raw") {
      return(path)
    } else if (meta$type == "table") {
      return(arrow::read_parquet(path))
    }
  }

  if (meta$type == "raw") {
    utils::download.file(meta$url, mkdir(path))
    stopifnot(file.exists(path))
    return(path)
  }

  raw <- ers_rural_get(meta$depends)
  ers_rural_read_ruc(raw, meta)

}


#' Read RUC table
ers_rural_read_ruc <- function(raw, meta) {
  year <- as.integer(meta$keys$year)
  if (year == 2003) {
    df <- readxl::read_excel(raw)
    names(df) <- names(meta$schema)
  } else if (year == 2023) {
    df <- readr::read_csv(raw, col_types = "c")
    names(df) <- tolower(names(df))
  }
  df
}

