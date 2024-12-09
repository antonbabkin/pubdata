#' List data keys in the BDS collection
#'
#' @param pattern grep pattern to filter with
#'
#' @return Vector of keys that match pattern.
#' @export
#'
#' @examples
#' bds_ls("raw")
bds_ls <- function(pattern = ".") {
  meta_path <- system.file("extdata/bds/meta.yml", package = "pubdata")
  full_meta <- yaml::read_yaml(meta_path)
  all_keys <- names(full_meta$data)
  grep(pattern, all_keys, value = TRUE)
}


#' BDS metadata for a data object
#'
#' @param key String key of the specific data object
#'
#' @return Metadata as a list.
#' @export
#' @examples
#' # use as a list
#' bds_meta("ruc_2023")
#'
#' # print in a compact format
#' bds_meta("ruc_2023") |>
#'   yaml::as.yaml() |>
#'   cat()
bds_meta <- function(key) {
  meta_path <- system.file("extdata/bds/meta.yml", package = "pubdata")
  full_meta <- yaml::read_yaml(meta_path)

  if (!(key %in% names(full_meta$data))) {
    stop(key, " is not a valid data key")
  }

  glue_meta(key, full_meta$data[[key]])
}


#' BDS data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
#' @export
bds_get <- function(key) {
  meta <- bds_meta(key)
  path <- pubdata_path("bds", meta$path)

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

  raw <- bds_get(meta$depends)
  bds_read(raw, meta)

}


#' Read BDS table
bds_read <- function(raw, meta) {
  df <- readr::read_csv(raw, col_types = cols(
    year = "i",
    st = "c",
    cty = "c",
    firms = "i",
    estabs = "i",
    emp = "i",
    denom ="i",
    estabs_entry = "i",
    estabs_entry_rate = "d",
    estabs_exit = "i",
    estabs_exit_rate = "d",
    job_creation = "i",
    job_creation_births = "i",
    job_creation_continuers = "i",
    job_creation_rate_births = "d",
    job_creation_rate = "d",
    job_destruction = "i",
    job_destruction_deaths = "i",
    job_destruction_continuers = "i",
    job_destruction_rate_deaths = "d",
    job_destruction_rate = "d",
    net_job_creation = "i",
    net_job_creation_rate = "d",
    reallocation_rate = "d",
    firmdeath_firms = "i",
    firmdeath_estabs = "i",
    firmdeath_emp = "i"
  ))

  df
}

