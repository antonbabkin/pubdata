


#' QCEW data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
qcew_get <- function(key) {
  this_meta <- meta("qcew", key, print = FALSE)

  if (this_meta$type == "raw") {
    path <- pubdata_path("qcew", this_meta$path)
    download_file(this_meta$url, path)
    return(path)
  }


  raw <- get("qcew", this_meta$depends)
  types <- purrr::map(this_meta$schema, \(x) x$type)
  if (stringr::str_detect(key, "^sic_ann_[:digit:]{4}$")) {
    y <- readr::read_csv(raw, col_types = types, na = c("", "NA", "Infinity"))
  } else {
    y <- readr::read_csv(raw, col_types = types)
  }
  y
}


