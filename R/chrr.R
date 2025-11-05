


#' CHRR data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
chrr_get <- function(key) {
  this_meta <- meta("chrr", key, print = FALSE)

  if (this_meta$type == "raw") {
    path <- pubdata_path("chrr", this_meta$path)
    download_file(this_meta$url, path)
    return(path)
  }

  raw <- get("chrr", this_meta$depends)
  types <- purrr::map(this_meta$schema, \(x) x$type)
  y <- readr::read_csv(raw, col_types = types)
  y
}
