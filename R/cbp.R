
#' CBP data table
#'
#' @param key Data table key.
#'
#' @return Tidy table or path to raw file.
cbp_get <- function(key) {
  key_meta <- meta("cbp", key, print = FALSE)
  stopifnot(key_meta$type == "table")

  raw <- get("cbp", key_meta$depends)
  types <- purrr::map(key_meta$schema, \(x) x$type)

  df <- readr::read_csv(raw, col_types = types)
  df
}


