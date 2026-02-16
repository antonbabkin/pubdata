


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

  # CHRR data has a human-readable header in line 1 followed by variable name in line 2
  # line 1 are already captured in YAML schema `desc`, so we skip line 1
  y <- readr::read_csv(raw, skip = 1, show_col_types = FALSE)
  colnames(y) <- trimws(colnames(y))
  types <- purrr::map(this_meta$schema, \(x) x$type)
  y
}
