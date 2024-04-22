


#' @importFrom magrittr "%>%"



pubdata_path <- function(...) {
  pubdata_dir <- Sys.getenv("PUBDATA_CACHE_DIR")
  if (pubdata_dir == "") stop("PUBDATA_CACHE_DIR environmental variable is not set.")
  file.path(pubdata_dir, ...)
}


mkdir <- function(p) {
  d <- dirname(p)
  if (!dir.exists(d)) {
    logger::log_debug("Creating directory {d}")
    dir.create(d, recursive = TRUE)
  }
  return(p)
}
