#' ERS Rurality data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
ers_rural_get <- function(key) {
  this_meta <- meta("ers_rural", key, print = FALSE)

  if (this_meta$type == "raw") {
    path <- pubdata_path("ers_rural", this_meta$path)
    utils::download.file(this_meta$url, mkdir(path))
    stopifnot(file.exists(path))
    return(path)
  }

  raw <- get("ers_rural", this_meta$depends)
  ers_rural_read_ruc(raw, this_meta)

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

