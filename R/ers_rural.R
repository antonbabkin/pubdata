#' ERS Rurality data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
ers_rural_get <- function(key) {
  this_meta <- meta("ers_rural", key, print = FALSE)

  if (this_meta$type == "raw") {
    path <- pubdata_path("ers_rural", this_meta$path)
    download_file(this_meta$url, path)
    return(path)
  }

  raw <- get("ers_rural", this_meta$depends)
  types <- purrr::map(this_meta$schema, \(x) x$type)

  # read RUC tables
  if (key == "ruc_2003") {
    # read_excel() uses different type names and does not distinguish integer and double
    excel_types <- purrr::map_chr(types, \(x) switch(
      x,
      logical = "logical",
      character = "text",
      integer = "numeric",
      double = "numeric"
    ))
    df <- readxl::read_excel(raw, col_names = names(types), skip = 1, col_types = excel_types)
    for (col in names(df)) {
      if (types[[col]] == "integer") {
        df[[col]] <- as.integer(df[[col]])
      }
    }
  } else if (key == "ruc_2023") {
    df <- readr::read_csv(raw, col_names = names(types), col_types = types, skip = 1)
  }

  df
}



