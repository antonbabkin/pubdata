
#' BDS data table
#'
#' @param key Data table key.
#'
#' @return Tidy table or path to raw file.
bds_get <- function(key) {
  key_meta <- meta("bds", key, print = FALSE)
  stopifnot(key_meta$type == "table")

  raw <- get("bds", key_meta$depends)
  # read everything as character to preserve suppression flags, then convert types according to the schema
  df <- readr::read_csv(raw, col_types = list(.default = "c"))
  suppression_flags <- c("D", "S", "X", "N")
  for (col in names(df)) {
    # create a flag column if it exist in the scmema
    flag_col <- paste0(col, "_f")
    if (!is.null(key_meta$schema[[flag_col]])) {
      df <- df %>%
        dplyr::mutate(
          !!flag_col := dplyr::if_else(!!dplyr::sym(col) %in% suppression_flags, !!dplyr::sym(col), NA), 
          .after = !!dplyr::sym(col)
        )
    }
    # convert numeric cols, dropping non-numeric values already captured in the flag columns
    if (key_meta$schema[[col]]$type == "integer") {
      df[[col]] <- readr::parse_integer(df[[col]], na = suppression_flags)
    } else if (key_meta$schema[[col]]$type == "double") {
      df[[col]] <- readr::parse_double(df[[col]], na = suppression_flags)
    }
  }

  df
}


