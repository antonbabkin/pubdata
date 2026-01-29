


#' NASS data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
nass_get <- function(key) {
  key_meta <- meta("nass", key, print = FALSE)
  stopifnot(key_meta$type == "table")

  raw <- get("nass", key_meta$depends)
  suppression_flags <- c("(D)", "(X)", "(Z)", "(L)", "(H)")
  # most of the columns are character
  # year is converted to integer
  # value and cv_% are converted to double, and suppression flags are kept in separate columns
  y <- readr::read_tsv(raw, col_types = readr::cols(YEAR = "i", .default = "c")) %>%
    dplyr::rename_with(tolower) %>%
    dplyr::mutate(value_f = dplyr::if_else(value %in% suppression_flags, value, NA), .after = value) %>%
    dplyr::mutate(value = as.double(dplyr::if_else(is.na(value_f), stringr::str_replace_all(value, ",", ""), NA))) %>%
    dplyr::mutate(`cv_%_f` = dplyr::if_else(`cv_%` %in% suppression_flags, `cv_%`, NA), .after = `cv_%`) %>%
    dplyr::mutate(`cv_%` = as.double(dplyr::if_else(is.na(`cv_%_f`), stringr::str_replace_all(`cv_%`, ",", ""), NA)))

  y
}


