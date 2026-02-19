


#' CHRR data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
chrr_get <- function(key) {
  key_meta <- meta("chrr", key, print = FALSE)
  stopifnot(key_meta$type == "table")

  raw <- get("chrr", key_meta$depends)

  # 2012 has duplicate line 1 descriptions, read 2 lines without column names
  var_desc <- readr::read_csv(raw, col_names = FALSE, col_types = list(.default = "c"), n_max = 2) |>
    tibble::add_column(col = c("description", "variable"), .before = 1) |>
    tidyr::pivot_longer(!col) |>
    tidyr::pivot_wider(id_cols = "name", names_from = "col") |>
    dplyr::select(variable, description)

  types <- list(
    statecode = "c",
    countycode = "c",
    fipscode = "c",
    state = "c",
    county = "c",
    year = "i",
    .default = "d")
  if (key_meta$keys$year %in% c("2024", "2025")) {
    types$county_clustered <- "i"
  } else {
    types$county_ranked <- "i"
  }

  y <- readr::read_csv(raw, skip = 1, col_types = types) |>
    tidyr::pivot_longer(starts_with("v"), names_to = "variable") |>
    dplyr::left_join(var_desc, by = "variable") |>
    dplyr::relocate(value, .after = description)
  y
}
