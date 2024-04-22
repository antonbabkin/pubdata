

#' I-O Tables metadata
#'
#' @param key String key of the specific data object, leave NULL to get all metadata.
#'
#' @return List of metadata.
#' @export
bea_io_meta <- function(key = NULL) {
  meta_path <- system.file("extdata/bea_io/meta.yaml", package = "pubdata")
  full_meta <- yaml::read_yaml(meta_path)
  if (is.null(key)) return(full_meta)

  key_meta <- full_meta$data[[key]]
  if (is.null(key_meta$mask)) return(key_meta)

  keys <- unglue::unglue(key, key_meta$mask)[[1]]
  for (x in names(key_meta)) {
    if (x %in% c("mask", "read")) next
    key_meta[[x]] <- glue::glue_data(keys, key_meta[[x]]) |>
      as.character()
  }
  key_meta$keys <- as.list(keys)
  key_meta
}



#' Title
#'
#' @param key Table key.
#'
#' @return Tidy table.
#' @export
bea_io_get <- function(key) {
  meta <- bea_io_meta(key)
  path <- pubdata_path("bea_io", meta$path)

  if (file.exists(path)) {
    if (meta$type == "raw") {
      return(path)
    } else if (meta$type == "table") {
      return(arrow::read_parquet(path))
    }
  }

  if (meta$type == "raw") {
    utils::download.file(meta$url, mkdir(path))
    stopifnot(file.exists(path))
    return(path)
  }

  raw <- bea_io_get(meta$depends)
  unzipped_spreadsheet <- utils::unzip(raw, meta$read$file, exdir = tempdir())
  on.exit(unlink(unzipped_spreadsheet))
  logger::log_debug("unzipped to {unzipped_spreadsheet}")
  x <- bea_io_read_table(unzipped_spreadsheet, meta)

  x

}



bea_io_read_table <- function(path, meta) {

  # read data section of sheet as text
  x_wide <- readxl::read_excel(
    path, sheet = meta$keys$year, col_types = "text", skip = meta$read$skip,
    n_max = meta$read$rows, .name_repair = "unique_quiet")
  colnames(x_wide)[1:2] <- c("row_code", "row_name")

  # rows and columns of the core matrix
  mat_rows <- x_wide$row_code[1 + (1:meta$read$matrix[1])]
  mat_cols <- colnames(x_wide)[2 + (1:meta$read$matrix[2])]

  # pivot columns to rows
  x_long <- tidyr::pivot_longer(x_wide, !c(row_code, row_name), names_to = "col_code")

  # column code and name table
  col_names <- x_long |>
    dplyr::filter(row_name == "Name") |>
    dplyr::rename(col_name = value) |>
    dplyr::select(col_code, col_name)

  # add column names, change value type to numeric, add core matrix indicator
  x <- x_long |>
    dplyr::filter(row_name != "Name") |>
    dplyr::left_join(col_names, "col_code") |>
    dplyr::relocate(row_code, row_name, col_code, col_name, value) |>
    dplyr::mutate(
      value = as.numeric(dplyr::if_else(value == "...", NA, value)),
      core_matrix = (row_code %in% mat_rows) & (col_code %in% mat_cols)
    )

  x
}


#' Title
#'
#' @param data Tidy table.
#' @param core Core matrix or full table.
#' @param labels "code" or "name"
#'
#' @return Wide table.
#' @export
bea_io_pivot_wider <- function(data, core = FALSE, labels = c("code", "name")) {
  labels <- match.arg(labels)
  if (core) {
    data <- dplyr::filter(data, core_matrix)
  }
  if (labels == "code") {
    data <- data |>
      dplyr::select(row_code, col_code, value) |>
      tidyr::pivot_wider(id_cols = "row_code", names_from = "col_code")
  } else {
    data <- data |>
      dplyr::select(row_name, col_name, value) |>
      tidyr::pivot_wider(id_cols = "row_name", names_from = "col_name")
  }
  data
}

