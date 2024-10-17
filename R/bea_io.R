#' List data keys in the BEA I-O collection
#'
#' @param pattern grep pattern to filter with
#'
#' @return Vector of keys that match pattern.
#' @export
#'
#' @examples
#' bea_io_ls("raw")
bea_io_ls <- function(pattern = ".") {
  meta_path <- system.file("extdata/bea_io/meta.yml", package = "pubdata")
  full_meta <- yaml::read_yaml(meta_path)
  all_keys <- names(full_meta$data)
  grep(pattern, all_keys, value = TRUE)
}


#' BEA I-O Tables metadata for a data object
#'
#' @param key String key of the specific data object
#'
#' @return Metadata as a list.
#' @export
#' @examples
#' # use as a list
#' bea_io_meta("2023_use_det_2017")
#'
#' # print in a compact format
#' bea_io_meta("2023_use_det_2017") |>
#'   yaml::as.yaml() |>
#'   cat()
#'
bea_io_meta <- function(key) {
  meta_path <- system.file("extdata/bea_io/meta.yml", package = "pubdata")
  full_meta <- yaml::read_yaml(meta_path)

  if (!(key %in% names(full_meta$data))) {
    stop(key, " is not a valid data key")
  }

  glue_meta(key, full_meta$data[[key]])
}



#' BEA I-O Tables data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
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
  if (grepl("_(sup|use)_(sec|sum|det)_", key)) {
    return(bea_io_read_table(unzipped_spreadsheet, meta))
  } else if (grepl("_naics", key)) {
    return(bea_io_read_naics(unzipped_spreadsheet, meta))
  }

}

#' Read and tidy supply/use table
#' @param path Path to unzipped spreadsheet
#' @param meta Metadata list as returned by bea_io_meta("key")
bea_io_read_table <- function(path, meta) {

  # read data section of sheet as text
  x_wide <- readxl::read_excel(
    path, sheet = meta$keys$year, col_names = FALSE, col_types = "text",
    skip = meta$read$skip, n_max = meta$read$rows, .name_repair = "unique_quiet")

  # remove first two rows from table and use them as column names
  # det table header is (name, code), sec and sum are (code, name)
  code_name_idx <- if (meta$keys$level == "det") 2:1 else 1:2
  col_names <- x_wide[code_name_idx, -(1:2)] |>
    t() |>
    magrittr::set_colnames(c("col_code", "col_name")) |>
    as.data.frame()
  x_wide <- x_wide[-(1:2), ]
  colnames(x_wide) <- c("row_code", "row_name", col_names$col_code)

  # rows and columns of the core matrix
  core_rows <- x_wide$row_code[1:meta$read$matrix[1]]
  core_cols <- col_names$col_code[1:meta$read$matrix[2]]

  # pivot columns to rows
  x_long <- tidyr::pivot_longer(x_wide, !c(row_code, row_name), names_to = "col_code")

  # add column names, change value type to numeric, add core matrix indicator
  x <- x_long |>
    dplyr::left_join(col_names, "col_code") |>
    dplyr::relocate(row_code, row_name, col_code, col_name, value) |>
    dplyr::mutate(
      value = as.numeric(dplyr::if_else(value == "...", NA, value)),
      core_matrix = (row_code %in% core_rows) & (col_code %in% core_cols)
    )

  x
}


#' Transform I-O table from long to wide format
#'
#' @param data Tidy table.
#' @param core Core matrix or full table.
#' @param labels "code" or "name"
#'
#' @return Wide table.
#' @export
#' @examples
#' bea_io_get("2023_use_sec_2017") |>
#'   bea_io_pivot_wider(core = TRUE, labels = "code")
#'
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




#' Read and tidy NAICS crosswalk
#' @param path Path to unzipped spreadsheet
#' @param meta Metadata list as returned by bea_io_meta("key")
bea_io_read_naics <- function(path, meta) {

  # read data section of sheet as text
  d <- readxl::read_excel(
    path,
    sheet = "NAICS Codes",
    col_names = c("sector", "summary", "u_summary", "detail", "title", "notes", "naics"),
    col_types = "text",
    skip = meta$read$skip,
    n_max = meta$read$rows,
    .name_repair = "unique_quiet")


  d <- d |>
    dplyr::filter(dplyr::if_any(dplyr::everything(), \(x) !is.na(x))) |>
    dplyr::mutate( # move titles to single column
      title = dplyr::coalesce(title, detail, u_summary, summary, sector),
      summary = dplyr::if_else(is.na(sector), summary, NA),
      u_summary = dplyr::if_else(is.na(summary), u_summary, NA),
      detail = dplyr::if_else(is.na(u_summary), detail, NA))

  # at this stage, every row must have exactly one code in either of the four levels
  codes_per_row <- d |>
    dplyr::select(sector, summary, u_summary, detail) %>%
    {!is.na(.)} |>
    rowSums()
  if (any(codes_per_row != 1)) {
    stop("Some rows have more than one code.")
  }

  # forward-fill codes within higher level group
  d <- d |>
    tidyr::fill(sector) |>
    dplyr::group_by(sector) |> tidyr::fill(summary) |>
    dplyr::group_by(summary) |> tidyr::fill(u_summary) |>
    dplyr::ungroup()

  # handle "n.a." and "23*", trailing whitespace
  d <- d |>
    dplyr::mutate(
      naics = stringr::str_trim(naics),
      naics = dplyr::case_match(naics, "23*" ~ "23", "n.a." ~ NA, .default = naics)
    )

  # expand naics lists and create separate row for each naics code
  # TODO: remove invalid NAICS codes that are created, e.g. 32192-9 must be only 32192 and 32199
  d <- d |>
    dplyr::rowwise() |>
    dplyr::mutate(naics = expand_naics_lists(naics)) |>
    tidyr::separate_longer_delim(naics, delim = ",")

  dplyr::select(d, !notes)
}



# expand "111-3, 116" into "111,112,113,116"
expand_naics_lists <- function(x) {
  if (is.na(x)) return(x)

  stringr::str_split_1(x, ", ") |>
    lapply(expand_naics_dash) |>
    as.character() |>
    stringr::str_flatten(collapse = ",")

}

# expand "111-3" into "111,112,113"
expand_naics_dash <- function(x) {
  fromto <- stringr::str_split_1(x, "-")
  if (length(fromto) == 1) return(x)
  from <- fromto[1]
  to <- fromto[2]
  stopifnot(stringr::str_length(to) == 1)
  to <- paste0(stringr::str_sub(from, end = -2), to)
  seq(as.integer(from), as.integer(to)) |>
    as.character() |>
    stringr::str_flatten(collapse = ",")
}


