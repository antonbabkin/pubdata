#' BEA I-O Tables data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
bea_io_get <- function(key) {
  this_meta <- meta("bea_io", key, print = FALSE)
  path <- pubdata_path("bea_io", this_meta$path)

  if (file.exists(path)) {
    if (this_meta$type == "raw") {
      return(path)
    } else if (this_meta$type == "table") {
      return(arrow::read_parquet(path))
    }
  }

  if (this_meta$type == "raw") {
    utils::download.file(this_meta$url, mkdir(path))
    stopifnot(file.exists(path))
    return(path)
  }


  # raw is single spreadsheet
  if (key == "curr_und_peq_det") {
    return(bea_io_read_peq(this_meta))
  }

  # raw is Zip archive
  raw <- bea_io_get(this_meta$depends)
  # TODO verify that raw file is a zip
  unzipped_spreadsheet <- utils::unzip(raw, this_meta$read$file, exdir = tempdir())
  on.exit(unlink(unzipped_spreadsheet))
  logger::log_debug("unzipped to {unzipped_spreadsheet}")
  if (grepl("_su_(sup|use)_(sec|sum|det)_", key) ||
      grepl("_imp-(bef|aft)_(sum|det)_", key) ||
      grepl("_mu_use-(bef|aft)-(pro|pur)_(sec|sum|det)_", key) ||
      grepl("_mu_mak-(bef|aft)_(sec|sum|det)_", key)) {
    return(bea_io_read_table(unzipped_spreadsheet, this_meta))
  } else if (grepl("_naics", key)) {
    return(bea_io_read_naics(unzipped_spreadsheet, this_meta))
  } else {
    stop("Not implemented")
  }

}

#' Read and tidy supply/use table
#' @param path Path to unzipped spreadsheet
#' @param meta Metadata list
bea_io_read_table <- function(path, meta) {

  # read data section of sheet as text
  x_wide <- readxl::read_excel(
    path, sheet = meta$keys$year, col_names = FALSE, col_types = "text",
    skip = meta$read$skip, n_max = meta$read$rows, .name_repair = "unique_quiet")

  # remove first two rows from table and save them as column names to merge back later
  # det table header is (name, code), sec and sum are (code, name)
  code_name_idx <- if (meta$keys$level == "det") 2:1 else 1:2
  col_names <- x_wide[code_name_idx, -(1:2)] |>
    t() |>
    magrittr::set_colnames(c("col_code", "col_name")) |>
    as.data.frame() |>
    tibble::rownames_to_column("raw_col")
  x_wide <- x_wide[-(1:2), ]
  colnames(x_wide)[1:2] <- c("row_code", "row_name")

  # rows and columns of the core matrix
  core_rows <- x_wide$row_code[1:meta$read$matrix[1]]
  core_cols <- col_names$col_code[1:meta$read$matrix[2]]

  # pivot columns to rows
  # browser()
  x_long <- tidyr::pivot_longer(x_wide, !c(row_code, row_name), names_to = "raw_col")

  # add column names back, strip footnote markers, change value type to numeric, add core matrix indicator
  x <- x_long |>
    dplyr::left_join(col_names, "raw_col") |>
    dplyr::select(row_code, row_name, col_code, col_name, value) |>
    dplyr::mutate(
      row_name = stringr::str_remove(row_name, " [/\\[][:digit:][/\\]]$"),
      col_name = stringr::str_remove(col_name, " [/\\[][:digit:][/\\]]$"),
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
#' get("bea_io", "2023_use_sec_2017") |>
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
#' @param meta Metadata list
bea_io_read_naics <- function(path, this_meta) {

  # read data section of sheet as text
  d <- readxl::read_excel(
    path,
    sheet = "NAICS Codes",
    col_names = c("sector", "summary", "u_summary", "detail", "title", "notes", "naics"),
    col_types = "text",
    skip = this_meta$read$skip,
    n_max = this_meta$read$rows,
    .name_repair = "unique_quiet")

  d <- d |>
    # drop empty rows
    dplyr::filter(dplyr::if_any(dplyr::everything(), \(x) !is.na(x))) |>
    # move titles to single column
    dplyr::mutate(
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
  d <- d |>
    dplyr::rowwise() |>
    dplyr::mutate(naics = expand_naics_lists(naics)) |>
    tidyr::separate_longer_delim(naics, delim = ",")

  # remove invalid NAICS codes that are created, e.g. 32192-9 must be only 32192 and 32199
  naics_key <- switch(
    this_meta$keys$revision,
    "2022" = "2012_code",
    "2023" = "2017_code"
  )
  stopifnot(!is.null(naics_key))
  valid_naics_codes <- get("naics", naics_key)$code
  d <- d |>
    dplyr::filter(is.na(naics) | naics %in% valid_naics_codes)

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


#' PEQ-IO commodity bridge
bea_io_read_peq <- function(this_meta) {

  raw_path <- bea_io_get(this_meta$depends)

  # identify year sheets
  years <- raw_path |>
    readxl::excel_sheets() |>
    stringr::str_subset("^[1-9][0-9]{3}$")

  # read all year sheets in dataframes
  df <- list()
  for (year in years) {
    df[[year]] <- raw_path |>
      readxl::read_excel(
        sheet = year,
        skip = 5,
        col_names = c("nipa_line", "peq_cat", "com_code", "com_desc", "pro_val", "transp", "trade_wh", "trade_re", "pur_val", "year")
      )
  }
  # combine into single dataframe
  df <- dplyr::bind_rows(df) |>
    dplyr::relocate(year)

  df
}
