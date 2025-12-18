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

  # read RUC and UI tables
  if (key %in% c("ruc_1974", "ruc_1983", "ruc_1993", "ruc_2003", "ruc_2003pr", "ruc_2013", "ruc_2023",
                 "ui_2003pr", "ui_2003_1993", "ui_2013", "ui_2024",
                 "ruca_1990", "ruca_2000", "ruca_2010", "ruca_2010zip", "ruca_2020", "ruca_2020zip",
                 "far_2000", "far_2010")) {
    # if sheet not specified in meta, it will be "NULL" and default to first sheet
    sheet <- this_meta$read$sheet
    # if skip rows not specified in meta, skip 1
    skip <- this_meta$read$skip
    if (is.null(skip)) skip <- 1
    # read everything as text and convert manually
    # for numeric types, read_excel does not distinguish integer and double
    # reading text columns as numeric prints warning for each value
    df <- readxl::read_excel(raw, sheet = sheet, col_names = names(types), skip = skip, col_types = "text")
    for (col in names(df)) {
      if (types[[col]] == "integer") {
        df[[col]] <- as.integer(df[[col]])
      } else if (types[[col]] == "double") {
        df[[col]] <- as.double(df[[col]])
      } else if (types[[col]] == "logical") {
        df[[col]] <- as.logical(df[[col]])
      }
    }

    if (key == "ruca_2000") {
      # secondary code is stored as number in excel, and reads with too many digits, e.g. 9.1999999999999993 instead of 9.2
      df[["ruca2_code"]] <- df[["ruca2_code"]] |>
        as.double() |>
        round(digits = 1) |>
        formatC(digits = 1, format = "f") |>
        stringr::str_remove("\\.0")
      # state, county and tract components of the FIPS code are missing leading zeroes
      df[["st_code"]] <- df[["st_code"]] |>
        stringr::str_pad(width = 2, pad = "0")
      df[["cty_code"]] <- df[["cty_code"]] |>
        stringr::str_pad(width = 3, pad = "0")
      df[["tract_code"]] <- df[["tract_code"]] |>
        stringr::str_pad(width = 6, pad = "0")
    } else if (key == "ruca_2010") {
      # secondary code is stored as number in excel, and reads with too many digits, e.g. 9.1999999999999993 instead of 9.2
      df[["ruca2_code"]] <- df[["ruca2_code"]] |>
        as.double() |>
        round(digits = 1) |>
        formatC(digits = 1, format = "f") |>
        stringr::str_remove("\\.0")
    }

  }

  df
}



