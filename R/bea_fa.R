
bea_fa_get <- function(key) {
  this_meta <- meta("bea_fa", key, print = FALSE)

  if (this_meta$type == "raw") {
    path <- pubdata_path("bea_fa", this_meta$path)
    utils::download.file(this_meta$url, mkdir(path))
    stopifnot(file.exists(path))
    return(path)
  }

  ret <- bea_fa_read(this_meta)
  ret

}


bea_fa_read <- function(this_meta) {
  raw_path <- get("bea_fa", this_meta$depends)

  # read single industry sheet to use for long asset type descriptions
  df_oneind <- raw_path |>
    readxl::read_excel(
      sheet = "110C",
      skip = 5
    ) |>
    dplyr::slice(-1) |>
    dplyr::rename(asset_code = "Asset Codes", asset_type = "NIPA Asset Types") |>
    # codes of aggregate groups as they appear in the "datasets" sheet
    dplyr::mutate(
      asset_code = case_match(
        asset_code,
        "EQUIPMENT" ~ "EQ00",
        "STRUCTURES" ~ "ST00",
        "IPP" ~ "IP00",
        .default = asset_code
      )
    )

  # read all industries from the "datasets" sheet
  df_allind <- raw_path |>
    readxl::read_excel(
      sheet = "Datasets",
      .name_repair = "unique_quiet"
    ) |>
    dplyr::rename(line_code = ...1) |>
    # structure of the line code
    # examples: "K1N110C1EP1A.A", "M1N110C1SO02.A", "I3N110C1EQ00.A"
    # 1-3: non-residential, current cost table type = "K1N" stock, "M1N" depreciation, "I3N" investment
    # 4-7: "110C" = 4-digit industry code
    # 8: "1" = always 1
    # 9-12: "EQ00" = 4-digit asset code
    # 13-14: ".A" = always .A
    tidyr::separate_wider_position(line_code, widths = c(3, ind_code = 4, 1, asset_code = 4, 2), cols_remove = FALSE)

  # add asset type descriptions and pivot to long format
  x <- df_oneind |>
    dplyr::select(asset_code, asset_type)
  ret <- df_allind |>
    dplyr::left_join(x, by = "asset_code", relationship = "many-to-one") |>
    tidyr::pivot_longer(!c(line_code, ind_code, asset_code, asset_type), names_to = "year") |>
    dplyr::mutate(year = as.numeric(year)) |>
    dplyr::relocate(line_code, ind_code, asset_code, asset_type, year, value)

  ret
}

