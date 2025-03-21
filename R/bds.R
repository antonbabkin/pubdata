
#' BDS data object
#'
#' @param key Data object key.
#'
#' @return Tidy table or path to raw file.
bds_get <- function(key) {
  this_meta <- meta("bds", key, print = FALSE)

  if (this_meta$type == "raw") {
    path <- pubdata_path("bds", this_meta$path)
    utils::download.file(this_meta$url, mkdir(path))
    stopifnot(file.exists(path))
    return(path)
  }

  raw <- get("bds", this_meta$depends)
  bds_read(raw, this_meta)

}


#' Read BDS table
bds_read <- function(raw, meta) {

  # TODO: handle missing values
  # TODO: only specify columns that exist in a given file to avoid "Warning: The following named parsers don't match the column names: ..."
  df <- readr::read_csv(raw, col_types = readr::cols(
    year = "i",
    metro = "c",
    st = "c",
    cty = "c",
    eage = "c",
    eagecoarse = "c",
    esizecoarse = "c",
    sector = "c",
    firms = "i",
    estabs = "i",
    emp = "i",
    denom ="i",
    estabs_entry = "i",
    estabs_entry_rate = "d",
    estabs_exit = "i",
    estabs_exit_rate = "d",
    job_creation = "i",
    job_creation_births = "i",
    job_creation_continuers = "i",
    job_creation_rate_births = "d",
    job_creation_rate = "d",
    job_destruction = "i",
    job_destruction_deaths = "i",
    job_destruction_continuers = "i",
    job_destruction_rate_deaths = "d",
    job_destruction_rate = "d",
    net_job_creation = "i",
    net_job_creation_rate = "d",
    reallocation_rate = "d",
    firmdeath_firms = "i",
    firmdeath_estabs = "i",
    firmdeath_emp = "i"
  ))

  df
}

