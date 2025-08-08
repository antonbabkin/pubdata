
naics_get <- function(key) {
  this_meta <- meta("naics", key, print = FALSE)

  if (this_meta$type == "raw") {
    path <- pubdata_path("naics", this_meta$path)
    utils::download.file(this_meta$url, mkdir(path))
    stopifnot(file.exists(path))
    return(path)
  }

  if (this_meta$keys$table == "code") {
    ret <- naics_read_code(this_meta)
  }
  ret
}


#' Read NAICS 2-6 digit codes table
naics_read_code <- function(this_meta) {

  raw <- get("naics", this_meta$depends)

  # All years have only 3 columns of data, but some spreadsheets are read with extra columns of all missing values
  ct <- switch(
    this_meta$keys$revision,
    "2017" = c("numeric", "text", "text", "skip", "skip", "skip"),
    "2022" = c("numeric", "text", "text", "skip", "skip"),
    c("numeric", "text", "text")
  )

  ret <- readxl::read_excel(
    raw,
    col_names = c("seq_no", "code", "title"),
    col_types = ct,
    skip = 2
  )

  ret
}

