#' Top level package interface

# list of existing data collections
collections <- c(
  "bds",
  "bea_io",
  "bea_reg",
  "ers_rural",
  "naics"
)



#' List available collections or datasets within a collection
#'
#' @param collection Collection name. Leave NULL to get list of collections.
#' @param pattern Regex pattern to filter datasets.
#' @param detail If true, return a dataframe with additional details.
#'
#' @returns Character vector of collections or datasets, or dataframe with details.
#' @export
#'
#' @examples
#' ls(collection = "bds", pattern = "cty")
ls <- function(collection = NULL, pattern = ".", detail = FALSE) {
  if (is.null(collection)) {
    if (detail) {
      metas <- lapply(collections, \(x) meta(x, print = FALSE))
      desc <- sapply(metas, \(x) x$desc, USE.NAMES = FALSE)
      ret <- data.frame(collection = collections, description = desc)
    } else {
      ret <- collections
    }
  } else {
    stopifnot(collection %in% collections)
    full_meta <- pubdata_meta_path(collection) |>
      yaml::read_yaml()
    all_keys <- names(full_meta$data)
    keys <- grep(pattern, all_keys, value = TRUE)
    if (detail) {
      metas <- lapply(keys, \(x) meta(collection, x, print = FALSE))
      desc <- sapply(metas, \(x) x$desc, USE.NAMES = FALSE)
      cache_files <- sapply(metas, \(x) x$path, USE.NAMES = FALSE)
      cache_exists <- sapply(cache_files, \(x) file.exists(pubdata_path(collection, x)), USE.NAMES = FALSE)
      ret <- data.frame(key = keys, description = desc, cache_file = cache_files, cache_exists = cache_exists)
    } else {
      ret <- keys
    }
  }
  ret
}


#' Metadata about a collection or a specific dataset
#'
#' @param collection Name of a collection.
#' @param key Data object key.
#' @param print Pretty-print metadata or return a list.
#'
#' @returns A list with metadata.
#' @export
#'
#' @examples
#' # metadata about the BDS collection
#' meta("bds")
#' # metadata about specific BDS dataset
#' meta("bds", "2022_st_cty")
meta <- function(collection, key = NULL, print = TRUE) {
  stopifnot(collection %in% collections)
  full_meta <- pubdata_meta_path(collection) |>
    yaml::read_yaml()
  if (is.null(key)) {
    ret <- full_meta[c("collection", "desc", "urls")]
    ret$data_keys <- length(full_meta$data)
  } else {
    if (!(key %in% names(full_meta$data))) {
      stop(key, " is not a valid data key. Find a correct key using 'ls(collection, pattern)'.")
    }
    ret <- glue_meta(key, full_meta$data[[key]])
  }
  if (print) {
    ret |>
      yaml::as.yaml() |>
      cat()
    invisible(ret)
  } else {
    return(ret)
  }
}


#' Get data object
#'
#'
#' @param collection Name of a collection.
#' @param key Data object key.
#' @returns Tidy table or path to raw data file.
#' @export
get <- function(collection, key) {
  col_get <- base::get(paste0(collection, "_get"))
  col_get(key)
}
