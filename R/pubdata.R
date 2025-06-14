#' Top level package interface

# list of existing data collections
collections <- c(
  "bds",
  "bea_fa",
  "bea_io",
  "bea_reg",
  "ers_rural",
  "naics"
)


# memory cache object
memory <- cachem::cache_mem()


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
  # return from memory cache if exists
  memkey <- paste0(collection, "_", key)
  value <- memory$get(memkey)
  if (!cachem::is.key_missing(value)) return(value)

  # check if disk cache exists
  value <- cache_read(collection, key)
  if (is.null(value)) {
    # calculate value from collection specific _get() function
    col_get <- base::get(paste0(collection, "_get"))
    value <- col_get(key)
    # save to disk cache
    cache_write(collection, key, value)
  }
  memory$set(memkey, value)
  value
}


cache_read <- function(collection, key) {
  me <- meta(collection, key, FALSE)
  path <- pubdata_path(collection, me$path)
  if (file.exists(path)) {
    if (me$type == "raw") {
      value <- path
    } else if (me$type == "table") {
      value <- arrow::read_parquet(path)
    }
    logger::log_debug("disk cache get \"{collection}:{key}\"")
    return(value)
  }
}


cache_write <- function(collection, key, value) {
  me <- meta(collection, key, FALSE)
  path <- pubdata_path(collection, me$path)
  if (me$type == "table") {
    arrow::write_parquet(value, mkdir(path))
  }
  logger::log_debug("disk cache set \"{collection}:{key}\"")
}


#' Pack data files into a zip archive
#'
#' @param zipfile Relative path to archive file
#' @param col_keys List of keys of form list(collection1 = c(keys, ...), collection2 = ...)
#' @param overwrite Overwrite existing zip file?
#'
#' @export
cache_pack <- function(zipfile, col_keys, overwrite = FALSE) {

  if (file.exists(zipfile)) {
    if (overwrite) {
      logger::log_info(paste("Replacing existing Zip file:", zipfile))
      file.remove(zipfile)
    }
    else stop("Zip file already exists: ", zipfile)
  }

  # vector of relative paths to data files of specified keys
  files <- col_keys |>
    purrr::imap(\(keys, collection)
                purrr::map_chr(keys, \(key) file.path(collection, meta(collection, key, FALSE)$path))) |>
    purrr::list_c()

  # change working dir to cache dir, zip and change back
  zipfile_abs <- file.path(getwd(), zipfile)
  cur_dir <- getwd()
  setwd(pubdata_path())
  utils::zip(mkdir(zipfile_abs), files)
  setwd(cur_dir)
}



#' Unpack zipped data files to cache directory
#'
#' @param zipfile Relative path to archive file
#' @param overwrite Overwrite existing data files?
#'
#' @export
cache_unpack <- function(zipfile, overwrite = FALSE) {
  utils::unzip(zipfile, overwrite = overwrite, exdir = pubdata_path())
}



