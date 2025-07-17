

#' @importFrom magrittr "%>%"


#' Construct path within pubdata cache folder
pubdata_path <- function(...) {
  pubdata_dir <- Sys.getenv("PUBDATA_CACHE_DIR")
  if (pubdata_dir == "") stop("PUBDATA_CACHE_DIR environmental variable is not set.")
  file.path(pubdata_dir, ...)
}

#' Path to metadata YAML file
pubdata_meta_path <- function(collection) {
  system.file(file.path("extdata", collection, "meta.yml"), package = "pubdata")
}


# Create parent directory of given path.
# Returns back given path for convenience.
mkdir <- function(p) {
  d <- dirname(p)
  if (!dir.exists(d)) {
    logger::log_debug("Creating directory {d}")
    dir.create(d, recursive = TRUE)
  }
  invisible(p)
}



#' Custom download function for instances where download.file() does not work
download_file <- function(url, path) {
  mkdir(path)
  download_status <- try(utils::download.file(url, path))
  if (download_status == 0) {
    logger::log_info("download success: ", url, " to ", path)
  } else {
    logger::log_warn("download failed, attempting alternative method... ", url)
    req <- httr2::request(url) |>
      httr2::req_headers(
        `User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0",
      )
    resp <- httr2::req_perform(req, path)
    logger::log_info("download success: ", url, " to ", path)
  }
}


#' Fill placeholders in meta nodes with values parsed from key
#'
#' Parse key components from using mask.
#' Recursively walk through the nested meta object and fill placeholders
#' in strings with key values.
#' Attach keys list as a node in the meta.
#'
#' @param key Character key that matches template in meta$mask.
#' @param meta Tree-like structure of nested lists lists and vectors.
#'
glue_meta <- function(key, meta) {
  # "mask" node specifies the template of the key components
  # if it is not present, no need to fill anything
  if (is.null(meta$mask)) return(meta)

  # parse key components matching mask template against key string
  keys <- unglue::unglue(key, meta$mask)[[1]] |>
    as.list()

  # modify tree made of lists and vectors, applying glue to character leaves
  # do not glue the special node named "mask"
  walk <- function(node) {
    if (length(node) == 1) { # leaf
      if (node == meta$mask) {
        return(node)
      }
      if (is.character(node)) {
        return(node |>
          glue::glue(.envir = as.environment(keys)) |>
          as.character()
        )
      }
      return(node)
    } else { # node, recurse further down
      return(purrr::modify(node, walk))
    }
  }

  meta <- walk(meta)
  meta$keys <- keys
  meta
}




