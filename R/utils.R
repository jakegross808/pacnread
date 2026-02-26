#' Load raw data into package environment
#' @description Run this function before you do anything else.
#'
#' @param ftpc_params Connection information for FTPC database. Either a DSN name, a path to a csv containing connection information, or a named list of connection arguments. The csv or list should contain the following parameters:
#' * Driver
#' * Server
#' * Database
#' * Trusted_Connection
#' @param eips_paths Character vector of paths to EIPS database(s).
#' @param data_path A path to either:
#' * a folder containing the data in csv format
#' * a .zip file containing the data in csv format
#' * an .Rdata file
#' @param data_source Either "db" (fetch data from databases or cache) or "file" (fetch data from folder or zip archive of csv's).
#' @param cache Should the data be cached locally to avoid reading from the databases every time?
#' @param expire_interval_days Amount of time (in days) before the cache expires and has to be refreshed from the Access db. Defaults to 7 days. Ignored if `ftpc_conn` and `eips_paths` are `NULL`.
#' @param force_refresh Refresh the cache from the databases even if it's not expired? Ignored if `cache == FALSE`.
#' @param TE_Species default = TRUE. Include T&E species names in output.
#'
#' @return Invisibly return a list containing all raw data
#' @export
#'
#' @examples
#' \dontrun{
#'
#' eips <- c("path/to/database1_be.mdb", "path/to/database2_be.mdb")
#' ftpc <- "pacnveg"
#'
#' # Read data from veg databases and cache it locally for a week (default)
#' LoadPACNVeg(ftpc, eips)
#'
#' # Force refresh cached data that haven't expired
#' LoadPACNVeg(ftpc, eips, force_refresh = TRUE)
#'
#' # Read data from a folder of csv files. This will not be cached.
#' path_to_csv <- "path/to/csv/folder"
#' LoadPACNVeg(data_path = path_to_csv)
#' }
#'
LoadPACNVeg <- function(ftpc_params = "pacn", eips_paths, data_path, data_source = "db", cache = TRUE, expire_interval_days = 7, force_refresh = FALSE, TE_Species = TRUE) {

  ## Read from cache or database
  if (data_source == "db") {
    # Standardize path names, create path to cache, check whether cache exists
    eips_paths <- normalizePath(eips_paths, mustWork = FALSE)
    cache_path <- normalizePath(paste0(rappdirs::user_cache_dir(appname = "pacnvegetation"), "/pacnveg_cache_data.rds"), mustWork = FALSE)
    cache_expiration_path <- normalizePath(paste0(rappdirs::user_cache_dir(appname = "pacnvegetation"), "/pacnveg_cache_expiration.rds"), mustWork = FALSE)
    cache_lastrefreshed_path <- normalizePath(paste0(rappdirs::user_cache_dir(appname = "pacnvegetation"), "/pacnveg_cache_lastrefreshed.rds"), mustWork = FALSE)
    cache_exists <- file.exists(cache_path) && file.exists(cache_expiration_path)
    cache_valid <- cache_exists && (is.null(readRDS(cache_expiration_path)) ||  # If cache expiration set to NULL, always read from cache
                                      readRDS(cache_expiration_path) > Sys.time())

    # If cache = TRUE and isn't expired, read data from there, otherwise load from databases
    if (cache & cache_valid & !force_refresh) {
      message(paste0("Loading data from local cache\n", "Data last read from database on ", format(readRDS(cache_lastrefreshed_path), "%m/%d/%y")))
      data <- readRDS(cache_path)
    } else {
      # Verify that Access db(s) exist
      if (!(all(grepl(".*\\.mdb$", eips_paths)) && all(file.exists(eips_paths)))) {
        stop("Path(s) to EIPS Access database(s) are invalid")
      }

      # If FTPC params provided as csv, read it and store in a list
      if (length(ftpc_params) == 1 && grepl(".*\\.csv$", ftpc_params)) {
        ftpc_params <- readr::read_csv(ftpc_params, col_types = readr::cols(.default = readr::col_character()))
        ftpc_params <- as.list(ftpc_params)
      }
      # If FTPC params are in list form, use do.call to pass them all to dbConnect. Otherwise, just pass the DSN name straight to dbConnect.
      if (is.list(ftpc_params)) {
        ftpc_params$drv <- odbc::odbc()
        ftpc_conn <- do.call(DBI::dbConnect, ftpc_params)
      } else {
        ftpc_conn <- DBI::dbConnect(odbc::odbc(), ftpc_params)
      }

      ftpc_data <- ReadFTPC(conn = ftpc_conn, TE_Species = TE_Species)
      DBI::dbDisconnect(ftpc_conn)
      eips_data <- ReadEIPS(eips_paths)

      data <- c(ftpc_data, eips_data)

      if (cache) {
        # Create cache folder if it doesn't exist yet
        if (!dir.exists(dirname(cache_path))) {
          dir.create(dirname(cache_path), recursive = TRUE)
        }
        # Save data and expiration date to cache
        refreshed <- Sys.time()
        expiration <- refreshed + lubridate::days(expire_interval_days)
        saveRDS(data, file = cache_path)
        saveRDS(expiration, file = cache_expiration_path)
        saveRDS(refreshed, file = cache_lastrefreshed_path)
        message(paste0("Saved data to local cache\n", "Cache expires ", format(expiration, "%m/%d/%y")))
      }
    }
    # End read from cache or database
    # Read from csv, zip, or rds
  } else if (data_source == "file") {
    # Standardize data path
    data_path <- ifelse(missing(data_path), NA, normalizePath(data_path, mustWork = FALSE))
    # Figure out whether data path points to folder of csv's or zip
    is_zip <- !is.na(data_path) & grepl("*.\\.zip$", data_path, ignore.case = TRUE)  # Is the data in a .zip file?
    if(is_zip) {
      file_list <- basename(unzip(data_path, list = TRUE)$Name)
    } else {
      file_list <- list.files(data_path)
    }
    expected_files <- paste0(names(GetColSpec()), ".csv")

    if (!all(expected_files %in% file_list)) {
      missing_files <- setdiff(expected_files, file_list)
      missing_files <- paste(missing_files, collapse = "\n")
      stop(paste0("The folder provided is missing required data. Missing files:\n", missing_files))
    }

    if (is_zip) {
      temp_dir <- tempdir()
      # Use this trycatch so that even if there's an error unzipping or reading, the temp dir will be deleted
      tryCatch({
        unzip(data_path, overwrite = TRUE, exdir = temp_dir, junkpaths = TRUE)
        data <- ReadCSV(temp_dir)
      },
      finally = unlink(temp_dir, recursive = TRUE)
      )
    } else {
      data <- ReadCSV(data_path)
    }

  } else {
    stop("data_source type is invalid. It should be set to either 'db' or 'file'.")
  }

  # Tidy up the data
  data <- lapply(data, function(df) {
    df %>%
      dplyr::mutate_if(is.character, trimws, whitespace = "[\\h\\v]") %>%  # Trim leading and trailing whitespace
      dplyr::mutate_if(is.character, dplyr::na_if, "") %>%  # Replace empty strings with NA
      dplyr::mutate_if(is.character, dplyr::na_if, "NA") %>%  # Replace "NA" strings with NA
      dplyr::mutate_if(is.character, stringr::str_replace_all, pattern = "[\\v]+", replacement = ";  ")  # Replace newlines with semicolons - reading certain newlines into R can cause problems
  })

  # Actually load the data into an environment for the package to use
  tbl_names <- names(data)
  lapply(tbl_names, function(n) {assign(n, data[[n]], envir = pkg_globals)})

  invisible(data)
}
