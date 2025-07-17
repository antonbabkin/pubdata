
# Activate renv environment if it has been initialized
if (("renv" %in% utils::installed.packages()[, "Package"])
    && dir.exists(renv:::renv_bootstrap_library_root(getwd()))
    && file.exists("renv/activate.R")) {
  source("renv/activate.R")
  print("Active package libraries:")
  print(.libPaths())
}

# load devtools library for package development
if (interactive()) {
  suppressMessages(require(devtools))
}
