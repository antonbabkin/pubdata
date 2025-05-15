# Public data

Describing, tidying and providing usage examples for publicly available datasets.

# R package installation

Ensure Rstudio is installed on to your computer if not install it. Follow the steps below on you Rstudio console to install the packages.

```         
install.packages("remotes")
remotes::install_github("antonbabkin/pubdata@rpkg")
```

Installation will likely request you to install and/or update additional packages. This is normal, as pubdata depends on a few packages such as tidyverse or arrow.

Pub data needs to know where you want to store the data that it downloads. To configure this location, run the following command in RStudio console:

`Sys.setenv(PUBDATA_CACHE_DIR = "path/to/pubdata_cache")` Choose your preferred location on your computer while setting the pub data directory.

This path can be any folder on your computer that you can write files to. Verify that the environment variable is set: `Sys.getenv("PUBDATA_CACHE_DIR")`

You should see the path that you just entered. This is a temporary setting, and you will need to redo it every time you restart your R session. We will configure it in a more persistent way later.

# Checking R package is installed properly

Try to call the following functions from pub data. They should produce no errors, print respective outputs, and download some data to the folder you specified earlier.

Print list of all available collections

`pubdata::ls()`

Print it with additional details `pubdata::ls(details=True)` Print metadata about the “ers_rural” collection `pubdata::meta("ers_rural")` Print all keys available in the “ers_rural” collection

`pubdata::ls("ers_rural")`

print all keys in naics collection that match a regex pattern, with details

`pubdata::ls("ers_rural", pattern = "2023", detail = TRUE)`

print metadata about “ruc_2023” dataset in the “ers_rural” collection

`pubdata::meta("ers_rural", key = "ruc_2023")`

download and read the “ruc_2023” dataset in the “ers_rural” collection

`pubdata::get("ers_rural", key = "ruc_2023")`

The last command should return a tibble (data frame) that you can assign to a variable and use in your code.

If everything worked above, it would be great. Explore what other collections and data sets currently exist in the package, try to load them using the “get()” function with correct arguments.

# Installation

1.  Install [mamba](https://mamba.readthedocs.io/en/latest/index.html) and [Quarto](https://quarto.org/). Configure [SSH authentication](https://docs.github.com/en/authentication/connecting-to-github-with-ssh) with GitHub.
2.  Clone this repository with sub modules: `git clone --recurse-submodules git@github.com:antonbabkin/pubdata.git`
3.  Go to repository folder and create new environment: `mamba env create --file environment.yml`.
    -   As the project evolves, environment file might get outdated. Adding missing packages with `mamba install` when they fail to import will usually fix the problem.
4.  Activate environment and start Jupiter. Jupiter enables opening `.md` and `.qmd` files in `nbs` folder as Jupyter notebooks.
