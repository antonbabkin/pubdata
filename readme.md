# Tidy public data

"Like families, tidy datasets are all alike but every messy dataset is messy in its own way" ([Wickham, 2014][1]).

[1]: https://doi.org/10.18637/jss.v059.i10 "Wickham, H. (2014). Tidy Data. Journal of Statistical Software, 59(10), 1–23."

`pubdata` is an R package that provides a minimalist interface to a wide range of publicly available datasets in a standardized tidy table form. 

- Discover datasets by searching table and variable metadata.
- Programmatically retrieve datasets from their online source to local storage.
- YAML metadata format for easy entry of new datasets.
- Work with raw data files or tidy data frames.
- Standardized column names and embedded data types.
- Minimal modification of source data, all original data preserved.
- Parquet format for efficient storage and loading or out-of-core processing.
- Disk and memory cache for quick repeated access.

Visit [documentation website](https://antonbabkin.github.io/pubdata/) for a list of available datasets, variable descriptions and stats, and dataset usage examples.

# Installation

Install the latest version of the package from GitHub.
Installation will likely request you to install and/or update additional packages.
This is normal, as `pubdata` depends on a few packages such as `dplyr` and `arrow`.

```{r}
install.packages("remotes")
remotes::install_github("antonbabkin/pubdata")
```

Next, you need to specify a location where `pubdata` stores raw downloaded and processed data files.
This location needs to be set in the `PUBDATA_CACHE_DIR` environmental variable and can be any folder on your computer that you can write files to.
You can verify if the variable is set by running:

```{r}
Sys.getenv("PUBDATA_CACHE_DIR")
```

Use the following command to set the environment variable in your active R session.
This is a temporary setting, and you will need to redo it every time the session is restarted.

```{r}         
Sys.setenv(PUBDATA_CACHE_DIR = "/full/path/to/pubdata_cache")
```

Alternatively, save the variable in the `.Renviron` file for a persistent configuration.
Add the following line to a file named `.Renviron` in the root folder of your project.
This file will be automatically read by R at startup to set environment variables.

```
PUBDATA_CACHE_DIR="/full/path/to/pubdata_cache"
```

# Quick start

It is recommended not to attach the package with `library(pubdata)`.
Instead, use the full namespace to call package functions, e.g. `pubdata::ls()`.
Package functions are a few and short, and using the full namespace helps avoid name conflicts.

Main package interface consists of these three functions:

- `ls()` lists available collections and datasets.
- `meta()` provides metadata information.
- `get()` retrieves a raw data file or a clean dataframe.

Print the list of all available data collections.

```{r}
pubdata::ls()
```

Print it with additional details.

```{r}
pubdata::ls(detail = TRUE)
```

Print metadata about the "ers_rural" collection.

```{r}
pubdata::meta("ers_rural")
```

Print all data keys available in the "ers_rural" collection.

```{r}
pubdata::ls("ers_rural")
```

Print all data keys in "ers_rural" collection that match a regex pattern, with details.

```{r}
pubdata::ls("ers_rural", pattern = "2023", detail = TRUE)
```

Print metadata about "ruc_2023" dataset in the "ers_rural" collection.

```{r}
pubdata::meta("ers_rural", key = "ruc_2023")
```

Download and read the "ruc_2023" dataset in the "ers_rural" collection.
This function returns a tibble (data frame) that you can assign to a variable and use in your code.

```{r}
pubdata::get("ers_rural", key = "ruc_2023")
```

