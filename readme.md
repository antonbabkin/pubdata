# Public data

Describing, tidying and providing usage examples for publicly available datasets.

# R package installation

Ensure Rstudio is installed on to your computer if not install it.
Follow the steps below on you Rstudio console to install the packages.

```         
install.packages("remotes")
remotes::install_github("antonbabkin/pubdata@rpkg")
```

Installation will likely request you to install and/or update additional packages.
This is normal, as `pubdata` depends on a few packages such as `dplyr` or `arrow`.

`pubdata` needs to know where you want to store the data that it downloads.
To configure this location, run the following command in RStudio console.


```
Sys.setenv(PUBDATA_CACHE_DIR = "path/to/pubdata_cache")
``` 

Choose your preferred location on your computer while setting the `pubdata` directory.
This path can be any folder on your computer that you can write files to.
Verify that the environment variable is set: `Sys.getenv("PUBDATA_CACHE_DIR")`. 
You should see the path that you just entered.
This is a temporary setting, and you will need to redo it every time you restart your R session.

## Setting the `PUBDATA_CACHE_DIR` Environment Variable

To specify where the `pubdata` cache is stored, you need to set the `PUBDATA_CACHE_DIR` environment variable.
There are two ways to do this:

1. Set the environment variable directly in your R scripts.  
Add this line near the start of your script, replacing the path with your actual cache directory:
`Sys.setenv(PUBDATA_CACHE_DIR = "/full/path/to/pubdata_cache")`

2. Save the variable in the `.Renviron` file.
Add the following line to a file named `.Renviron` in the root folder of your project:
`PUBDATA_CACHE_DIR="/full/path/to/pubdata_cache"`.
This file will be automatically read by R at startup to set environment variables.


## Verifying the installation

Try to call the following functions from `pubdata`.
They should produce no errors, print respective outputs, and download some data to the folder you specified earlier.

Print list of all available collections.

```
pubdata::ls()
```

Print it with additional details.

```
pubdata::ls(details=True)
```

Print metadata about the “ers_rural” collection.

```
pubdata::meta("ers_rural")
```

Print all keys available in the “ers_rural” collection.

```
pubdata::ls("ers_rural")
```

Print all keys in "naics" collection that match a regex pattern, with details.

```
pubdata::ls("ers_rural", pattern = "2023", detail = TRUE)
```

Print metadata about “ruc_2023” dataset in the “ers_rural” collection.

```
pubdata::meta("ers_rural", key = "ruc_2023")
```

Download and read the “ruc_2023” dataset in the “ers_rural” collection.
This command returns a tibble (data frame) that you can assign to a variable and use in your code.

```
pubdata::get("ers_rural", key = "ruc_2023")
```




# Legacy Python installation

1.  Install [mamba](https://mamba.readthedocs.io/en/latest/index.html) and [Quarto](https://quarto.org/). Configure [SSH authentication](https://docs.github.com/en/authentication/connecting-to-github-with-ssh) with GitHub.
2.  Clone this repository with sub modules: `git clone --recurse-submodules git@github.com:antonbabkin/pubdata.git`
3.  Go to repository folder and create new environment: `mamba env create --file environment.yml`.
    -   As the project evolves, environment file might get outdated. Adding missing packages with `mamba install` when they fail to import will usually fix the problem.
4.  Activate environment and start Jupiter. Jupiter enables opening `.md` and `.qmd` files in `nbs` folder as Jupyter notebooks.
