# Public data

Describing, tidying and providing usage examples for publicly available datasets.

# R package installation




# Installation

1. Install [mamba](https://mamba.readthedocs.io/en/latest/index.html) and [Quarto](https://quarto.org/). Configure [SSH authentication](https://docs.github.com/en/authentication/connecting-to-github-with-ssh) with GitHub.
1. Clone this repository with submodules: `git clone --recurse-submodules git@github.com:antonbabkin/pubdata.git`
1. Go to repository folder and create new environment: `mamba env create --file environment.yml`.
    - As the project evolves, environment file might get outdated. Addinig missing packages with `mamba install` when they fail to import will usually fix the problem.
1. Activate environment and start JupyterLab. Jupytext enables opening `.md` and `.qmd` files in `nbs` folder as Jupyter notebooks.
