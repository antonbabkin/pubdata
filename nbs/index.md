---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

```{raw-cell}

---
title: "Public data"
format:
  html:
    code-fold: true
    ipynb-filters:
      - pubdata/reseng/nbd.py filter-docs
---
```

Describing, tidying and providing usage examples for publicly available datasets.

+++ {"tags": ["nbd-docs"]}

# Initialization

Run code in this section when you start working with the project.
It will create symbolic links necessary for file discovery within project directory structure.
If project is used as a library, importing code must call the `init()` function.

```{code-cell} ipython3
:tags: [nbd-module]

import os
import importlib
from pathlib import Path

def init():
    """Initialize project file structure by recreating symlinks to package and all submodule packages.
    Safe to run multiple times.
    """
    print('Initializing project "pubdata" and submodule "reseng"...')
    root_dir = _this_proj_root()
    print(f'  Project "pubdata" root directory: "{root_dir}"')
    
    _recreate_dir_symlink('nbs/pubdata', '../pubdata', root_dir)
    _recreate_dir_symlink('pubdata/reseng', f'../submodules/reseng/reseng', root_dir)
    from pubdata.reseng.index import init as reseng_init
    reseng_init()
    
    print('Initialization of "pubdata" finished.\n')

def _this_proj_root():
    """Return abs path to this project's root dir."""
    try:
        # caller is "index.py" module
        caller_dir = Path(__file__).parent.resolve()
    except Exception as e:
        if str(e) != "name '__file__' is not defined": raise
        # caller is "index.ipynb" notebook
        caller_dir = Path.cwd()
    return caller_dir.parent

def _recreate_dir_symlink(link, targ, root):
    """Remove and create new symlink from `link` to `targ`.
    `link` must be relative to `root`.
    `targ` must be relative to directory containing `link`.
    Example: _recreate_dir_symlink('nbs/reseng', '../reseng', Path('/path/to/proj/root'))
    """
    link = (root / link).absolute()
    assert (link.parent / targ).is_dir()
    link.unlink(missing_ok=True)
    link.symlink_to(Path(targ), target_is_directory=True)
    link_res = link.resolve()
    assert link_res.is_dir()
    print(f'  symlink: "{link.relative_to(root)}" -> "{link_res.relative_to(root)}"')
```

+++ {"tags": ["nbd-docs"]}

Run initialization in the notebook.

```{code-cell} ipython3
:tags: [nbd-docs]

#| code-fold: false
init()
```

# Reproduction and testing

## Quick

Full reproduction of this project downloads a lot of data and might take significant time.
This section performs a set of quick tests.
Passing these tests does not guarantee that everything works, but gives a high degree of confidence.

```{code-cell} ipython3
:tags: []

from pubdata import naics, cbp, bds, population, geography, ers_rurality
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
naics.compute_structure_summary(2017)
```

```{code-cell} ipython3
:tags: []

cbp.get_df('us', 2019).head()
```

```{code-cell} ipython3
:tags: []

bds.get_df('').head()
```

```{code-cell} ipython3
:tags: []

population.get_df().head()
```

```{code-cell} ipython3
:tags: []

geography.get_state_df(geometry=False).head()
```

```{code-cell} ipython3
:tags: []

ers_rurality.get_ruc_df().head()
```

```{code-cell} ipython3
:tags: []

ers_rurality.get_ui_df().head()
```

```{code-cell} ipython3
:tags: []

ers_rurality.get_ruca_df().head()
```

```{code-cell} ipython3
:tags: []

from pubdata import bea_io
bea_io.get_naics_df().head()
```

## Full

```{code-cell} ipython3
:tags: []

from pubdata import geography_cbsa
geography_cbsa.test_all(redownload=False)
```

# Build this module

```{code-cell} ipython3
:tags: []

from pubdata.reseng.nbd import Nbd
nbd = Nbd('pubdata')
nbd.nb2mod('index.ipynb')
```
