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
execute:
  echo: false
jupyter: python3
---
```

Describing, tidying and providing usage examples for publicly available datasets.

+++

# Recreate symlinks (Windows)

```{code-cell} ipython3
:tags: [nbd-module]

import os, pathlib

def init_symlinks():
    """Recreate symlinks of this project and all subprojects."""
    print('Initializing symlinks for the project "pubdata".')
    root_dir = _dir_up()
    print(f'VERIFY! Project root directory: "{root_dir}"')
    
    _recreate_dir_symlink('nbs/pubdata', '../pubdata', root_dir)
    _recreate_dir_symlink('pubdata/reseng', '../submodules/reseng/reseng', root_dir)
    from pubdata import reseng

def _dir_up():
    """Return dir path two levels above current notebook or script."""
    try:
        caller_dir = pathlib.Path(__file__).parent.resolve()
    except Exception as e:
        if str(e) != "name '__file__' is not defined": raise
        caller_dir = pathlib.Path.cwd()
    return caller_dir.parent

def _recreate_dir_symlink(link, targ, root):
    """Remove and create new symlink from `link` to `targ`.
    `link` must be relative to `root`.
    `targ must be relative to directory containing `link`.
    """
    link = (root / link).absolute()
    assert (link.parent / targ).is_dir()
    link.unlink(missing_ok=True)
    link.symlink_to(pathlib.Path(targ), target_is_directory=True)
    link_res = link.resolve()
    assert link_res.is_dir()
    print(f'symlink: "{link.relative_to(root)}" -> "{link_res.relative_to(root)}"')
```

```{code-cell} ipython3
init_symlinks()
```

# Quick test

Run a selection of quick functions from each module.

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

# Built this module

```{code-cell} ipython3
from pubdata.reseng.nbd import Nbd
nbd = Nbd('pubdata')
nbd.nb2mod('index.ipynb')
```
