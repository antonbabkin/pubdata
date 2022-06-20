---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.7
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

# Quick test

Run a selection of quick functions from each module. 

```{code-cell} ipython3
:tags: []

from pubdata import naics, cbp, bds, population, geography
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
