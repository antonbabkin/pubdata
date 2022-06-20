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

# Business Dynamics Statistics

[Homepage](https://www.census.gov/programs-surveys/bds.html) |
[About](https://www.census.gov/programs-surveys/bds/about.html) |
[CSV datasets](https://www.census.gov/data/datasets/time-series/econ/bds/bds-datasets.html) |
[API](https://www.census.gov/data/developers/data-sets/business-dynamics.html) |
[Methodology](https://www.census.gov/programs-surveys/bds/documentation/methodology.html)  
Release notes: [2019](https://www2.census.gov/programs-surveys/bds/updates/bds2019-release-note.pdf)

```{code-cell} ipython3
:tags: [nbd-module]

import pathlib

import pandas as pd

from pubdata.reseng.util import download_file
from pubdata.reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'root': nbd.root,
    'source': nbd.root/'data/source/bds/'
}
```

## Source files

```{code-cell} ipython3
:tags: [nbd-module]

def get_source(key: str = ''):
    if key != '': key = '_' + key
    url = f'https://www2.census.gov/programs-surveys/bds/tables/time-series/bds2019{key}.csv'
    file_path = PATH['source'] / pathlib.Path(url).name
    if file_path.exists(): return file_path
    return download_file(url, PATH['source'])
```

+++ {"tags": []}

## Dataframe

`metro`:
- `M` metro
- `N` nonmetro
- `SW` statewide (county code `999`)
- `U` unclassified (county code `998`, location unknown)

Suppression flags:
- `(D)` less than 3 entities in a cell
- `(S)` data quality concerns
- `(X)` structurally missing or zero

```{code-cell} ipython3
:tags: [nbd-module]

def get_df(key: str = ''):
    dtypes = {
        'year': 'int16',
        'st': 'str',
        'cty': 'str',
        'metro': 'str',
        'sector': 'str',
        'eage': 'str',
        'eagecoarse': 'str',
        'esize': 'str',
        'esizecoarse': 'str',
        # more columns to be added as needed
    }

    f = get_source(key)
    cols = pd.read_csv(f, nrows=0).columns
    dt = {c: dtypes[c] if c in dtypes else 'float64' for c in cols}
    df = pd.read_csv(f, dtype=dt, na_values=['(D)', '(S)', '(X)', '.'])
    return df
```

```{code-cell} ipython3
:tags: []

d = get_df('').set_index('year')[['estabs', 'estabs_entry_rate', 'estabs_exit_rate']]
left = d['estabs'].plot(color='black', ylim=(0, 8e6), grid=True, figsize=(8, 6))
left.legend(loc='lower left')
left.set_ylabel('establishments')
right = left.twinx()
d[['estabs_entry_rate', 'estabs_exit_rate']].plot(ax=right, ylim=(0, 20))
right.legend(loc='lower right')
right.set_ylabel('rates, %')
left.set_title('Establishments entry and exit rates, economy-wide');
```

## Build this module

```{code-cell} ipython3
:tags: []

from pubdata.reseng.nbd import Nbd
nbd = Nbd('pubdata')
nbd.nb2mod('bds.ipynb')
```
