---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.4
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
Release notes:
[2019](https://www2.census.gov/programs-surveys/bds/updates/bds2019-release-note.pdf)
[2020](https://www2.census.gov/programs-surveys/bds/updates/bds2020-release-note.pdf)
[2021](https://www2.census.gov/programs-surveys/bds/updates/bds2021-release-note.pdf)

```{code-cell} ipython3
:tags: [nbd-module]

import pathlib
import shutil

import pandas as pd

from pubdata.reseng.util import download_file
from pubdata.reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'root': nbd.root,
    'src': nbd.root/'data/source/bds/'
}

def cleanup():
    print(f'cleanup deleting {PATH["src"]}')
    shutil.rmtree(PATH['src'], ignore_errors=True)
```

# Source files

Tables are accessed using their file name as key.
For example, two-way county-by-sector table key is `"st_cty_sec"`.
Lookup keys by inspecting table URLs at the [CSV datasets](https://www.census.gov/data/datasets/time-series/econ/bds/bds-datasets.html) page.

```{code-cell} ipython3
:tags: [nbd-module]

def get_src(key: str = ''):
    if key != '': 
        key = '_' + key
    url = f'https://www2.census.gov/programs-surveys/bds/tables/time-series/2021/bds2021{key}.csv'
    file_path = PATH['src'] / pathlib.Path(url).name
    if file_path.exists():
        return file_path
    return download_file(url, PATH['src'])
```

+++ {"tags": []}

## Dataframe

Dataframes are not cached in parquet format, just read from source CSV.

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
        'vcnaics3': 'str',
        'vcnaics4': 'str',
        'eage': 'str',
        'eagecoarse': 'str',
        'esize': 'str',
        'esizecoarse': 'str',
        # more columns to be added as needed
    }

    f = get_src(key)
    cols = pd.read_csv(f, nrows=0).columns
    dt = {c: dtypes[c] if c in dtypes else 'float64' for c in cols}
    df = pd.read_csv(f, dtype=dt, na_values=['(D)', '(S)', 'S', '(X)', '.'])
    return df
```

```{code-cell} ipython3
:tags: []

d = get_df('').set_index('year')
d.query('year > 2006').emp.plot(grid=True)
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

```{code-cell} ipython3
:tags: []

d = get_df('sec')
d = d.query('year.isin([1978, 2021])')
d = d.set_index(['sector', 'year'])['emp'].unstack()
d = d.apply(lambda x: x / x.sum())
d.plot.barh(title='Employment share by sector');
```

## Build this module

```{code-cell} ipython3
:tags: []

nbd.nb2mod('bds.ipynb')
```
