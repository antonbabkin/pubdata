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

```{raw-cell}

---
title: "BEA - National Income and Product Account (NIPA)"
format:
  html:
    code-fold: true
    embed-resources: true
#    ipynb-filters:
#      - pubdata/reseng/nbd.py filter-docs
---
```

# Prices & Inflation

[Overview](https://www.bea.gov/resources/learning-center/what-to-know-prices-inflation) and [quick comparison](https://www.bea.gov/resources/learning-center/quick-guide-some-popular-bea-price-indexes) of the five BEA inflation indexes.
[Chapter 4](https://www.bea.gov/resources/methodologies/nipa-handbook/pdf/chapter-04.pdf) of the NIPA Handbook describes calculation of indexes in detail.

**Personal Consumpton Expenditures (PCE) Price Index.**
Closely watched by the Federal Reserve.
Similar to the BLS Consumer Price Index; the formulas and uses differ.
Captures consumers' changing behavior and a wide range of expenses.

**Core PCE Price Index.**
PCE Price Index, Excluding Food and Energy.
Closely watched by the Federal Reserve.
Excludes two categories prone to volatile prices that may distort overall trends.

**Gross Domestic Purchases Price Index.**
BEA's featured measure of inflation in the U.S. economy overall.

**GDP Price Index.**
Measures only U.S.-produced goods and services.

**GDP Price Deflator.**
Closely mirrors the GDP price index, although calculated differently.
Used by some firms to adjust payments in contracts.

```{code-cell} ipython3
:tags: [nbd-module]

import shutil

import pandas as pd

from pubdata.reseng.util import download_file
from pubdata.reseng.caching import simplecache
from pubdata.reseng.nbd import Nbd
nbd = Nbd('pubdata')

PATH = {
    'source': nbd.root / 'data/source/bea_nipa',
    'proc': nbd.root / 'data/bea_nipa'
}

def _init_dirs():
    """Create necessary directories."""
    PATH['source'].mkdir(parents=True, exist_ok=True)
    PATH['proc'].mkdir(parents=True, exist_ok=True)

def cleanup(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['source'], ignore_errors=True)
    print('Removing processed files...')
    shutil.rmtree(PATH['proc'], ignore_errors=True)
```

# Source files

"Interactive Data Tables" section of the BEA website has a page for flat data [files bulk downloads](https://apps.bea.gov/iTable/?isuri=1&reqid=19&step=4&categories=flatfiles&nipa_table_list=1).
It has flat files with annual, quarterly and mohthly series, as well as table and series registers.

```{code-cell} ipython3
:tags: [nbd-module]

def _get_src_series_register():
    _init_dirs()
    f = PATH['source'] / 'SeriesRegister.txt'
    if not f.exists():
        url = 'https://apps.bea.gov/national/Release/TXT/SeriesRegister.txt'
        download_file(url, PATH['source'])
    return f

def _get_src_nipa_annual():
    _init_dirs()
    f = PATH['source'] / 'NipaDataA.txt'
    if not f.exists():
        url = 'https://apps.bea.gov/national/Release/TXT/NipaDataA.txt'
        download_file(url, PATH['source'])
    return f
```

+++ {"tags": []}

# Data

We download annual file and select series for the five price indices prepared by the BEA.

## Series codes

"Series codes" uniquely identify series in flat data files.
An easy way to find necessary code is to first identify table and line number for the wanted series, and then search for it's code in the Series Register file.

- `DPCERG`: Personal Consumpton Expenditures (PCE) Price Index
- `DPCCRG`: PCE Price Index, Excluding Food and Energy (Core PCE)
- `B712RG`: Gross Domestic Purchases Price Index
- `A191RG`: GDP Price Index
- `A191RD`: GDP Price Deflator

```{code-cell} ipython3
:tags: []

src = _get_src_series_register()
d = pd.read_csv(src)
d[d['%SeriesCode'].isin(['DPCERG', 'DPCCRG', 'B712RG', 'A191RG', 'A191RD'])]
```

`get_price_index_df()` returns dataframe with 5 price indices by year.
2012 is the base year, with indices normalized at 100.

```{code-cell} ipython3
:tags: [nbd-module]

@simplecache(PATH['proc'] / 'price_index_df.pkl')
def get_price_index_df():
    _init_dirs()
    src = _get_src_nipa_annual()
    df = pd.read_csv(src)
    df = df[df['%SeriesCode'].isin(['DPCERG', 'DPCCRG', 'B712RG', 'A191RG', 'A191RD'])]
    df['Value'] = df['Value'].str.replace(',', '').astype('float64')
    df = df.set_index(['Period', '%SeriesCode'])['Value'].unstack()
    df.columns.name = None
    df.index.name = 'year'
    df = df.rename(columns={
        'DPCERG': 'pce_price_index',
        'DPCCRG': 'core_pce_price_index',
        'B712RG': 'purchases_price_index',
        'A191RG': 'gdp_price_index',
        'A191RD': 'gdp_price_deflator'
    })
    return df
```

Over 20 years (2002-2021), cumulative price growth was about 143%.
Spread between most different indices reaches is about 6 percentage points.
GDP price index and implicit deflator are virtually indistinguishable.

```{code-cell} ipython3
:tags: []

d = get_price_index_df().query('year > 2001')
d = d.apply(lambda row: row / d.loc[2002, :] * 100, 1)
d.plot(xticks=d.index[::2], grid=True);
```

# Full test

```{code-cell} ipython3
:tags: [nbd-module]

def test_all(redownload=False):
    cleanup(redownload)
    _get_src_series_register()
    _get_src_nipa_annual()
    d = get_price_index_df()
    assert (d.loc[2012] == 100).all()
```

```{code-cell} ipython3
:tags: []

test_all(redownload=False)
```

# Build this module

```{code-cell} ipython3
:tags: []

nbd.nb2mod('bea_nipa.ipynb')
```
