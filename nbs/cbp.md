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
title: "County Business Patterns"
format:
  html:
    code-fold: true
---
```

# Synopsis

`get_cbp_df`

`get_efsy_year_df`

`get_efsy_panel_df`

+++

# County Business Patterns

[Homepage](https://www.census.gov/programs-surveys/cbp.html) |
[CSV datasets](https://www.census.gov/programs-surveys/cbp/data/datasets.html)

```{code-cell} ipython3
:tags: [nbd-module]

import sys
import typing
import zipfile
from collections import defaultdict
import logging
import shutil

import pandas as pd
import numpy as np
import pyarrow
import pyarrow.dataset

from pubdata import naics
from pubdata.reseng.util import download_file
from pubdata.reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'root': nbd.root,
    'cbp_src': nbd.root / 'data/cbp/cbp_src/',
    'cbp_pq': nbd.root / 'data/cbp/cbp_pq/',
    'efsy_src': nbd.root / 'data/cbp/efsy_src/',
    'efsy_pq': nbd.root / 'data/cbp/efsy_pq/'
}

log = logging.getLogger('pubdata.cbp')
log.handlers.clear()
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel('DEBUG')

def cleanup(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['cbp_src'], ignore_errors=True)
        shutil.rmtree(PATH['efsy_src'], ignore_errors=True)
    print('Removing processed files...')
    shutil.rmtree(PATH['cbp_pq'], ignore_errors=True)
    shutil.rmtree(PATH['efsy_pq'], ignore_errors=True)
```

```{code-cell} ipython3
:tags: [nbd-module]

import pathlib
import pickle
import functools
import typing
import json

def cacher(dump, load):
    """Caching function factory.
    dump(obj, path) saves to disk. load(path) loads from disk.
    """

    def cache(path: typing.Union[str, pathlib.Path]):
        """
        Pickle function's returned value. Function returns pickled value if it exists.
        If `path` is str, may use "{}" placeholders to be filled from function arguments.
        Placeholders must be consistent with function call arguments ({} for args, {...} for kwargs).
        """
        def wrapper(func):
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                p = path
                if isinstance(p, str):
                    p = pathlib.Path(p.format(*args, **kwargs))
                if p.exists():
                    res = load(p)
                    log.debug(f'{func.__name__}() result loaded from cache "{p}"')
                    return res
                else:
                    res = func(*args, **kwargs)
                    p.parent.mkdir(parents=True, exist_ok=True)
                    dump(res, p)
                    log.debug(f'{func.__name__}() result saved to cache "{p}"')
                    return res
            return wrapped
        return wrapper

    return cache

cache_pq = cacher(lambda o, p: pd.DataFrame.to_parquet(o, p, engine='pyarrow', index=False),
                  lambda p: pd.read_parquet(p, engine='pyarrow'))

cache_json = cacher(lambda o, p: json.dump(o, pathlib.Path(p).open('w')), lambda p: json.load(pathlib.Path(p).open('r')))


import pandas as pd

def dispall(df):
    with pd.option_context('display.max_columns', None, 'display.max_rows', None):
        display(df)
```

## source files

Years covered: 1986-2021.

U.S. record layouts:
[1986-1997](),
[1998-2006](),
[2007-2013](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/noise-layout/us_lfo_layout.txt),
[2014](),
[2015-2016](),
[2017](),
[2018-2021](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/2018_record_layouts/us-layout-2018.txthttps://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/2018_record_layouts/us-layout-2018.txt)



County record layouts:
[1986-1997](http://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/full-layout/county_layout_sic.txt),
[1998-2006](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/full-layout/county_layout.txt),
[2007-2013](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/noise-layout/county_layout.txt),
[2014](https://www2.census.gov/programs-surveys/rhfs/cbp/technical%20documentation/2014_record_layouts/county_layout_2014.txt),
[2015-2016](https://www2.census.gov/programs-surveys/rhfs/cbp/technical%20documentation/2015_record_layouts/county_layout_2015.txt),
[2017](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/2017_record_layouts/county_layout_2017.txt),
[2018-2019](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/2018_record_layouts/county-layout-2018.txt),
[2020-2021](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/2020_record_layouts/county-layout-2020.txt)

Applicable NAICS classification:
1998-2002 - [NAICS-1997](https://www2.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions/naics.txt),
2003-2007 - [NAICS-2002](https://www2.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions/naics2002.txt),
2008-2011 - [NAICS-2007](https://www2.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions/naics2007.txt),
2012-2016 - [NAICS-2012](https://www2.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions/naics2012.txt),
2017-2021 - [NAICS-2017](https://www2.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions/naics2017.txt)

```{code-cell} ipython3
:tags: [nbd-module]

def _get_cbp_src(geo: typing.Literal['us', 'state', 'county'], year: int):
    ext = 'txt' if geo == 'us' and year < 2008 else 'zip'
    path = PATH['cbp_src'] / f'{geo}/{year}.{ext}'
    if path.exists(): return path

    yr = str(year)[2:]
    if geo == 'us':
        url = f'https://www2.census.gov/programs-surveys/cbp/datasets/{year}/cbp{yr}us.{ext}'
    elif geo == 'state':
        url = f'https://www2.census.gov/programs-surveys/cbp/datasets/{year}/cbp{yr}st.zip'
    elif geo == 'county':
        url = f'https://www2.census.gov/programs-surveys/cbp/datasets/{year}/cbp{yr}co.zip'
    
    download_file(url, path.parent, path.name)

    return path

def naics_year(cbp_year):
    """Return NAICS revision year that was used in the given CBP year."""
    # 1998-2002 - NAICS-1997, 2003-2007 - NAICS-2002, 2008-2011 - NAICS-2007, 2012-2016 - NAICS-2012, 2017-2021 - NAICS-2017
    if 1998 <= cbp_year < 2003:
        return 1997
    elif 2003 <= cbp_year < 2008:
        return 2002
    elif 2008 <= cbp_year < 2012:
        return 2007
    elif 2012 <= cbp_year < 2017:
        return 2012
    elif 2017 <= cbp_year < 2022:
        return 2017
    raise
```

## columns over years

`sic` was replaced with `naics` in 1998.  
`1_4` size class was renamed to `<5` in 2017.  
`...nf` (noise flags) were added in 2007.  
`empflag` was removed in 2018.

**U.S.**  
`lfo` was added in 2008.  
`f...` (Data Suppression Flag) removed in 2018.

**State**  
`lfo` was added in 2010.  
`f...` (Data Suppression Flag) removed in 2018.

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
# U.S.
df = {}
for y in range(1986, 2022):
    d = pd.read_csv(_get_cbp_src('us', y), dtype=str, nrows=1)
    df[y] = pd.Series(True, index=d.columns.str.lower())
df = pd.concat(df, axis=1).fillna(False).replace({False: '', True: 'X'})
dispall(df)
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
# state
df = {}
for y in range(1986, 2022):
    d = pd.read_csv(_get_cbp_src('state', y), dtype=str, nrows=1)
    df[y] = pd.Series(True, index=d.columns.str.lower())
df = pd.concat(df, axis=1).fillna(False).replace({False: '', True: 'X'})
dispall(df)
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
# county
df = {}
for y in range(1986, 2022):
    d = pd.read_csv(_get_cbp_src('county', y), dtype=str, nrows=1)
    df[y] = pd.Series(True, index=d.columns.str.lower())
df = pd.concat(df, axis=1).fillna(False).replace({False: '', True: 'X'})
dispall(df)
```

## parquet

In this section, single year source files are downloaded, loaded and returned as dataframes with no modification.
Dataframes are cached in parquet for subsequent faster access.

```{code-cell} ipython3
:tags: [nbd-module]

# Not sure how SIC classification works.
# There are only 9 unique 3-digit codes (`'399/', '497/', '519/', '599/', '899/', '679/', '149/', '179/', '098/'`), which seems too little. 
# Maybe it is not nested in the same sense as NAICS is.

@cache_pq(str(PATH['cbp_pq'] / '{}/{}.pq'))
def get_cbp_df(geo: typing.Literal['us', 'state', 'county'], year: int):
    """Return dataframe with unmodified CBP."""
    # dtypes can probably be further optimized:
    # switch to int32 or int64 in columns with no NA
    
    dtype = defaultdict(
        lambda: str,
        emp='float64',
        qp1='float64',
        ap='float64',
        est='float64'
    )
    for c in ['<5', '1_4', '5_9', '10_19', '20_49', '50_99', '100_249', '250_499', '500_999', '1000', '1000_1', '1000_2', '1000_3', '1000_4']:
        for x in 'eqan':
            dtype[f'{x}{c}'] = 'float64'
    # column case varies over years
    dtype.update({c.upper(): t for c, t in dtype.items()})
    
    # numerical columns have "N" as N/A value
    na_values = {c: 'N' for c, t in dtype.items() if t == 'float64'}

    df = pd.read_csv(_get_cbp_src(geo, year), dtype=dtype, na_values=na_values)
    df.columns = df.columns.str.lower()
    return df


def preproc_get_cbp_df():
    for year in range(1986, 2022):
        for geo in ['us', 'state', 'county']:
            get_cbp_df(geo, year)
        print(year)
```

```{code-cell} ipython3
:tags: []

get_cbp_df('state', 1999).head()
```

# EFSY

[County Business Patterns Database](https://www.fpeckert.me/cbp/)
by Fabian Eckert, Teresa C. Fort, Peter K. Schott, and Natalie J. Yang

Fabian Eckert, Teresa C. Fort, Peter K. Schott, and Natalie J. Yang. "Imputing Missing Values in the US Census Bureau's County Business Patterns." NBER Working Paper \#26632, 2021.

Data are available in two versions that each may best suit different needs.
Annual ZIP files contain both CSV with imputed values and raw CBP of the same year, and imputed employment is comparable to CBP values.
Single panel zipped CSVs are available either with native (raw CBP) or harmonized 2012 NAICS codes, but employment in aggregate industry levels is the leftover that could not be assigned.

+++

## annual files

```{code-cell} ipython3
:tags: [nbd-module]

@cache_pq(str(PATH['efsy_pq'] / 'years/{}.pq'))
def get_efsy_year_df(year):
    url = f'http://fpeckert.me/cbp/Imputed%20Files/efsy_{year}.zip'
    src = download_file(url, PATH['efsy_src'] / 'years')
    
    with zipfile.ZipFile(src) as zf:
        if year == 1975:
            fname = f'{year}/Final Imputed/efsy_cbp_{year}'
        else:
            fname = f'{year}/Final Imputed/efsy_cbp_{year}.csv'
        with zf.open(fname) as f:
            dtype = defaultdict(lambda: str, lb='int32', ub='int32')
            df = pd.read_csv(f, dtype=dtype)

    df['fipstate'] = df['fipstate'].str.zfill(2)
    df['fipscty'] = df['fipscty'].str.zfill(3)
    return df
```

```{code-cell} ipython3
:tags: []

get_efsy_year_df(2012).head()
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
for year in range(1975, 2017):
    d = get_efsy_year_df(year)
    print(year)
```

The only year where `lb != ub` is 1997 (308 observations), and in those observations always `lb == 0`, EFSY panel `emp == 0` and CBP `emp` is NA.

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
for year in range(1975, 2017):
    d = get_efsy_year(year)
    print(year, (d['lb'] != d['ub']).sum())
```

## single file county-industry panel

[Readme](https://fpeckert.me/cbp/Imputed%20Files/efsy_readme_panel.txt)

> Full County×Industry Panel 1975-2018  
NB: Census changed the way the CBP data are reported starting in 2017. For these years, Census now perturbs cells with small employment counts, making these data fundamentally different from earlier periods. We do not impute data in those years since there are no missing cells in the data. We nevertheless appended 2017 and 2018 to the panel for completeness. Note that the 2017 and 2018 data are reported on a NAICS2017 basis.

```{code-cell} ipython3
:tags: [nbd-module]

@cache_pq(str(PATH['efsy_pq'] / 'efsy_panel_{}.pq'))
def _get_efsy_panel(industry: typing.Literal['native', 'naics']):
    """Download and save as parquet."""
    if industry == 'native':
        url = 'http://fpeckert.me/cbp/Imputed%20Files/efsy_panel_native.csv.zip'
        fname = 'efsy_panel_native.csv'
    elif industry == 'naics':
        url = 'http://fpeckert.me/cbp/Imputed%20Files/efsy_panel_naics.csv.zip'
        fname = 'efsy_panel_naics.csv'
        
    src = download_file(url, PATH['efsy_src'])
    with zipfile.ZipFile(src) as zf:
        with zf.open(fname) as f:
            dtype = defaultdict(lambda: str, year='int16', emp='int32' if industry == 'native' else 'float64')
            d = pd.read_csv(f, dtype=dtype)
    d['fipstate'] = d['fipstate'].str.zfill(2)
    d['fipscty'] = d['fipscty'].str.zfill(3)
    return d


def get_efsy_panel(industry: typing.Literal['native', 'naics'],
                   filters=None):
    path = PATH['efsy'] / f'efsy_panel_{industry}.pq'
    if not path.exists():
        d = _get_efsy_panel(industry)
        if filters is None:
            return d
    return pd.read_parquet(path, engine='pyarrow', filters=filters)
```

```{code-cell} ipython3
:tags: []

d = get_efsy_panel('native', [('naics', '==', '113310'), ('fipstate', '==', '01'), ('fipscty', '==', '001')])
```

# Build this module

```{code-cell} ipython3
:tags: []

nbd.nb2mod('cbp.ipynb')
```
