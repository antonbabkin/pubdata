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

import pandas as pd
import pyarrow
import pyarrow.dataset

from pubdata.reseng.util import download_file
from pubdata.reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'root': nbd.root,
    'source': nbd.root / 'data/source/cbp/',
    'parquet': nbd.root / 'data/cbp/parquet/',
    'cbp_raw': nbd.root / 'data/cbp/raw/',
    'cbp_panel': nbd.root / 'data/cbp/panel/',
    'src_efsy': nbd.root / 'data/cbp/src_efsy/',
    'efsy': nbd.root / 'data/cbp/efsy/'
}

log = logging.getLogger('pubdata.cbp')
log.handlers.clear()
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel('DEBUG')
```

## Source files

U.S. record layouts:
[1986-1997](),
[1998-2006](),
[2007-2013](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/noise-layout/us_lfo_layout.txt),
[2014](),
[2015-2016](),
[2017](),
[2018-2019]()



County record layouts:
[1986-1997](http://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/full-layout/county_layout_sic.txt),
[1998-2006](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/full-layout/county_layout.txt),
[2007-2013](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/noise-layout/county_layout.txt),
[2014](https://www2.census.gov/programs-surveys/rhfs/cbp/technical%20documentation/2014_record_layouts/county_layout_2014.txt),
[2015-2016](https://www2.census.gov/programs-surveys/rhfs/cbp/technical%20documentation/2015_record_layouts/county_layout_2015.txt),
[2017](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/2017_record_layouts/county_layout_2017.txt),
[2018-2019](https://www2.census.gov/programs-surveys/cbp/technical-documentation/records-layouts/2018_record_layouts/county-layout-2018.txt)

```{code-cell} ipython3
:tags: [nbd-module]

def get_source_file(geo: typing.Literal['us', 'state', 'county'], year: int):
    ext = 'txt' if geo == 'us' and year < 2008 else 'zip'
    path = PATH['source']/f'{geo}/{year}.{ext}'
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
```

### Columns over years

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
for y in range(1986, 2020):
    d = pd.read_csv(get_source_file('us', y), dtype=str, nrows=1)
    df[y] = pd.Series(True, index=d.columns.str.lower())
df = pd.concat(df, axis=1).fillna(False).replace({False: '', True: 'X'})
with pd.option_context('display.max_columns', 50, 'display.max_rows', 100):
    display(df)
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
# state
df = {}
for y in range(1986, 2020):
    d = pd.read_csv(get_source_file('state', y), dtype=str, nrows=1)
    df[y] = pd.Series(True, index=d.columns.str.lower())
df = pd.concat(df, axis=1).fillna(False).replace({False: '', True: 'X'})
with pd.option_context('display.max_columns', 50, 'display.max_rows', 100):
    display(df)
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
# county
df = {}
for y in range(1986, 2020):
    d = pd.read_csv(get_source_file('county', y), dtype=str, nrows=1)
    df[y] = pd.Series(True, index=d.columns.str.lower())
df = pd.concat(df, axis=1).fillna(False).replace({False: '', True: 'X'})
with pd.option_context('display.max_columns', 50):
    display(df)
```

## annual files
Not sure how SIC classification works. There are only 9 unique 3-digit codes (`'399/', '497/', '519/', '599/', '899/', '679/', '149/', '179/', '098/'`), which seems too little. Maybe it is not nested in the same sense as NAICS is.

Source files are read and converted into parquet files. Some columns are left out, mainly flags and size class distributions.

```{code-cell} ipython3
:tags: [nbd-module]

@cache_pq(str(PATH['cbp_raw'] / '{}/{}.pq'))
def get_cbp_raw(geo: typing.Literal['us', 'state', 'county'], year: int):

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

    df = pd.read_csv(get_source_file(geo, year), dtype=dtype, na_values=na_values)
    df.columns = df.columns.str.lower()
    return df

def preproc_get_cbp_raw():
    for year in range(1986, 2022):
        for geo in ['us', 'state', 'county']:
            get_cbp_raw(geo, year)
        log.info(f'preproc_get_cbp_raw year finished {year}')
```

```{code-cell} ipython3
:tags: []

d = get_cbp_raw('county', 2021)
d.head()
```

```{code-cell} ipython3
:tags: [nbd-module]

employee_size_classes = ['1_4', '5_9', '10_19', '20_49', '50_99', '100_249', '250_499', '500_999', '1000', '1000_1', '1000_2', '1000_3', '1000_4']

def get_df(geo: typing.Literal['us', 'state', 'county'], year: int):
    path = PATH['parquet']/f'{geo}/{year}/part.pq'
    if path.exists():
        return pd.read_parquet(path, 'pyarrow')

    # which columns to read and what data types, depending on year and geography
    dt = {
        'emp': float,
        'qp1': float,
        'ap': float,
        'est': float
    }

    if year < 1998:
        dt['sic'] = str
    else:
        dt['naics'] = str

    if geo == 'county':
        dt.update({'fipstate': str, 'fipscty': str})
    if geo == 'state':
        dt.update({'fipstate': str})
    
    if ((geo == 'us') and (year >= 2008)) or ((geo == 'state') and (year >= 2010)):
        dt.update({'lfo': str})
    
    suf = (['1_4'] if year < 2017 else ['<5']) \
        + (employee_size_classes[1:] if geo == 'county' else employee_size_classes[1:-4])
    dt.update({f'n{s}': float for s in suf})
    
    if (year == 2015) or ((geo, year) == ('us', 2006)):
        dt = {k.upper(): v for k, v in dt.items()}
    
    # numerical columns have "N" as N/A value
    na_val = {c: 'N' for c, t in dt.items() if t == float}

    df = pd.read_csv(get_source_file(geo, year), usecols=dt.keys(), dtype=dt, na_values=na_val)
    df.columns = df.columns.str.lower()
    
    if year >= 2017:
        df = df.rename(columns={'n<5': 'n1_4'})
    
    # add dummy lfo column for multi-year parquet dataset support
    if (geo in ['us', 'state']) and ('lfo' not in df):
        df['lfo'] = '-'
    
    df['ind_digits'] = -1
    if year < 1998:
        s = df['sic']
        assert s.notna().all() and (s != '').all()
        df.loc[s == '----', 'ind_digits'] = 0
        df.loc[s.str[:2].str.isdigit() & (s.str[2:] == '--'), 'ind_digits'] = 2
        pad = '/' if year == 1986 else '\\'
        df.loc[s.str[:3].str.isdigit() & (s.str[3] == pad), 'ind_digits'] = 3
        df.loc[s.str.isdigit(), 'ind_digits'] = 4
        df = df.rename(columns={'sic': 'industry'})
    else:
        s = df['naics']
        assert s.notna().all() and (s != '').all()
        df.loc[s == '------', 'ind_digits'] = 0
        df.loc[s.str[:2].str.isdigit() & (s.str[2:] == '----'), 'ind_digits'] = 2
        df.loc[s.str[:3].str.isdigit() & (s.str[3:] == '///'), 'ind_digits'] = 3
        df.loc[s.str[:4].str.isdigit() & (s.str[4:] == '//'), 'ind_digits'] = 4
        df.loc[s.str[:5].str.isdigit() & (s.str[5] == '/'), 'ind_digits'] = 5
        df.loc[s.str.isdigit(), 'ind_digits'] = 6
        df = df.rename(columns={'naics': 'industry'})

    df['ind_digits'] = df['ind_digits'].astype('int8')
    assert (df['ind_digits'] != -1).all()
    for d in [2,3,4,5]:
        df.loc[df['ind_digits'] == d, 'industry'] = df['industry'].str[:d]
    df.loc[df['industry'].str[0] == '-', 'industry'] = '-'
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, 'pyarrow', index=False)
    return df


def build_all_parquet():
    for year in range(1986, 2020):
        for geo in ['us', 'state', 'county']:
            get_df(geo, year)
```

```{code-cell} ipython3
:tags: []

build_all_parquet()
```

With file structure like this (each year is stored `county/year/part.pq`), it is possible to read from multiple files treating them as [Parquet Dataset](https://arrow.apache.org/docs/python/dataset.html). Particularly useful can be the `filters` parameter - see filter specification [here](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html). Below are some examples.

```{code-cell} ipython3
:tags: []

part = pyarrow.dataset.partitioning(field_names=['year'])
# read subset of columns across all years
cols = ['year', 'fipstate', 'fipscty', 'industry', 'emp', 'est']
d = pd.read_parquet(PATH['parquet']/'county', 'pyarrow', columns=cols, partitioning=part)
# # filter by industry code
d = pd.read_parquet(PATH['parquet']/'county', 'pyarrow', columns=cols, partitioning=part,
                    filters=[('industry', '=', '-')])
# # filter by year and state
d = pd.read_parquet(PATH['parquet']/'county', 'pyarrow', columns=cols, partitioning=part,
                    filters=[('year', '>', 2000), ('fipstate', '=', '55')])
# US totals over time
d = pd.read_parquet(PATH['parquet']/'us', 'pyarrow', partitioning=part,
                    filters=[('lfo', '=', '-'), ('industry', '=', '-')])
```

```{code-cell} ipython3
:tags: [nbd-module]

def get_parquet(geo, cols=None, filters=None):
    path = PATH['parquet'] / geo
    part = pyarrow.dataset.partitioning(field_names=['year'])
    return pd.read_parquet(path, 'pyarrow', columns=cols, filters=filters,
                           partitioning=part)
```

```{code-cell} ipython3
:tags: []

get_parquet('state', ['year', 'est', 'emp', 'industry', 'lfo', 'fipstate'],
                 [('lfo', '=', '-'), ('industry', '=', '1151'), ('fipstate', '=', '55')])
```

# EFSY

[County Business Patterns Database](https://www.fpeckert.me/cbp/)
by Fabian Eckert, Teresa C. Fort, Peter K. Schott, and Natalie J. Yang

Fabian Eckert, Teresa C. Fort, Peter K. Schott, and Natalie J. Yang. "Imputing Missing Values in the US Census Bureau's County Business Patterns." NBER Working Paper \#26632, 2021.

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
```

## single file county-industry panel

[Readme](https://fpeckert.me/cbp/Imputed%20Files/efsy_readme_panel.txt)

> Full County×Industry Panel 1975-2018  
NB: Census changed the way the CBP data are reported starting in 2017. For these years, Census now perturbs cells with small employment counts, making these data fundamentally different from earlier periods. We do not impute data in those years since there are no missing cells in the data. We nevertheless appended 2017 and 2018 to the panel for completeness. Note that the 2017 and 2018 data are reported on a NAICS2017 basis. 

```{code-cell} ipython3
:tags: [nbd-module]

@cache_pq(str(PATH['efsy'] / 'efsy_panel_{}.pq'))
def _get_efsy_panel(industry: typing.Literal['native', 'naics']):
    """Download and save as parquet."""
    if industry == 'native':
        url = 'http://fpeckert.me/cbp/Imputed%20Files/efsy_panel_native.csv.zip'
        fname = 'efsy_panel_native.csv'
    elif industry == 'naics':
        url = 'http://fpeckert.me/cbp/Imputed%20Files/efsy_panel_naics.csv.zip'
        fname = 'efsy_panel_naics.csv'
    else:
        raise
        
    src = download_file(url, PATH['src_efsy'])
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

## annual files

1997 is the only year where `lb != ub` sometimes

```{code-cell} ipython3
:tags: [nbd-module]

@cache_pq(str(PATH['efsy'] / 'years/{}.pq'))
def get_efsy_year(year):
    url = f'http://fpeckert.me/cbp/Imputed%20Files/efsy_{year}.zip'
    src = download_file(url, PATH['src_efsy'])
    
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
---
jupyter:
  outputs_hidden: true
tags: []
---
for year in range(1975, 2017):
    d = get_efsy_year(year)
    print(year)
```

careful! UB and LB in yearly files look like CBP employment, but emp in the panel file in aggregate industry levels is the leftover that could not be assigned.

```{code-cell} ipython3
:tags: []

year = 1997
d1 = get_efsy_year(year).rename(columns={'naics': 'sic'})
d2 = get_efsy_panel('native', [('year', '==', year)])

d3 = get_cbp_raw('county', year)

d = d1.merge(d2[['fipstate', 'fipscty', 'sic', 'emp']], 'outer')
d = d.merge(d3[['fipstate', 'fipscty', 'sic', 'emp']].rename(columns={'emp': 'emp_cbp'}), 'outer')

d.head(20)
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

```{code-cell} ipython3
:tags: []

d.query('lb != ub')
```

# Combined panel

```{code-cell} ipython3
:tags: [nbd-module]

@cache_pq(str(PATH['cbp_panel'] / '{}.pq'))
def get_cbp_year(year):
    ind_col = 'naics' if year > 1997 else 'sic'
    df = get_cbp_raw('county', year)
    # df = df[['fipstate', 'fipscty', ind_col, 'est', 'emp', 'qp1', 'ap']]
    # rename applies before 2017
    df.rename(columns={'n1_4': 'n<5'}, inplace=True)
    
    df['industry'] = df[ind_col].str.replace('-', '').str.replace('/', '')
    df['ind_digits'] = df['industry'].str.len()
    
    # EFSY ends in 2016
    if year > 2016:
        return df
    
    # add EFSY employment
    d = get_efsy_year(year)
    if year < 1998:
        d.rename(columns={'naics': 'sic'}, inplace=True)
    d.rename(columns={'lb': 'efsy_lb', 'ub': 'efsy_ub'}, inplace=True)
    d.head()

    df = df.merge(d, 'left', ['fipstate', 'fipscty', ind_col], indicator=True)
    log.debug(f'{year}:\n {df["_merge"].value_counts()}')
    del df['_merge']

    # impute wage from state-industry
    d = get_cbp_raw('state', year)
    if year > 2009:
        d = d.query('lfo == "-"').copy()
    d['wage'] = d['qp1'] / d['emp'] * 4

    df = df.merge(d[['fipstate', ind_col, 'wage']], 'left', ['fipstate', ind_col], indicator=True)
    log.debug(f'{year}:\n {df["_merge"].value_counts()}')
    del df['_merge']

    # fill missing emp and ap in CBP
    df['cbp_emp'] = df['emp']
    df.loc[df['emp'] == 0, 'emp'] = (df['efsy_lb'] + df['efsy_ub']) / 2

    df['cbp_ap'] = df['ap']
    df.loc[df['ap'] == 0, 'ap'] = df['emp'] * df['wage']
    
    return df
```

TODO There are wages above \$900,000, which should not be possible. Think of a fix.

+++

TODO Does not work with SIC

```{code-cell} ipython3
:tags: []

d = get_cbp_year(2005)
d.query('ind_digits == 6')[['emp', 'cbp_emp', 'ap', 'cbp_ap']].sum()
```

# Build this module

```{code-cell} ipython3
:tags: []

nbd.nb2mod('cbp.ipynb')
```
