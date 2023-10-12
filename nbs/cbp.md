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

`get_cbp_path`

`get_efsy_year_df`

`get_efsy_year_path`

`get_efsy_panel_df`

`get_efsy_panel_path`

`get_icbp_df`

`get_icbp_path`

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
    'efsy_pq': nbd.root / 'data/cbp/efsy_pq/',
    'icbp_pq': nbd.root / 'data/cbp/icbp_pq/'
}

log = logging.getLogger('pubdata.cbp')
log.handlers.clear()
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel('DEBUG')

def cleanup(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['src'], ignore_errors=True)
        shutil.rmtree(PATH['src_efsy'], ignore_errors=True)
    print('Removing processed files...')
    shutil.rmtree(PATH['parquet'], ignore_errors=True)
    shutil.rmtree(PATH['cbp_panel'], ignore_errors=True)
    shutil.rmtree(PATH['efsy'], ignore_errors=True)
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


def get_cbp_path(geo: typing.Literal['us', 'state', 'county'], year: int):
    """Return path to Parquet file, first creating the file if it does not exist."""
    path = PATH['cbp_pq'] / f'{geo}/{year}.pq'
    if not path.exists():
        get_cbp_df(geo, year)
        assert path.exists()
    return path


def preproc_get_cbp_df():
    for year in range(1986, 2022):
        for geo in ['us', 'state', 'county']:
            get_cbp_raw(geo, year)
        log.info(f'preproc_get_cbp_raw year finished {year}')
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


def get_efsy_year_path(year):
    path = PATH['efsy_pq'] / f'years/{year}.pq'
    if not path.exists():
        get_efsy_year_df(year)
        assert path.exists()
    return path
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

# Wage

County-industry wage is calculated as follows.

*Step 1.*
We calculate *national* average annualized wage for every 2-6 digit NAICS industry as `wage = qp1 / emp * 4000`.
`emp` is March 12 employment, so it is more appropriate to use first quarter payroll `qp1` and not annual `ap`.
For industries with strong seasonality, the assumption here is that seasonality in wage is weaker than seasonality in employment.

When employment and payroll are suppressed (happens for some industries even at the national level) or resulting wage value is extreme (`< 0.1` or `> 1e9`), then corresponding 2-digit sector wage is used.

*Step 2.*
We calculate *state* wages from `qp1` and `emp` for every 2-6 digit NAICS industry.
For bad wage state-industry cells (suppressed or extreme), national wage in the corresponding industry is taken from step 1 and multiplied by state-to-nation ratio of economy-wide wages (over all industries).

*Step 3.*
We similarly calculate *county* wages, filling bad cells with state wages from Step 2 adjusted by county-to-state ratio.

```{code-cell} ipython3
:tags: [nbd-module]

# interface function
def get_wage(geo: typing.Literal['us', 'state', 'county'], year: int):
    if geo == 'us':
        return _get_wage_us(year)
    if geo == 'state':
        return _get_wage_state(year)
    if geo == 'county':
        return _get_wage_county(year)
```

## national

```{code-cell} ipython3
:tags: [nbd-module]

def _get_wage_us(year):
    
    path = get_cbp_path('us', year)
    filters = [('lfo', '==', '-')] if year > 2007 else None
    df = pd.read_parquet(path, engine='pyarrow', columns=['naics', 'emp', 'qp1'], filters=filters)    

    assert not df['naics'].duplicated().any()

    df['wage'] = (df['qp1'] / df['emp'] * 4000).round()
    df['wage_f'] = pd.Series(dtype=pd.CategoricalDtype(['county-industry', 'state-industry', 'nation-industry', 'nation-sector', 'nation']))
    df['wage_f'] = 'nation-industry'

    # national sector wage
    df['CODE'] = df['naics'].str.replace('-', '').str.replace('/', '')
    n = naics.get_df(naics_year(year), 'code')[['CODE', 'CODE_2', 'DIGITS']]
    n.loc[n['DIGITS'] == 2, 'CODE'] = n['CODE'].str[:2]
    n['CODE_2'] = n['CODE_2'].str[:2]
    df = df.merge(n, 'left')

    d = df.query('DIGITS == 2').rename(columns={'wage': 'sector_wage'})
    df = df.merge(d[['CODE_2', 'sector_wage']], 'left')

    bad_wage = ~df['wage'].between(0.1, 1e9)
    df.loc[bad_wage, 'wage'] = df['sector_wage']
    df.loc[bad_wage, 'wage_f'] = 'nation-sector'
    
    nat_wage = df.loc[df['naics'] == "------", 'wage'].values[0]
    bad_wage = ~df['wage'].between(0.1, 1e9)
    df.loc[bad_wage, 'wage'] = nat_wage
    df.loc[bad_wage, 'wage_f'] = 'nation'

    assert df['wage'].between(0.1, 1e9).all()
    df['wage'] = df['wage'].astype('int32')

    return df.reset_index()[['naics', 'wage', 'wage_f']]
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
t = {}
for y in range(1998, 2022):
    d = _get_wage_us(y)
    x = d['wage_f'].value_counts()
    x['national wage'] = d.loc[d['naics'] == "------", 'wage'].values[0]
    t[y] = x
t = pd.concat(t, axis=1).T.fillna(0).astype(int)[['national wage', 'nation-industry', 'nation-sector', 'nation']]
t
```

## state

```{code-cell} ipython3
:tags: [nbd-module]

def _get_wage_state(year):
    
    path = get_cbp_path('state', year)
    filters = [('lfo', '==', '-')] if year > 2009 else None
    df = pd.read_parquet(path, engine='pyarrow', columns=['fipstate', 'naics', 'emp', 'qp1'], filters=filters)

    assert not df.duplicated(['fipstate', 'naics']).any()

    df['wage'] = (df['qp1'] / df['emp'] * 4000).round()
    df['wage_f'] = pd.Series(dtype=pd.CategoricalDtype(['county-industry', 'state-industry', 'nation-industry', 'nation-sector', 'nation']))
    df['wage_f'] = 'state-industry'

    # national wages
    d = _get_wage_us(year)
    df = df.merge(d, 'left', 'naics', suffixes=('', '_nation'))
    # state-to-nation ratio
    d = df.query('naics == "------"').copy()
    d['state/nation'] = d['wage'] / d['wage_nation']
    d.loc[~d['wage'].between(0.1, 1e9), 'state/nation'] = 1
    df = df.merge(d[['fipstate', 'state/nation']], 'left', 'fipstate')
    # replace extreme state wages with nation
    bad_wage = ~df['wage'].between(0.1, 1e9)
    df.loc[bad_wage, 'wage'] = (df['wage_nation'] * df['state/nation']).round()
    df.loc[bad_wage, 'wage_f'] = df['wage_f_nation']

    assert df['wage'].between(0.1, 1e9).all()
    df['wage'] = df['wage'].astype('int32')

    return df[['fipstate', 'naics', 'wage', 'wage_f', 'state/nation']]
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
t = {}
for y in range(1998, 2022):
    d = _get_wage_state(y)
    x = d['wage_f'].value_counts()
    t[y] = x
t = pd.concat(t, axis=1).T.fillna(0).astype(int)
t
```

## county

```{code-cell} ipython3
:tags: [nbd-module]

def _get_wage_county(year):

    df = pd.read_parquet(
        get_cbp_path('county', year), engine='pyarrow', 
        columns=['fipstate', 'fipscty', 'naics', 'emp', 'qp1']
    )
    
    if year == 1999:
        # small number of duplicate records
        df.drop_duplicates(['fipstate', 'fipscty', 'naics'], inplace=True)
    
    assert not df.duplicated(['fipstate', 'fipscty', 'naics']).any()

    df['wage'] = (df['qp1'] / df['emp'] * 4000).round()
    df['wage_f'] = pd.Series(dtype=pd.CategoricalDtype(['county-industry', 'state-industry', 'nation-industry', 'nation-sector', 'nation']))
    df['wage_f'] = 'county-industry'

    # state wage
    d = _get_wage_state(year)
    df = df.merge(d, 'left', ['fipstate', 'naics'], suffixes=('', '_state'))
    # county-to-state ratio
    d = df.query('naics == "------"').copy()
    d['county/state'] = d['wage'] / d['wage_state']
    d.loc[~d['wage'].between(0.1, 1e9), 'county/state'] = 1
    df = df.merge(d[['fipstate', 'fipscty', 'county/state']], 'left', ['fipstate', 'fipscty'])
    # replace extreme county wages with state
    bad_wage = ~df['wage'].between(0.1, 1e9)
    df.loc[bad_wage, 'wage'] = (df['wage_state'] * df['county/state']).round()
    df.loc[bad_wage, 'wage_f'] = df['wage_f_state']

    assert df['wage'].between(0.1, 1e9).all()
    df['wage'] = df['wage'].astype('int32')

    return df[['fipstate', 'fipscty', 'naics', 'wage', 'wage_f', 'county/state']]
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
t = {}
for y in range(1998, 2022):
    d = _get_wage_county(y)
    x = d['wage_f'].value_counts()
    t[y] = x
    print(y, end=' ')
print()
t = pd.concat(t, axis=1).T.fillna(0).astype(int)
t
```

# iCBP

CBP with missing employment values replaced with EFSY.
Corresponding payroll is imputed from employment and state wage.
Raw values are kept in `cbp_emp`, `cbp_pay`, `efsy_lb` and `efsy_ub`.

TODO Does not work with SIC

```{code-cell} ipython3
:tags: [nbd-module]

@cache_pq(str(PATH['icbp_pq'] / '{}.pq'))
def get_icbp_df(year):
    ind_col = 'naics' if year > 1997 else 'sic'
    df = get_cbp_df('county', year)
    # df = df[['fipstate', 'fipscty', ind_col, 'est', 'emp', 'qp1', 'ap']]
    # rename applies before 2017
    df.rename(columns={'n1_4': 'n<5'}, inplace=True)
    
    df['industry'] = df[ind_col].str.replace('-', '').str.replace('/', '')
    df['ind_digits'] = df['industry'].str.len()
    
    # wage
    d = get_wage('county', year)
    df = df.merge(d, 'left', ['fipstate', 'fipscty', ind_col], indicator=True)
    log.debug(f'wage merge {year}:\n {df["_merge"].value_counts()}')
    del df['_merge']
    
    # EFSY ends in 2016
    if year > 2016:
        return df
    
    # add EFSY employment
    d = get_efsy_year_df(year)
    if year < 1998:
        d.rename(columns={'naics': 'sic'}, inplace=True)
    d.rename(columns={'lb': 'efsy_lb', 'ub': 'efsy_ub'}, inplace=True)

    df = df.merge(d, 'left', ['fipstate', 'fipscty', ind_col], indicator=True)
    log.debug(f'efsy merge {year}:\n {df["_merge"].value_counts()}')
    del df['_merge']

    # fill missing emp and ap in CBP
    df['cbp_emp'] = df['emp']
    df.loc[df['emp'] == 0, 'emp'] = (df['efsy_lb'] + df['efsy_ub']) / 2

    df['cbp_ap'] = df['ap']
    df.loc[df['ap'] == 0, 'ap'] = df['emp'] * df['wage'] / 1000
    
    return df


def get_icbp_path(year: int):
    """Return path to Parquet file, first creating the file if it does not exist."""
    path = PATH['icbp_pq'] / f'{year}.pq'
    if not path.exists():
        get_icbp_df(year)
        assert path.exists()
    return path

def _cleanup_get_icbp():
    p = PATH['icbp_pq']
    log.info(f'Removing {p}...')
    shutil.rmtree(p, ignore_errors=True)
```

```{code-cell} ipython3
:tags: []

# _cleanup_get_cbp_year()
```

# Summary

```{code-cell} ipython3
:tags: []

log.setLevel('INFO')
tb = {}
for year in range(1998, 2022):
    d = get_cbp_df('us', year)
    if 'lfo' in d:
        d = d.query('lfo == "-"')
    t = {
        'us': d.loc[d['naics'].str[:2] == '--', ['emp', 'ap']].iloc[0, :]
    }
    
    if year < 2017:
        d = pd.read_parquet(get_icbp_path(year), engine='pyarrow', columns=['ind_digits', 'emp', 'ap', 'cbp_emp', 'cbp_ap'])
        x = d.query('ind_digits == 6')[['cbp_emp', 'cbp_ap']].sum().rename(index={'cbp_emp': 'emp', 'cbp_ap': 'ap'})
        t['county %'] = round(100 * x / t['us'], 1)
        x = d.query('ind_digits == 6')[['emp', 'ap']].sum()
        t['efsy %'] = round(100 * x / t['us'], 1)
    else:
        d = pd.read_parquet(get_icbp_path(year), engine='pyarrow', columns=['ind_digits', 'emp', 'ap'])
        x = d.query('ind_digits == 6')[['emp', 'ap']].sum()
        t['county %'] = round(100 * x / t['us'], 1)
    t = pd.concat(t, axis=1)
    t['us'] = round(t['us'] / 1e6, 1)
    tb[year] = t.unstack()
tb = pd.concat(tb, axis=1).T
log.setLevel('DEBUG')
tb
```

## EFSY by industry

```{code-cell} ipython3
:tags: []

year = 2012

df = pd.read_parquet(
    get_icbp_path(year), 
    columns=['fipstate', 'fipscty', 'industry', 'est', 'emp', 'ap', 'wage', 'wage_f', 'qp1', 'cbp_emp', 'cbp_ap', 'efsy_ub'],
    filters=[('ind_digits', '==', 6)])
```

```{code-cell} ipython3
:tags: []

d1 = df.groupby('industry')[['est', 'emp', 'ap']].sum().add_prefix('efsy_').reset_index().rename(columns={'industry': 'naics'})
d2 = get_cbp_df('us', year)
d2 = d2.loc[(d2['lfo'] == '-') & d2['naics'].str[5].str.isdigit(), ['naics', 'est', 'emp', 'ap']].set_index('naics').add_prefix('nat_').reset_index()
d = d2.merge(d1)
for x in ['est', 'emp', 'ap']:
    d[f'efsy/nat_{x}'] = d[f'efsy_{x}'] / d[f'nat_{x}']
```

Employment is 98%-100% of national in over 99% of 6-digit industries.

```{code-cell} ipython3
:tags: []

d['efsy/nat_emp_bin'] = pd.cut(d['efsy/nat_emp'], np.arange(0.96, 1, 0.005))
(d['efsy/nat_emp_bin'].value_counts().sort_index() / len(d) * 100).round(2)
```

Payroll is much less aligned.

```{code-cell} ipython3
:tags: []

d['efsy/nat_ap_bin'] = pd.cut(d['efsy/nat_ap'], np.arange(0.7, 1.3, 0.05))
(d['efsy/nat_ap_bin'].value_counts().sort_index() / len(d) * 100).round(2)
```

```{code-cell} ipython3
:tags: []

d.plot.scatter(x='naics', y='efsy/nat_ap')
```

# Build this module

```{code-cell} ipython3
:tags: []

nbd.nb2mod('cbp.ipynb')
```
