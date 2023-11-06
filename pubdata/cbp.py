#!/usr/bin/env python
# coding: utf-8

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

from . import naics
from .reseng.util import download_file
from .reseng.nbd import Nbd

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

