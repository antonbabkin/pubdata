#!/usr/bin/env python
# coding: utf-8

import sys
import typing
import zipfile
from collections import defaultdict
import logging
import shutil

import pandas as pd
import pyarrow
import pyarrow.dataset

from . import naics
from .reseng.util import download_file
from .reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'root': nbd.root,
    'src': nbd.root / 'data/cbp/src/',
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

def cleanup(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['src'], ignore_errors=True)
        shutil.rmtree(PATH['src_efsy'], ignore_errors=True)
    print('Removing processed files...')
    shutil.rmtree(PATH['parquet'], ignore_errors=True)
    shutil.rmtree(PATH['cbp_panel'], ignore_errors=True)
    shutil.rmtree(PATH['efsy'], ignore_errors=True)


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
    path = PATH['src']/f'{geo}/{year}.{ext}'
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

    df = pd.read_csv(_get_cbp_src(geo, year), dtype=dtype, na_values=na_values)
    df.columns = df.columns.str.lower()
    return df


def get_cbp_raw_pq(geo: typing.Literal['us', 'state', 'county'], year: int):
    """Return path to Parquet file, first creating the file if it does not exist."""
    path = PATH['cbp_raw'] / f'{geo}/{year}.pq'
    if not path.exists():
        get_cbp_raw(geo, year)
        assert path.exists()
    return path

def preproc_get_cbp_raw():
    for year in range(1986, 2022):
        for geo in ['us', 'state', 'county']:
            get_cbp_raw(geo, year)
        log.info(f'preproc_get_cbp_raw year finished {year}')


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

    df = pd.read_csv(_get_cbp_src(geo, year), usecols=dt.keys(), dtype=dt, na_values=na_val)
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


def get_parquet(geo, cols=None, filters=None):
    path = PATH['parquet'] / geo
    part = pyarrow.dataset.partitioning(field_names=['year'])
    return pd.read_parquet(path, 'pyarrow', columns=cols, filters=filters,
                           partitioning=part)


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


# interface function
def get_wage(geo: typing.Literal['us', 'state', 'county'], year: int):
    if geo == 'us':
        return _get_wage_us(year)
    if geo == 'state':
        return _get_wage_state(year)
    if geo == 'county':
        return _get_wage_county(year)


def _get_wage_us(year):

    if year < 2008:
        df = pd.read_parquet(
            get_cbp_raw_pq('us', year), engine='pyarrow', 
            columns=['naics', 'emp', 'qp1']
        )    
    else:
        df = pd.read_parquet(
            get_cbp_raw_pq('us', year), engine='pyarrow',
            columns=['lfo', 'naics', 'emp', 'qp1']
        )
        df = df.query('lfo == "-"').copy()

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


def _get_wage_state(year):

    if year < 2010:
        df = pd.read_parquet(
            get_cbp_raw_pq('state', year), engine='pyarrow', 
            columns=['fipstate', 'naics', 'emp', 'qp1']
        )    
    else:
        df = pd.read_parquet(
            get_cbp_raw_pq('state', year), engine='pyarrow',
            columns=['fipstate', 'lfo', 'naics', 'emp', 'qp1']
        )
        df = df.query('lfo == "-"').copy()

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

    return df[['fipstate', 'naics', 'wage', 'wage_f']]


def _get_wage_county(year):

    df = pd.read_parquet(
        get_cbp_raw_pq('county', year), engine='pyarrow', 
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

    return df[['fipstate', 'fipscty', 'naics', 'wage', 'wage_f']]


@cache_pq(str(PATH['cbp_panel'] / '{}.pq'))
def get_cbp_year(year):
    ind_col = 'naics' if year > 1997 else 'sic'
    df = get_cbp_raw('county', year)
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
    d = get_efsy_year(year)
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


def get_cbp_year_pq(year: int):
    """Return path to Parquet file, first creating the file if it does not exist."""
    path = PATH['cbp_panel'] / f'{year}.pq'
    if not path.exists():
        get_cbp_year(year)
        assert path.exists()
    return path

def _cleanup_get_cbp_year():
    p = PATH['cbp_panel']
    log.info(f'Removing {p}...')
    shutil.rmtree(p, ignore_errors=True)

