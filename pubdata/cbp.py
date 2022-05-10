#!/usr/bin/env python
# coding: utf-8

import typing

import pandas as pd
import pyarrow
import pyarrow.dataset

from .reseng.util import download_file
from .reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'root': nbd.root,
    'source': nbd.root/'data/cbp/source/',
    'parquet': nbd.root/'data/cbp/parquet/'
}


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


def get_parquet(geo, cols=None, filters=None):
    path = PATH['parquet'] / geo
    part = pyarrow.dataset.partitioning(field_names=['year'])
    return pd.read_parquet(path, 'pyarrow', columns=cols, filters=filters,
                           partitioning=part)

