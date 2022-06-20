#!/usr/bin/env python
# coding: utf-8

import pathlib

import pandas as pd

from .reseng.util import download_file
from .reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'root': nbd.root,
    'source': nbd.root/'data/source/bds/'
}


def get_source(key: str = ''):
    if key != '': key = '_' + key
    url = f'https://www2.census.gov/programs-surveys/bds/tables/time-series/bds2019{key}.csv'
    file_path = PATH['source'] / pathlib.Path(url).name
    if file_path.exists(): return file_path
    return download_file(url, PATH['source'])


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

