#!/usr/bin/env python
# coding: utf-8

import shutil

import pandas as pd

from .reseng.util import download_file
from .reseng.caching import simplecache
from .reseng.nbd import Nbd
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


def test_all(redownload=False):
    cleanup(redownload)
    _get_src_series_register()
    _get_src_nipa_annual()
    d = get_price_index_df()
    assert (d.loc[2012] == 100).all()

