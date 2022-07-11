#!/usr/bin/env python
# coding: utf-8

import zipfile

import pandas as pd

from .reseng.util import download_file
from .reseng.nbd import Nbd
from . import naics, cbp

nbd = Nbd('pubdata')
PATH = {
    'source': nbd.root/'data/source/bea_io/',
    'naics_codes': nbd.root/'data/bea_io/naics_codes.csv'
}
NAICS_REV = 2012


def get_source_files():
    if (PATH['source'] / 'AllTablesSUP').exists(): return
    url = 'https://apps.bea.gov/industry/iTables Static Files/AllTablesSUP.zip'
    f = download_file(url, PATH['source'])
    with zipfile.ZipFile(f) as z:
        z.extractall(PATH['source'])


def get_naics_df():
    path = PATH['naics_codes']
    if path.exists():
        return pd.read_csv(path, dtype=str)
    
    get_source_files()
    df = pd.read_excel(PATH['source']/'AllTablesSUP/Use_SUT_Framework_2007_2012_DET.xlsx',
                       sheet_name='NAICS Codes',
                       skiprows=4,
                       skipfooter=6,
                       dtype=str)

    df.columns = ['sector', 'summary', 'u_summary', 'detail', 'description', 'notes', 'naics']
    df = df.drop(columns='notes')
    df = df.dropna(how='all')

    # move descriptions to single column
    df['description'].fillna(df['detail'], inplace=True)
    df['description'].fillna(df['u_summary'], inplace=True)
    df['description'].fillna(df['summary'], inplace=True)

    df.loc[df['sector'].notna(), 'summary'] = None
    df.loc[df['summary'].notna(), 'u_summary'] = None
    df.loc[df['u_summary'].notna(), 'detail'] = None

    assert (df[['sector', 'summary', 'u_summary', 'detail']].notna().sum(1) == 1).all(),        'Code in more than one column'
    assert df['description'].notna().all()

    # pad higher level codes
    df['sector'] = df['sector'].fillna(method='ffill')
    df['summary'] = df.groupby('sector')['summary'].fillna(method='ffill')
    df['u_summary'] = df.groupby(['sector', 'summary'])['u_summary'].fillna(method='ffill')

    df['naics'] = df['naics'].str.strip().apply(split_codes)
    df = df.explode('naics', ignore_index=True)
    
    # drop non-existent NAICS codes, created from expanding ranges like "5174-9"
    feasible_naics_codes = ['23*', 'n.a.'] + naics.get_df(NAICS_REV, 'code')['CODE'].to_list()
    df = df[df['naics'].isna() | df['naics'].isin(feasible_naics_codes)]
    
    df[df.isna()] = None
    df = df.reset_index(drop=True)
    df = df.rename(columns=str.upper)
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return df

def split_codes(codes):
    if pd.isna(codes):
        return [codes]
    def expand_dash(codes):
        if '-' in codes:
            first, last = codes.split('-')
            assert len(last) == 1
            last = int(first[:-1] + last)
            first = int(first)
            return [str(c) for c in range(first, last+1)]
        else:
            return [codes]

    codes = codes.split(', ')
    codes = sum((expand_dash(c) for c in codes), [])
    return codes

assert split_codes('1') == ['1']
assert split_codes('1, 2') == ['1', '2']
assert split_codes('1-3') == ['1', '2', '3']
assert split_codes('1-3, 5') == ['1', '2', '3', '5']
assert split_codes('1-3, 5-7') == ['1', '2', '3', '5', '6', '7']

