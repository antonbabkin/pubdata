#!/usr/bin/env python
# coding: utf-8

import zipfile
import typing
import shutil
from contextlib import redirect_stdout

import pandas as pd

from .reseng.util import download_file
from .reseng.monitor import log_start_finish
from .reseng.nbd import Nbd
from . import naics, cbp

nbd = Nbd('pubdata')

NAICS_REV = 2012


PATH = {
    'source': nbd.root / 'data/source/bea_io/',
    'proc': nbd.root / 'data/bea_io/',
    'naics_codes': nbd.root / 'data/bea_io/naics_codes.csv'
}

def init_dirs():
    """Create necessary directories."""
    PATH['source'].mkdir(parents=True, exist_ok=True)
    PATH['proc'].mkdir(parents=True, exist_ok=True)
    
def cleanup(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['source'])
    print('Removing processed files...')
    shutil.rmtree(PATH['proc'])


def get_source_files():
    init_dirs()
    src_dir = PATH['source'] / 'AllTablesSUP'
    if src_dir.exists(): return

    print('Downloading source files...')
    url = 'https://apps.bea.gov/industry/iTables Static Files/AllTablesSUP.zip'
    f = download_file(url, PATH['source'])
    with zipfile.ZipFile(f) as z:
        z.extractall(src_dir)
    print('Source files downloaded and extracted.')


def _read_table(file, sheet, level, labels, skip_head, skip_foot):
    get_source_files()
    
    src_file = PATH['source'] / 'AllTablesSUP' / file
    
    df = pd.read_excel(src_file, sheet, dtype=str, header=None, 
                       skiprows=skip_head, skipfooter=skip_foot)
    
    # swap code and label rows for consistency with sec and sum
    if level == 'det':
        df.iloc[[0, 1], :] = df.iloc[[1, 0], :].values
    if labels:
        rows = df.iloc[2:, 1]
        cols = df.iloc[1, 2:]
    else:
        rows = df.iloc[2:, 0]
        cols = df.iloc[0, 2:]

    df = pd.DataFrame(df.iloc[2:, 2:].values, index=rows, columns=cols)
    df = df.replace('...', None).astype('float64')

    assert not df.index.duplicated().any()
    assert not df.columns.duplicated().any()

    return df


def get_sup(year: int,
            level: typing.Literal['sec', 'sum', 'det'],
            labels: bool = False):
    """Return dataframe from "Supply_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()
    
    if level == 'sec':
        df = _read_table('Supply_Tables_1997-2021_SEC.xlsx', str(year), level, labels, 5, 0)
    elif level == 'sum':
        df = _read_table('Supply_Tables_1997-2021_SUM.xlsx', str(year), level, labels, 5, 0)
    elif level == 'det':
        df = _read_table('Supply_2007_2012_DET.xlsx', str(year), level, labels, 4, 2)

    df.index.name = 'commodity'
    df.columns.name = 'industry'
    
    return df

@log_start_finish
def test_get_sup(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_sup(year, level, labels)
                assert len(d) > 0


def get_use(year: int,
                level: typing.Literal['sec', 'sum', 'det'],
                labels: bool = False):
    """Return dataframe from "Use_SUT_Framework_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()
    
    if level == 'sec':
        df = _read_table('Use_SUT_Framework_1997-2021_SECT.xlsx', str(year), level, labels, 5, 0)
    elif level == 'sum':
        df = _read_table('Use_SUT_Framework_1997-2021_SUM.xlsx', str(year), level, labels, 5, 0)
    elif level == 'det':
        df = _read_table('Use_SUT_Framework_2007_2012_DET.xlsx', str(year), level, labels, 4, 2)

    df.index.name = 'commodity'
    df.columns.name = 'industry'
    
    return df


@log_start_finish
def test_get_use(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_use(year, level, labels)
                assert len(d) > 0


def get_ixi(year: typing.Literal[2007, 2012],
            level: typing.Literal['sec', 'sum', 'det'],
            labels: bool = False):
    """Return dataframe from "IxI_TR_*_PRO_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()

    if level == 'sec':
        df = _read_table('IxI_TR_1997-2021_PRO_SEC.xlsx', str(year), level, labels, 5, 2)
    elif level == 'sum':
        df = _read_table('IxI_TR_1997-2021_PRO_SUM.xlsx', str(year), level, labels, 5, 2)
    elif level == 'det':
        df = _read_table('IxI_TR_2007_2012_PRO_DET.xlsx', str(year), level, labels, 3, 0)

    df.index.name = 'industry'
    df.columns.name = 'industry'
    
    return df

@log_start_finish
def test_get_ixi(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_ixi(year, level, labels)
                assert len(d) > 0


def get_ixc(year: typing.Literal[2007, 2012],
            level: typing.Literal['sec', 'sum', 'det'],
            labels: bool = False):
    """Return dataframe from "IxC_TR_*_PRO_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()

    if level == 'sec':
        df = _read_table('IxC_TR_1997-2021_PRO_SEC.xlsx', str(year), level, labels, 5, 2)
    elif level == 'sum':
        df = _read_table('IxC_TR_1997-2021_PRO_SUM.xlsx', str(year), level, labels, 5, 2)
    elif level == 'det':
        df = _read_table('IxC_TR_2007_2012_PRO_DET.xlsx', str(year), level, labels, 3, 0)

    df.index.name = 'industry'
    df.columns.name = 'commodity'
    
    return df    


@log_start_finish
def test_get_ixc(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_ixc(year, level, labels)
                assert len(d) > 0


def get_cxc(year: typing.Literal[2007, 2012],
            level: typing.Literal['sec', 'sum', 'det'],
            labels: bool = False):
    """Return dataframe from "CxC_TR_*_PRO_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()
    
    if level == 'sec':
        df = _read_table('CxC_TR_1997-2021_PRO_SEC.xlsx', str(year), level, labels, 5, 2)
    elif level == 'sum':
        df = _read_table('CxC_TR_1997-2021_PRO_SUM.xlsx', str(year), level, labels, 5, 2)
    elif level == 'det':
        df = _read_table('CxC_TR_2007_2012_PRO_DET.xlsx', str(year), level, labels, 3, 0)

    df.index.name = 'commodity'
    df.columns.name = 'commodity'
    
    return df

@log_start_finish
def test_get_cxc(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_cxc(year, level, labels)
                assert len(d) > 0


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

    df['naics'] = df['naics'].str.strip().apply(_split_codes)
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

def _split_codes(codes):
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

def test_get_naics_df(redownload=False):
    cleanup(redownload)
    
    assert _split_codes('1') == ['1']
    assert _split_codes('1, 2') == ['1', '2']
    assert _split_codes('1-3') == ['1', '2', '3']
    assert _split_codes('1-3, 5') == ['1', '2', '3', '5']
    assert _split_codes('1-3, 5-7') == ['1', '2', '3', '5', '6', '7']
    
    d = get_naics_df()
    assert len(d) > 0


@log_start_finish
def test_all(redownload=False):
    test_get_sup(redownload)
    test_get_use(redownload)
    test_get_ixi(redownload)
    test_get_ixc(redownload)
    test_get_cxc(redownload)
    test_get_naics_df(redownload)

