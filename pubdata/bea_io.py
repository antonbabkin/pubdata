#!/usr/bin/env python
# coding: utf-8

import sys
import zipfile
import typing
import shutil
import logging
from contextlib import redirect_stdout

import pandas as pd

from .reseng.util import download_file
from .reseng.monitor import log_start_finish
from .reseng.nbd import Nbd
from . import naics, cbp

nbd = Nbd('pubdata')


log = logging.getLogger('pubdata.bea_io')
log.handlers.clear()
log.addHandler(logging.StreamHandler(sys.stdout))


log.setLevel('INFO')
cbp.log.setLevel('INFO')

PATH = {
    'src': nbd.root / 'data/bea_io/src',
    'proc': nbd.root / 'data/bea_io/'
}


def _get_src(year):
    if year == 2022:
        url = 'https://apps.bea.gov/histdata/Releases/Industry/2022/GDP_by_Industry/Q2/Annual_September-29-2022/AllTablesSUP.zip'
        fnm = 'AllTablesSUP_2022q2.zip'
    elif year == 2023:
        url = 'https://apps.bea.gov/industry/iTables%20Static%20Files/AllTablesSUP.zip'
        fnm = 'AllTablesSUP_2023.zip'
    path = PATH['src'] / fnm
    if path.exists():
        log.debug(f'Source file already exists: {path}')
        return path
    log.debug(f'File {fnm} not found, attempting download from {url}')
    path.parent.mkdir(parents=True, exist_ok=True)
    download_file(url, PATH['src'], fnm)
    log.debug(f'File downloaded to {path}')
    # tables are read directly from Zip archive, without explicitcly extracting all files
    return path


def _read_table(src, spreadsheet, sheet, level, labels, skip_head, skip_foot):
    
    log.debug(f'Reading table from {src.name}/{spreadsheet}/{sheet}')
    
    with zipfile.ZipFile(src) as z:
        df = pd.read_excel(
            z.open(spreadsheet),
            sheet_name=sheet,
            header=None,
            dtype=str,
            skiprows=skip_head,
            skipfooter=skip_foot
        )
    
    # swap code and label rows for consistency with sec and sum
    if level == 'det':
        df.iloc[[0, 1], :] = df.iloc[[1, 0], :].values    

    row_names = df.iloc[2:, :2].values.tolist()
    col_names = df.iloc[:2, 2:].values.T.tolist()
    df.columns = df.iloc[1, :] if labels else df.iloc[0, :]
    df.index = df.iloc[:, 1] if labels else df.iloc[:, 0]
    df = df.iloc[2:, 2:]
    df = df.replace('...', None).astype('float64')
    
    return dict(table=df, row_names=row_names, col_names=col_names)


def get_sup(year, level, labels=False):
    """Supply table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2022 for "sec" and "sum"; 2007, 2012 or 2017 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """

    y = str(year)
    if year < 2017:
        src = _get_src(2022)
        if level == 'sec':
            x = _read_table(src, 'Supply_Tables_1997-2021_SEC.xlsx', y, level, labels, 5, 0)
        elif level == 'sum':
            x = _read_table(src, 'Supply_Tables_1997-2021_SUM.xlsx', y, level, labels, 5, 0)
        elif level == 'det':
            x = _read_table(src, 'Supply_2007_2012_DET.xlsx', y, level, labels, 4, 2)
    else:
        src = _get_src(2023)
        if level == 'sec':
            x = _read_table(src, 'Supply_Tables_2017-2022_Sector.xlsx', y, level, labels, 5, 0)
        elif level == 'sum':
            x = _read_table(src, 'Supply_Tables_2017-2022_Summary.xlsx', y, level, labels, 5, 0)
        elif level == 'det':
            x = _read_table(src, 'Supply_2017_DET.xlsx', y, level, labels, 4, 2)

    x['table'].index.name = 'commodity'
    x['table'].columns.name = 'industry'
    
    return x

@log_start_finish
def test_get_sup():
    for year in range(1997, 2023):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012, 2017]:
                continue
            for labels in [False, True]:
                x = get_sup(year, level, labels)
                print(year, level, labels, x['table'].shape)
                assert len(x['table']) > 0


def get_use(year, level, labels=False):
    """Use table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2022 for "sec" and "sum"; 2007, 2012 or 2017 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """
    
    y = str(year)
    if year < 2017:
        src = _get_src(2022)
        if level == 'sec':
            x = _read_table(src, 'Use_SUT_Framework_1997-2021_SECT.xlsx', y, level, labels, 5, 0)
        elif level == 'sum':
            x = _read_table(src, 'Use_SUT_Framework_1997-2021_SUM.xlsx', y, level, labels, 5, 0)
        elif level == 'det':
            x = _read_table(src, 'Use_SUT_Framework_2007_2012_DET.xlsx', y, level, labels, 4, 2)
    else:
        src = _get_src(2023)
        if level == 'sec':
            x = _read_table(src, 'Use_Tables_Supply-Use_Framework_2017-2022_Sector.xlsx', y, level, labels, 5, 0)
        elif level == 'sum':
            x = _read_table(src, 'Use_Tables_Supply-Use_Framework_2017-2022_Summary.xlsx', y, level, labels, 5, 0)
        elif level == 'det':
            x = _read_table(src, 'Use_SUT_Framework_2017_DET.xlsx', y, level, labels, 4, 2)

    x['table'].index.name = 'commodity'
    x['table'].columns.name = 'industry'
    
    return x


@log_start_finish
def test_get_use():
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012, 2017]:
                continue
            for labels in [False, True]:
                x = get_use(year, level, labels)
                print(year, level, x['table'].shape)
                assert len(x['table']) > 0


def get_ixi(year, level, labels):
    """Industry-by-industry Total requirements table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2021 for "sec" and "sum"; 2007 or 2012 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """
    
    src = _get_src(2022)
    y = str(year)
    
    if level == 'sec':
        x = _read_table(src, 'IxI_TR_1997-2021_PRO_SEC.xlsx', y, level, labels, 5, 2)
    elif level == 'sum':
        x = _read_table(src, 'IxI_TR_1997-2021_PRO_SUM.xlsx', y, level, labels, 5, 2)
    elif level == 'det':
        x = _read_table(src, 'IxI_TR_2007_2012_PRO_DET.xlsx', y, level, labels, 3, 0)

    x['table'].index.name = 'industry'
    x['table'].columns.name = 'industry'
    
    return x

@log_start_finish
def test_get_ixi():
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                d = get_ixi(year, level, labels)['table']
                print(year, level, labels, d.shape)
                assert len(d) > 0


def get_ixc(year, level, labels):
    """Industry-by-commodity Total requirements table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2021 for "sec" and "sum"; 2007 or 2012 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """
    
    src = _get_src(2022)
    y = str(year)
    
    if level == 'sec':
        x = _read_table(src, 'IxC_TR_1997-2021_PRO_SEC.xlsx', y, level, labels, 5, 2)
    elif level == 'sum':
        x = _read_table(src, 'IxC_TR_1997-2021_PRO_SUM.xlsx', y, level, labels, 5, 2)
    elif level == 'det':
        x = _read_table(src, 'IxC_TR_2007_2012_PRO_DET.xlsx', y, level, labels, 3, 0)

    x['table'].index.name = 'industry'
    x['table'].columns.name = 'commodity'
    
    return x

@log_start_finish
def test_get_ixc():
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                d = get_ixc(year, level, labels)['table']
                print(year, level, labels, d.shape)
                assert len(d) > 0


def get_cxc(year, level, labels):
    """Commodity-by-commodity Total requirements table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2021 for "sec" and "sum"; 2007 or 2012 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """
    
    src = _get_src(2022)
    y = str(year)
    
    if level == 'sec':
        x = _read_table(src, 'CxC_TR_1997-2021_PRO_SEC.xlsx', y, level, labels, 5, 2)
    elif level == 'sum':
        x = _read_table(src, 'CxC_TR_1997-2021_PRO_SUM.xlsx', y, level, labels, 5, 2)
    elif level == 'det':
        x = _read_table(src, 'CxC_TR_2007_2012_PRO_DET.xlsx', y, level, labels, 3, 0)

    x['table'].index.name = 'commodity'
    x['table'].columns.name = 'commodity'
    
    return x

@log_start_finish
def test_get_cxc():
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                d = get_cxc(year, level, labels)['table']
                print(year, level, labels, d.shape)
                assert len(d) > 0


def get_naics_concord(year):
    """Return dataframe with BEA-NAICS concordance table.
    `year` can be 2012 or 2017.
    """
    
    if year == 2012:
        src = _get_src(2022)
        spreadsheet = 'Use_SUT_Framework_2007_2012_DET.xlsx'
    elif year == 2017:
        src = _get_src(2023)
        spreadsheet = 'Use_SUT_Framework_2017_DET.xlsx'
    sheet = 'NAICS Codes'
    log.debug(f'Reading table from {src.name}/{spreadsheet}/{sheet}')
    
    with zipfile.ZipFile(src) as z:
        df = pd.read_excel(
            z.open(spreadsheet),
            sheet_name=sheet,
            dtype=str,
            skiprows=4,
            skipfooter=6
        )

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

    assert (df[['sector', 'summary', 'u_summary', 'detail']].notna().sum(1) == 1).all(),\
        'Code in more than one column'
    assert df['description'].notna().all()

    # pad higher level codes
    df['sector'] = df['sector'].fillna(method='ffill')
    df['summary'] = df.groupby('sector', sort=False)['summary'].fillna(method='ffill')
    df['u_summary'] = df.groupby(['sector', 'summary'], sort=False)['u_summary'].fillna(method='ffill')

    df['naics'] = df['naics'].str.strip().apply(_split_codes)
    df = df.explode('naics', ignore_index=True)
    
    # drop non-existent NAICS codes, created from expanding ranges like "5174-9"
    feasible_naics_codes = ['23*', 'n.a.'] + naics.get_df(year, 'code')['CODE'].to_list()
    df = df[df['naics'].isna() | df['naics'].isin(feasible_naics_codes)]
    
    df[df.isna()] = None
    df = df.reset_index(drop=True)
    
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

def test_get_naics_concord():
    
    assert _split_codes('1') == ['1']
    assert _split_codes('1, 2') == ['1', '2']
    assert _split_codes('1-3') == ['1', '2', '3']
    assert _split_codes('1-3, 5') == ['1', '2', '3', '5']
    assert _split_codes('1-3, 5-7') == ['1', '2', '3', '5', '6', '7']
    
    d = get_naics_concord(2012)
    assert len(d) > 0
    d = get_naics_concord(2017)
    assert len(d) > 0


@log_start_finish
def test_all():
    test_get_sup()
    test_get_use()
    test_get_ixi()
    test_get_ixc()
    test_get_cxc()
    test_get_naics_concord()

