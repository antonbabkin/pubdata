#!/usr/bin/env python
# coding: utf-8

import typing
import pathlib
import urllib

import pandas as pd

from .reseng.util import download_file
from .reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'source': nbd.root/'data/source/naics',
}


_src_url_base = 'https://www.census.gov/naics/'
_src_urls = {
    (2002, 'code'): f'{_src_url_base}reference_files_tools/2002/naics_2_6_02.txt',
    (2007, 'code'): f'{_src_url_base}reference_files_tools/2007/naics07.txt',
    (2012, 'code'): f'{_src_url_base}2012NAICS/2-digit_2012_Codes.xls',
    (2017, 'code'): f'{_src_url_base}2017NAICS/2-6%20digit_2017_Codes.xlsx',
    (2022, 'code'): f'{_src_url_base}2022NAICS/2-6%20digit_2022_Codes.xlsx',
    (2007, 'index'): f'{_src_url_base}2007NAICS/2007_NAICS_Index_File.xls',
    (2012, 'index'): f'{_src_url_base}2012NAICS/2012_NAICS_Index_File.xls',
    (2017, 'index'): f'{_src_url_base}2017NAICS/2017_NAICS_Index_File.xlsx',
    (2022, 'index'): f'{_src_url_base}2022NAICS/2022_NAICS_Index_File.xlsx',
    (2017, 'descriptions'): f'{_src_url_base}2017NAICS/2017_NAICS_Descriptions.xlsx',
    (2022, 'descriptions'): f'{_src_url_base}2022NAICS/2022_NAICS_Descriptions.xlsx',
    (2017, 'summary'): f'{_src_url_base}2017NAICS/2017_NAICS_Structure_Summary_Table.xlsx',
    (2022, 'summary'): f'{_src_url_base}2022NAICS/2022_NAICS_Structure_Summary_Table.xlsx',
}

def get_src(year: typing.Literal[2002, 2007, 2012, 2017, 2022],
            kind: typing.Literal['code', 'index', 'descriptions', 'summary']):
    """Download source file and return local path."""
    
    assert (year, kind) in _src_urls, f'Source file not available.'
    url = _src_urls[(year, kind)]
    fname = urllib.parse.urlparse(url).path
    fname = urllib.parse.unquote(pathlib.Path(fname).name)
    
    path = PATH['source']/f'{year}/{fname}'
    if path.exists(): return path

    download_file(url, path.parent, path.name)
    return path


def get_df(year: typing.Literal[2002, 2007, 2012, 2017, 2022],
           kind: typing.Literal['code', 'index', 'descriptions', 'summary']):
    """Return tidy dataframe built from source file."""
    
    src_file = get_src(year, kind)
    
    if kind == 'code':
        if year == 2002:
            df = pd.read_fwf(src_file, widths=(8, 999), dtype=str, skiprows=5, names=['CODE', 'TITLE'])
        elif year == 2007:
            df = pd.read_fwf(src_file, widths=(8, 8, 999), dtype=str, skiprows=2,
                             names=['SEQ_NO', 'CODE', 'TITLE'], usecols=['CODE', 'TITLE'])
            df['TITLE'] = df['TITLE'].str.strip('"')
        elif year in [2012, 2017, 2022]:
            df = pd.read_excel(src_file, dtype=str, skiprows=2, header=None)
            df = df.iloc[:, [1,2]] 
            df.columns = ['CODE', 'TITLE']
        
        assert (df['CODE'].isin(['31-33', '44-45', '48-49']) | df['CODE'].str.isdigit()).all()
        
        df['DIGITS'] = df['CODE'].str.len()
        df.loc[df['CODE'] == '31-33', 'DIGITS'] = 2
        df.loc[df['CODE'] == '44-45', 'DIGITS'] = 2
        df.loc[df['CODE'] == '48-49', 'DIGITS'] = 2
        assert df['DIGITS'].isin([2, 3, 4, 5, 6]).all()

        df.loc[df['DIGITS'] == 2, 'CODE_2'] = df['CODE']
        df['CODE_2'] = df['CODE_2'].fillna(method='ffill')
        df.loc[df['DIGITS'] == 3, 'CODE_3'] = df['CODE']
        df['CODE_3'] = df.groupby('CODE_2')['CODE_3'].fillna(method='ffill')
        df.loc[df['DIGITS'] == 4, 'CODE_4'] = df['CODE']
        df['CODE_4'] = df.groupby('CODE_3')['CODE_4'].fillna(method='ffill')
        df.loc[df['DIGITS'] == 5, 'CODE_5'] = df['CODE']
        df['CODE_5'] = df.groupby('CODE_4')['CODE_5'].fillna(method='ffill')
        df.loc[df['DIGITS'] == 6, 'CODE_6'] = df['CODE']
        
    elif kind == 'index':
        df = pd.read_excel(src_file, names=['CODE', 'INDEX_ITEM'], dtype=str)
        # at the bottom of the table are ****** codes with comments for a few industries.
        df = df[df['CODE'] != '******']
        assert df['CODE'].str.isdigit().all()
        assert (df['CODE'].str.len() == 6).all()
    elif kind == 'descriptions':
        df = pd.read_excel(src_file, names=['CODE', 'TITLE', 'DESCRIPTION'], dtype=str)
        assert (df['CODE'].isin(['31-33', '44-45', '48-49']) | df['CODE'].str.isdigit()).all()
    elif kind == 'summary':
        df = pd.read_excel(src_file, header=None).fillna('')
        df.columns = pd.MultiIndex.from_frame(df.head(2).T, names=['', ''])
        df = df.drop(index=[0,1]).reset_index(drop=True)
        df.iloc[:, 2:] = df.iloc[:, 2:].astype(int)
        df['Sector'] = df['Sector'].astype(str)
    
    return df


def compute_structure_summary(year):
    """Return dataframe with total counts of classes at every level by sector."""
    df = get_df(year, 'code')
    t = df.loc[df['DIGITS'] == 2, ['CODE', 'TITLE']]
    t.columns = ['Sector', 'Name']
    t = t.set_index('Sector')
    t['Subsectors (3-digit)'] = df.groupby('CODE_2')['CODE_3'].nunique()
    t['Industry Groups (4-digit)'] = df.groupby('CODE_2')['CODE_4'].nunique()
    t['NAICS Industries (5-digit)'] = df.groupby('CODE_2')['CODE_5'].nunique()
    same_as_5d = df['CODE_6'].str[-1] == '0'
    t['6-digit Industries (U.S. Detail)'] = df[~same_as_5d].groupby('CODE_2')['CODE_6'].nunique()
    t['6-digit Industries (Same as 5-digit)'] = df[same_as_5d].groupby('CODE_2')['CODE_6'].nunique()
    t['6-digit Industries (Total)'] = df.groupby('CODE_2')['CODE_6'].nunique()
    totals = t.iloc[:, 1:].sum()
    totals['Name'] = 'Total'
    t.loc['', :] = totals
    t.iloc[:, 1:] = t.iloc[:, 1:].fillna(0).astype(int)
    # 6-digit: us_detail + same_as_5digit == total
    assert (t.iloc[:, -3] + t.iloc[:, -2] == t.iloc[:, -1]).all()
    t = t.reset_index()
    return t

