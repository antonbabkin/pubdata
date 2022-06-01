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
    (1997, 'code'): 'https://www2.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions/naics.txt',
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
        if year == 1997:
            df = pd.read_fwf(src_file, widths=(8, 999), dtype=str, skiprows=2, names=['CODE', 'TITLE'])
            df['CODE'] = df['CODE'].str.strip('-/')
            df['CODE'] = df['CODE'].replace({'31': '31-33', '44': '44-45', '48': '48-49'})
            # drop code "99" - unclassified establishments in CBP
            df = df[df['CODE'] != '99']
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
    
    df = df.reset_index(drop=True)
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


_concord_src_urls = {
    ('naics', 1997, 'naics', 2002): f'{_src_url_base}concordances/1997_NAICS_to_2002_NAICS.xls',
    ('naics', 2002, 'naics', 1997): f'{_src_url_base}concordances/2002_NAICS_to_1997_NAICS.xls',
    ('naics', 2002, 'naics', 2007): f'{_src_url_base}concordances/2002_to_2007_NAICS.xls',
    ('naics', 2007, 'naics', 2002): f'{_src_url_base}concordances/2007_to_2002_NAICS.xls',
    ('naics', 2007, 'naics', 2012): f'{_src_url_base}concordances/2007_to_2012_NAICS.xls',
    ('naics', 2012, 'naics', 2007): f'{_src_url_base}concordances/2012_to_2007_NAICS.xls',
    ('naics', 2012, 'naics', 2017): f'{_src_url_base}concordances/2012_to_2017_NAICS.xlsx',
    ('naics', 2017, 'naics', 2012): f'{_src_url_base}concordances/2017_to_2012_NAICS.xlsx',
    ('naics', 2017, 'naics', 2022): f'{_src_url_base}concordances/2017_to_2022_NAICS.xlsx',
    ('naics', 2022, 'naics', 2017): f'{_src_url_base}concordances/2022_to_2017_NAICS.xlsx',
}

def get_concordance_src(fro: str, fro_year: int, to: str, to_year: int):
    """Download concordance source file and return local path.
    Concordance table from (`fro`, `fro_year`) to (`to`, `to_year`),
    e.g. from ("naics", 2017) to ("naics", 2022).
    """
    
    assert (fro, fro_year, to, to_year) in _concord_src_urls, f'Concordance source file not available.'
    url = _concord_src_urls[(fro, fro_year, to, to_year)]
    fname = urllib.parse.urlparse(url).path
    fname = urllib.parse.unquote(pathlib.Path(fname).name)
    
    path = PATH['source']/f'{fro_year}/{fname}'
    if path.exists(): return path

    download_file(url, path.parent, path.name)
    return path


def get_concordance_df(fro: str, fro_year: int, to: str, to_year: int):
    """Return concordance dataframe built from source file.
    Concordance table from (`fro`, `fro_year`) to (`to`, `to_year`),
    e.g. from ("naics", 2017) to ("naics", 2022).
    """
    
    src_file = get_concordance_src(fro, fro_year, to, to_year)

    c_fro = f'{fro}_{fro_year}'.upper()
    t_fro = f'TITLE_{fro_year}'.upper()
    c_to = f'{to}_{to_year}'.upper()
    t_to = f'TITLE_{to_year}'.upper()

    if (fro == to == 'naics') and ((fro_year == 1997) or (to_year == 1997)):
        df = pd.read_excel(src_file, sheet_name=1, dtype=str, skipfooter=1)
        df.columns = [c_fro, t_fro, c_to, t_to, 'EXPLANATION']
        
    if (fro == to == 'naics') and (fro_year > 1997) and (to_year > 1997):
        df = pd.read_excel(src_file, dtype=str, skiprows=3, header=None)
        # columns beyond first four have no data
        for c in df.iloc[:, 4:]:
            assert (df[c].isna() | df[c].str.isspace()).all()
        df = df.iloc[:, :4]
        df.columns = [c_fro, t_fro, c_to, t_to]
        
    # flag link types
    if (fro == to == 'naics'):
        dup0 = df[f'DUP_{fro_year}'] = df.duplicated(c_fro, False)
        dup1 = df[f'DUP_{to_year}'] = df.duplicated(c_to, False)

        flag = f'FLAG_{fro_year}_TO_{to_year}'
        df[flag] = ''
        df.loc[~dup0 & ~dup1 & (df[c_fro] == df[c_to]), flag] = '1-to-1 same'
        df.loc[~dup0 & ~dup1 & (df[c_fro] != df[c_to]), flag] = '1-to-1 diff'
        df.loc[~dup0 & dup1, flag] = 'join'
        # splits
        d = df[dup0].copy()
        clean = ~d.groupby(c_fro)[f'DUP_{to_year}'].transform('max')
        d.loc[clean, flag] = 'clean split'
        d.loc[~clean, flag] = 'messy split'
        df.loc[dup0, flag] = d[flag]        

    df = df.sort_values([c_fro, c_to], ignore_index=True)
    
    return df

