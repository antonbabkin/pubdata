#!/usr/bin/env python
# coding: utf-8

import pandas as pd

from .reseng.util import download_file
from .reseng.nbd import Nbd
from . import geography

nbd = Nbd('pubdata')

PATH = {
    'source': nbd.root / 'data/source/ers_rurality/',
    'ruc': nbd.root / 'data/ers_rurality/ruc.csv',
    'ruc_doc': nbd.root / 'data/ers_rurality/ruc_doc.txt',
    'ui': nbd.root / 'data/ers_rurality/ui.csv',
    'ui_doc': nbd.root / 'data/ers_rurality/ui_doc.txt',
    'ruca': nbd.root / 'data/ers_rurality/ruca.csv',
    'ruca_doc': nbd.root / 'data/ers_rurality/ruca_doc.txt'
}


def get_ruc_df():
    """Return `pandas.DataFrame` of Rural-Urban Continuum codes for all years."""
    path = PATH['ruc']
    if not path.exists():
        print(f'RUC data not found at "{path}", attempting to download and construct...')
        download_and_combine_ruc()
    df = pd.read_csv(path, dtype='str')
    for c in ['RUC_YEAR', 'POPULATION_YEAR', 'POPULATION', 'PERCENT_NONMETRO_COMMUTERS']:
        df[c] = pd.to_numeric(df[c])
    cats = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    df['RUC_CODE'] = pd.Categorical(df['RUC_CODE'], cats, True)
    return df

def download_and_combine_ruc():
    """Download Rural-Urban Continuum codes and documentation.
    Combine all years of data into single CSV file.
    Save all documentation into single TXT file.
    """
    path = PATH['ruc']
    path.parent.mkdir(parents=True, exist_ok=True)
    ruc_src = PATH['source'] / 'ruc'
    ruc_dfs = []
    ruc_doc_dfs = []

    # 1974
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53251/ruralurbancodes1974.xls?v=9631.3'
    fname = download_file(url, ruc_src)
    df = pd.read_excel(fname, dtype='str', nrows=3141)
    cols_map = {'FIPS Code': 'FIPS', 'State': 'STATE', 'County Name': 'COUNTY', '1974 Rural-urban Continuum Code': 'RUC_CODE'}
    df = df.rename(columns=cols_map)
    df['RUC_YEAR'] = '1974'
    ruc_dfs.append(df)

    df = pd.concat([
        pd.DataFrame(['RUC 1974 documentation', '-' * 80, f'Data source: {url}']),
        pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
        pd.DataFrame([[v, k] for k, v in cols_map.items()]),
        pd.DataFrame(['']),
        pd.read_excel(fname, dtype='str', skiprows=3143, header=None).dropna(axis=1, how='all')])
    ruc_doc_dfs.append(df)


    # 1983
    # 1993 file is contained within 1983-1993 file. 2003 file repeats 1993 column.
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53251/cd8393.xls?v=9631.3'
    fname = download_file(url, ruc_src)

    df = pd.read_excel(fname, dtype='str')
    cols_map = {'FIPS': 'FIPS', 'State': 'STATE', 'County Name': 'COUNTY', '1983 Rural-urban Continuum Code': 'RUC_CODE'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['RUC_YEAR'] = '1983'
    ruc_dfs.append(df)

    df = pd.concat([
        pd.DataFrame(['RUC 1983 documentation', '-' * 80, f'Data source: {url}']),
        pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
        pd.DataFrame([[v, k] for k, v in cols_map.items()])])
    ruc_doc_dfs.extend([pd.DataFrame(['', '']), df])


    # 1993
    df = pd.read_excel(fname, dtype='str')
    cols_map = {'FIPS': 'FIPS', 'State': 'STATE', 'County Name': 'COUNTY', '1993 Rural-urban Continuum Code': 'RUC_CODE'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['RUC_YEAR'] = '1993'
    ruc_dfs.append(df)

    df = pd.concat([
        pd.DataFrame(['RUC 1993 documentation', '-' * 80, f'Data source: {url}']),
        pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
        pd.DataFrame([[v, k] for k, v in cols_map.items()])])
    ruc_doc_dfs.extend([pd.DataFrame(['', '']), df])


    # 2003
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53251/ruralurbancodes2003.xls?v=9631.3'
    fname = download_file(url, ruc_src)
    df = pd.read_excel(fname, dtype='str')
    cols_map = {'FIPS Code': 'FIPS', 'State': 'STATE', 'County Name': 'COUNTY',
                '2003 Rural-urban Continuum Code': 'RUC_CODE', '2000 Population ': 'POPULATION',
                'Percent of workers in nonmetro counties commuting to central counties of adjacent metro areas': 'PERCENT_NONMETRO_COMMUTERS',
                'Description for 2003 codes': 'RUC_CODE_DESCRIPTION'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['RUC_YEAR'] = '2003'
    df['POPULATION_YEAR'] = '2000'
    ruc_dfs.append(df)

    df = pd.concat([
        pd.DataFrame(['RUC 2003 documentation', '-' * 80, f'Data source: {url}']),
        pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
        pd.DataFrame([[v, k] for k, v in cols_map.items()])])
    ruc_doc_dfs.extend([pd.DataFrame(['', '']), df])


    # Puerto Rico 2003
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53251/pr2003.xls?v=9631.3'
    fname = download_file(url, ruc_src)
    df = pd.read_excel(fname, dtype='str')

    cols_map = {'FIPS Code': 'FIPS', 'State': 'STATE', 'Municipio Name': 'COUNTY', 'Population 2003 ': 'POPULATION',
                'Rural-urban Continuum Code, 2003': 'RUC_CODE', 'Description of the 2003 Code': 'RUC_CODE_DESCRIPTION'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['RUC_YEAR'] = '2003'
    df['POPULATION_YEAR'] = '2003'
    ruc_dfs.append(df)

    df = pd.concat([
        pd.DataFrame(['RUC Puerto Rico 2003 documentation', '-' * 80, f'Data source: {url}']),
        pd.DataFrame([['Column names'], ['Renamed', 'Original']]),
        pd.DataFrame([[v, k] for k, v in cols_map.items()])])
    ruc_doc_dfs.extend([pd.DataFrame(['', '']), df])


    # 2013
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53251/ruralurbancodes2013.xls?v=9631.3'
    fname = download_file(url, ruc_src)
    df = pd.read_excel(fname, 'Rural-urban Continuum Code 2013', dtype='str')
    cols_map = {'FIPS': 'FIPS', 'State': 'STATE', 'County_Name': 'COUNTY', 'Population_2010': 'POPULATION',
                'RUCC_2013': 'RUC_CODE', 'Description': 'RUC_CODE_DESCRIPTION'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['RUC_YEAR'] = '2013'
    df['POPULATION_YEAR'] = '2010'
    ruc_dfs.append(df)

    df = pd.concat([pd.DataFrame(['RUC 2013 documentation', '-' * 80, f'Data source: {url}']),
                    pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
                    pd.DataFrame([[v, k] for k, v in cols_map.items()]),
                    pd.DataFrame(['']),
                    pd.read_excel(fname, 'Documentation', header=None, dtype='str')])
    ruc_doc_dfs.extend([pd.DataFrame(['', '']), df])

    # Combine and save to disk
    df = pd.concat(ruc_dfs)
    df = df[['FIPS', 'STATE', 'COUNTY', 'RUC_YEAR', 'RUC_CODE', 'RUC_CODE_DESCRIPTION', 
             'POPULATION_YEAR', 'POPULATION', 'PERCENT_NONMETRO_COMMUTERS']]
    for col in df:
        df[col] = df[col].str.strip()
    df = df.sort_values(['FIPS', 'RUC_YEAR'])
    df.to_csv(path, index=False)
    print(f'Saved combined data to "{path}".')

    df = pd.concat(ruc_doc_dfs)
    for col in df:
        df[col] = df[col].str.strip()
    p = PATH['ruc_doc']
    df.to_csv(p, '\t', header=False, index=False)
    print(f'Saved documentation to "{p}".')


def get_ui_df():
    """Return `pandas.DataFrame` of Urban Influence codes for all years."""
    path = PATH['ui']
    if not path.exists():
        print(f'UI data not found at "{path}", attempting to download and construct...')
        download_and_combine_ui()
    df = pd.read_csv(path, dtype='str')
    for c in ['UI_YEAR', 'POPULATION_YEAR', 'POPULATION', 'POPULATION_DENSITY']:
        df[c] = pd.to_numeric(df[c])
    cats = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
    df['UI_CODE'] = pd.Categorical(df['UI_CODE'], cats, True)
    return df

def download_and_combine_ui():
    """Download Urban Influence codes and documentation.
    Combine all years of data into single CSV file.
    Save all documentation into single TXT file.
    """
    path = PATH['ui']
    path.parent.mkdir(parents=True, exist_ok=True)
    ui_src = PATH['source'] / 'ui'
    ui_dfs = []
    ui_doc_dfs = []

    # 1993
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53797/UrbanInfluenceCodes.xls?v=1904.3'
    fpath = download_file(url, ui_src)

    df = pd.read_excel(fpath, 'Urban Influence Codes', dtype='str')
    cols_map = {'FIPS Code': 'FIPS', 'State': 'STATE', 'County name': 'COUNTY',
                '2000 Population': 'POPULATION', '2000 Persons per square mile': 'POPULATION_DENSITY',
                '1993 Urban Influence Code': 'UI_CODE', '1993 Urban Influence Code description': 'UI_CODE_DESCRIPTION'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['UI_YEAR'] = '1993'
    df['POPULATION_YEAR'] = '2000'
    ui_dfs.append(df)

    df = pd.concat([pd.DataFrame(['UI 1993 documentation', '-' * 80, f'Data source: {url}']),
                    pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
                    pd.DataFrame([[v, k] for k, v in cols_map.items()]),
                    pd.DataFrame(['']),
                    pd.read_excel(fpath, 'Information', header=None, dtype='str', skiprows=18)])

    ui_doc_dfs.append(df)

    # 2003
    df = pd.read_excel(fpath, 'Urban Influence Codes', dtype='str')
    cols_map = {'FIPS Code': 'FIPS', 'State': 'STATE', 'County name': 'COUNTY',
                '2003 Urban Influence Code': 'UI_CODE', '2003 Urban Influence Code description': 'UI_CODE_DESCRIPTION',
                '2000 Population': 'POPULATION', '2000 Persons per square mile': 'POPULATION_DENSITY'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['UI_YEAR'] = '2003'
    df['POPULATION_YEAR'] = '2000'
    ui_dfs.append(df)

    df = pd.concat([pd.DataFrame(['UI 2003 documentation', '-' * 80, f'Data source: {url}']),
                    pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
                    pd.DataFrame([[v, k] for k, v in cols_map.items()]),
                    pd.DataFrame(['']),
                    pd.read_excel(fpath, 'Information', header=None, dtype='str', skiprows=3, nrows=14)])
    ui_doc_dfs.extend([pd.DataFrame(['', '']), df])

    # Puerto Rico 2003
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53797/pr2003UrbInf.xls?v=1904.3'
    fpath = download_file(url, ui_src)

    df = pd.read_excel(fpath, dtype='str')
    cols_map = {'FIPS Code': 'FIPS', 'State': 'STATE', 'Municipio Name': 'COUNTY', 'Population 2003 ': 'POPULATION',
                'Urban Influence  Code, 2003': 'UI_CODE', 'Description of the 2003 Code': 'UI_CODE_DESCRIPTION'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['UI_YEAR'] = '2003'
    df['POPULATION_YEAR'] = '2003'
    ui_dfs.append(df)

    df = pd.concat([pd.DataFrame(['UI Puerto Rico 2003 documentation', '-' * 80, f'Data source: {url}']),
                    pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
                    pd.DataFrame([[v, k] for k, v in cols_map.items()])])
    ui_doc_dfs.extend([pd.DataFrame(['', '']), df])

    # 2013
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53797/UrbanInfluenceCodes2013.xls?v=1904.3'
    fpath = download_file(url, ui_src)

    df = pd.read_excel(fpath, 'Urban Influence Codes 2013', dtype='str')
    cols_map = {'FIPS': 'FIPS', 'State': 'STATE', 'County_Name': 'COUNTY', 'Population_2010': 'POPULATION',
                'UIC_2013': 'UI_CODE', 'Description': 'UI_CODE_DESCRIPTION'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['UI_YEAR'] = '2013'
    df['POPULATION_YEAR'] = '2010'
    ui_dfs.append(df)

    df = pd.concat([pd.DataFrame(['UI 2013 documentation', '-' * 80, f'Data source: {url}']),
                    pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
                    pd.DataFrame([[v, k] for k, v in cols_map.items()]),
                    pd.DataFrame(['']),
                    pd.read_excel(fpath, 'Documentation', header=None, dtype='str')])
    ui_doc_dfs.extend([pd.DataFrame(['', '']), df])

    # Combine and save to disk
    df = pd.concat(ui_dfs)
    df = df[['FIPS', 'STATE', 'COUNTY', 'UI_YEAR', 'UI_CODE', 'UI_CODE_DESCRIPTION', 
             'POPULATION_YEAR', 'POPULATION', 'POPULATION_DENSITY']]
    for col in df:
        df[col] = df[col].str.strip()
    df = df.sort_values(['FIPS', 'UI_YEAR'])
    df.to_csv(path, index=False)
    print(f'Saved combined data to "{path}".')

    df = pd.concat(ui_doc_dfs)
    for col in df:
        df[col] = df[col].str.strip()
    p = PATH['ui_doc']
    df.to_csv(p, '\t', header=False, index=False)
    print(f'Saved documentation to "{p}".')


def get_ruca_df():
    """Return `pandas.DataFrame` of Rural-Urban Commuting Area codes for all years."""
    path = PATH['ruca']
    if not path.exists():
        print(f'RUCA data not found at "{path}", attempting to download and construct...')
        download_and_combine_ruca()
    df = pd.read_csv(path, dtype='str')
    for c in ['YEAR', 'POPULATION', 'AREA']:
        # ValueError: Unable to parse string "6 23.063" at position 269
        # todo: input files probably had this error, add manual fix to `download_and_convert_ruca()`
        df[c] = pd.to_numeric(df[c], 'coerce')
    cats = ['1', '1.1', 
            '2', '2.1', '2.2', 
            '3', 
            '4', '4.1', '4.2', 
            '5', '5.1', '5.2', 
            '6', '6.1', 
            '7', '7.1', '7.2', '7.3', '7.4', 
            '8', '8.1', '8.2', '8.3', '8.4', 
            '9', '9.1', '9.2', 
            '10', '10.1', '10.2', '10.3', '10.4', '10.5', '10.6', 
            '99']
    df['RUCA_CODE'] = df['RUCA_CODE'].str.replace('.0', '', regex=False)
    df['RUCA_CODE'] = pd.Categorical(df['RUCA_CODE'], cats, True)
    return df

def download_and_combine_ruca():
    """Download Rural-Urban Commuting Area codes and documentation.
    Combine all years of data into single CSV file.
    Save all documentation into single TXT file."""
    path = PATH['ruca']
    path.parent.mkdir(parents=True, exist_ok=True)
    ruca_src = PATH['source'] / 'ruca'
    ruca_dfs = []
    ruca_doc_dfs = []

    # 1990
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53241/ruca1990.xls?v=9882.5'
    fname = download_file(url, ruca_src)

    df = pd.read_excel(fname, 'Data', dtype='str')
    cols_map = {'FIPS state-county-tract code': 'FIPS',
                'Rural-urban commuting area code': 'RUCA_CODE',
                'Census tract population, 1990': 'POPULATION',
                'Census tract land area, square miles, 1990': 'AREA',
                'County metropolitan status, 1993 (1=metro,0=nonmetro)': 'METRO'}
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['FIPS'] = df['FIPS'].str.replace('.', '', regex=False)
    df['YEAR'] = '1990'
    ruca_dfs.append(df)

    df = pd.concat([
        pd.DataFrame(['RUCA 1990 documentation', '-' * 80, f'Data source: {url}']),
        pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
        pd.DataFrame([[v, k] for k, v in cols_map.items()]),
        pd.DataFrame(['']),
        pd.read_excel(fname, 'RUCA code description', header=None, dtype='str'),
        pd.DataFrame(['', 'Data sources']),
        pd.read_excel(fname, 'Data sources', header=None, dtype='str')])
    ruca_doc_dfs.append(df)


    # 2000
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53241/ruca00.xls?v=9882.5'
    fname = download_file(url, ruca_src)

    df = pd.read_excel(fname, 'Data', dtype='str')

    cols_map = {
        'Select State': 'STATE',
        'Select County ': 'COUNTY',
        'State County Tract Code': 'FIPS',
        'RUCA Secondary Code 2000': 'RUCA_CODE',
        'Tract Population 2000': 'POPULATION'
    }
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['YEAR'] = '2000'
    ruca_dfs.append(df)

    df = pd.concat([
        pd.DataFrame(['RUCA 2000 documentation', '-' * 80, f'Data source: {url}']),
        pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
        pd.DataFrame([[v, k] for k, v in cols_map.items()]),
        pd.DataFrame(['']),
        pd.read_excel(fname, 'RUCA code description', header=None, dtype='str'),
        pd.DataFrame(['', 'Data sources']),
        pd.read_excel(fname, 'Data sources', header=None, dtype='str')])
    ruca_doc_dfs.extend([pd.DataFrame(['', '']), df])


    # 2010
    url = 'https://www.ers.usda.gov/webdocs/DataFiles/53241/ruca2010revised.xlsx?v=9882.5'
    fname = download_file(url, ruca_src)

    df = pd.read_excel(fname, 'Data', dtype='str', skiprows=1)
    cols_map = {
        'Select State': 'STATE',
        'Select County': 'COUNTY',
        'State-County-Tract FIPS Code (lookup by address at http://www.ffiec.gov/Geocode/)': 'FIPS',
        'Secondary RUCA Code, 2010 (see errata)': 'RUCA_CODE',
        'Tract Population, 2010': 'POPULATION',
        'Land Area (square miles), 2010': 'AREA'
    }
    df = df.rename(columns=cols_map)
    df = df[cols_map.values()]
    df['YEAR'] = '2010'
    ruca_dfs.append(df)

    df = pd.concat([
        pd.DataFrame(['RUCA 2010 documentation', '-' * 80, f'Data source: {url}']),
        pd.DataFrame([[''], ['Column names'], ['Renamed', 'Original']]),
        pd.DataFrame([[v, k] for k, v in cols_map.items()]),
        pd.DataFrame(['']),
        pd.read_excel(fname, 'RUCA code description', header=None, dtype='str'),
        pd.DataFrame(['', 'Data sources']),
        pd.read_excel(fname, 'Data sources', header=None, dtype='str')])
    ruca_doc_dfs.extend([pd.DataFrame(['', '']), df])


    # Combine and save to disk
    df = pd.concat(ruca_dfs)
    df = df[['FIPS', 'STATE', 'COUNTY', 'YEAR', 'RUCA_CODE', 'POPULATION', 'AREA', 'METRO']]
    for col in df:
        df[col] = df[col].str.strip()
    df = df.sort_values(['FIPS', 'YEAR'])
    df.to_csv(path, index=False)
    print(f'Saved combined data to "{path}".')

    df = pd.concat(ruca_doc_dfs)
    for col in df:
        df[col] = df[col].str.strip()
    p = PATH['ruca_doc']
    df.to_csv(p, '\t', header=False, index=False)
    print(f'Saved documentation to "{p}".')

