#!/usr/bin/env python
# coding: utf-8

import io

import pandas as pd
import pyarrow
import pyarrow.dataset

from .reseng.util import download_file
from .reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'root': nbd.root,
    'source': nbd.root/'data/source/population/',
    'parquet': nbd.root/'data/population/population.pq'
}


def prep_2010_2019():
    url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv'
    f = download_file(url, PATH['source'])
    cols = ['STATE', 'COUNTY'] + [f'POPESTIMATE{y}' for y in range(2010, 2020)]
    df = pd.read_csv(f, encoding='ISO-8859-1', dtype='str', usecols=cols)
    df = pd.wide_to_long(df, 'POPESTIMATE', ['STATE', 'COUNTY'], 'year')
    df = df.reset_index().rename(columns={'STATE': 'st', 'COUNTY': 'cty', 'POPESTIMATE': 'pop'})
    df['pop'] = df['pop'].astype('int')
    # add US total
    d = df.query('cty == "000"').groupby('year')['pop'].sum().to_frame('pop').reset_index()
    d[['st', 'cty']] = ['00', '000']
    df = pd.concat([df, d])
    return df


def prep_2000_2009():
    url = 'https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/co-est00int-tot.csv'
    f = download_file(url, PATH['source'])
    dt = {'STATE': str, 'COUNTY': str}
    cols = ['STATE', 'COUNTY'] + [f'POPESTIMATE{y}' for y in range(2000, 2010)]
    df = pd.read_csv(f, encoding='ISO-8859-1', usecols=cols, dtype='str')
    df = pd.wide_to_long(df, 'POPESTIMATE', ['STATE', 'COUNTY'], 'year')
    df = df.reset_index().rename(columns={'STATE': 'st', 'COUNTY': 'cty', 'POPESTIMATE': 'pop'})
    df['st'] = df['st'].str.pad(2, fillchar='0')
    df['cty'] = df['cty'].str.pad(3, fillchar='0')
    df['pop'] = df['pop'].astype('int')
    # add US total
    d = df.query('cty == "000"').groupby('year')['pop'].sum().to_frame('pop').reset_index()
    d[['st', 'cty']] = ['00', '000']
    df = pd.concat([df, d])
    return df


def prep_1990_1999():
    url = 'https://www2.census.gov/programs-surveys/popest/tables/1990-2000/estimates-and-change-1990-2000/2000c8_00.txt'
    f = download_file(url, PATH['source'])
    with open(f, encoding='ISO-8859-1') as file:
        data = io.StringIO()
        in_table = False
        for line in file:
            if in_table:
                if line[0] == '1':
                    data.write(line)
                else:
                    break
            else:
                if line[0] == '1':
                    in_table = True
                    data.write(line)

    data.seek(0)
    df = pd.read_fwf(data, dtype='str', header=None)
    # keep fips and popest cols
    df = df.iloc[:, 1:13]
    df.iloc[0, 0] = '00000' # US total
    df.columns = ['fips'] + [f'pop{y}' for y in range(2000, 1989, -1)]
    df['fips'] = df['fips'].str.pad(5, 'right', '0')
    df['st'] = df['fips'].str[:2]
    df['cty'] = df['fips'].str[2:]
    df = df.drop(columns=['pop2000', 'fips'])
    df = pd.wide_to_long(df, 'pop', ['st', 'cty'], 'year')
    df = df.reset_index()
    df['pop'] = pd.to_numeric(df['pop'].str.replace(',', '', regex=False)).astype('int')

    return df


def prep_1980_1989():
    url = 'https://www2.census.gov/programs-surveys/popest/tables/1980-1990/counties/totals/e8089co.xls'
    f = download_file(url, PATH['source'])
    df = pd.read_excel(f, skiprows=13, header=None)
    
    # create indicator of alternating blocks of 1980-1984 and 1984-1989 columns
    df['block'] = df.loc[df[0] == 'FIPS Code', 2]
    df = df.dropna(how='all')
    df['block'] = df['block'].fillna(method='ffill')
    
    # stack blocks side by side
    d0 = df.query('block == "Census 1980"').reset_index(drop=True)
    d1 = df.query('block == "Estimate 1985"').reset_index(drop=True)
    assert d0.shape == d1.shape
    df = pd.concat([d0, d1], axis=1, ignore_index=True)
    assert df[0].equals(df[8]), 'FIPS codes on two sides do not match'

    df = df.drop([1, 7, 8, 9, 15], axis=1)
    df.columns = ['fips'] + [f'pop{y}' for y in range(1980, 1990)]
    df = df.query('fips != "FIPS Code"')
    df = pd.wide_to_long(df, ['pop'], 'fips', 'year').reset_index()
    df['pop'] = df['pop'].astype('int64')
    df['st'] = df['fips'].str[:2]
    df['cty'] = df['fips'].str[2:]
    del df['fips']
    return df


def prep_1970_1979():
    url = 'https://www2.census.gov/programs-surveys/popest/tables/1900-1980/counties/totals/e7079co.txt'
    f = download_file(url, PATH['source'])
    df = pd.read_fwf(f, widths=[6, 16, 10, 10, 10, 10, 10], header=None, skiprows=26)
    df = df[df[0] != 'FIPS']
    
    # join multi-line rows that happen when county name is longer than column width
    assert ((df[0].isna().astype(int) + df[2].isna().astype(int)) <= 1).all(), 'FIPS and pop columns should not be both NA'
    df[[2, 3, 4, 5, 6]] = df[[2, 3, 4, 5, 6]].fillna(method='bfill')
    df = df[df[0].notna()]

    # create indicator of alternating blocks of 1970-1974 and 1975-1979 columns
    df['block'] = df.loc[df[0] == 'Code', 2]
    df['block'] = df['block'].fillna(method='ffill')

    # stack blocks side by side
    d0 = df.query('block == "1970"').reset_index(drop=True)
    d1 = df.query('block == "1975"').reset_index(drop=True)
    assert d0.shape == d1.shape
    df = pd.concat([d0, d1], axis=1, ignore_index=True)
    assert df[0].equals(df[8]), 'FIPS codes on two sides do not match'

    df = df.drop([1, 7, 8, 9, 15], axis=1)
    df.columns = ['fips'] + [f'pop{y}' for y in range(1970, 1980)]
    df = df.query('fips != "Code"')
    df = pd.wide_to_long(df, ['pop'], 'fips', 'year').reset_index()
    df['pop'] = df['pop'].astype('int64')
    df['st'] = df['fips'].str[:2]
    df['cty'] = df['fips'].str[2:]
    del df['fips']
    return df


def get_df():
    df_file = PATH['parquet']
    if df_file.exists():
        return pd.read_parquet(df_file)
    
    df = pd.concat([prep_1970_1979(), prep_1980_1989(), prep_1990_1999(), prep_2000_2009(), prep_2010_2019()], ignore_index=True)

    df = df.sort_values(['st', 'cty', 'year']).reset_index()
    df = df[['st', 'cty', 'year', 'pop']]

    df_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(df_file, engine='pyarrow', index=False)
    return df

