#!/usr/bin/env python
# coding: utf-8

import functools
import warnings
import shutil
import typing
import zipfile
import xml

import numpy as np
import pandas as pd
import geopandas
import pyarrow
import pyarrow.dataset

from .reseng.util import download_file
from .reseng.nbd import Nbd

nbd = Nbd('pubdata')

PATH = {
    'source': nbd.root / 'data/source/geo',
    'geo': nbd.root / 'data/geo/',
    'state': nbd.root / 'data/geo/state/',
    'county': nbd.root / 'data/geo/county.pq',
    'tract': nbd.root / 'data/geo/tract.pq',
    'zcta': nbd.root / 'data/geo/zcta/'
}
PATH['source'].mkdir(parents=True, exist_ok=True)
PATH['geo'].mkdir(parents=True, exist_ok=True)


# in geopandas 0.8, parquet support is still experimental
# https://geopandas.org/docs/user_guide/io.html#apache-parquet-and-feather-file-formats
import warnings
warnings.filterwarnings('ignore', message='.*initial implementation of Parquet.*')


_REGION_STATE = {}

_REGION_STATE['BEA'] = {
    # (region_name, region_code): [state_codes]
    ('New England', '91'): ['09', '23', '25', '33', '44', '50'],
    ('Mideast', '92'): ['10', '11', '24', '34', '36', '42'],
    ('Great Lakes', '93'): ['17', '18', '26', '39', '55'],
    ('Plains', '94'): ['19', '20', '27', '29', '31', '38', '46'],
    ('Southeast', '95'): ['01', '05', '12', '13', '21', '22', '28', '37', '45', '47', '51', '54'],
    ('Southwest', '96'): ['04', '35', '40', '48'],
    ('Rocky Mountain', '97'): ['08', '16', '30', '49', '56'],
    ('Far West', '98'): ['02', '06', '15', '32', '41', '53']
}


_STATE_REVISION_YEAR = 2021

def get_state_src(scale: typing.Literal['20m', '5m', '500k', 'tiger'] = '5m'):
    """Download state boundary zipped shapefile and return path to it."""
    year = _STATE_REVISION_YEAR
    url = 'https://www2.census.gov/geo/tiger/'
    if scale == 'tiger':
        file_name = f'tl_{year}_us_state.zip'
        url += f'TIGER{year}/STATE/{file_name}'
    else:
        file_name = f'cb_{year}_us_state_{scale}.zip'
        url += f'GENZ{year}/shp/{file_name}'
    local_path = PATH['source'] / 'state' / file_name
    if not local_path.exists():
        print(f'File "{local_path}" not found, attempting download.')
        download_file(url, local_path.parent, local_path.name)
    return local_path    

def get_state_df(geometry: bool = True,
                 scale: typing.Literal['20m', '5m', '500k', 'tiger'] = '5m'):
    """Return geopandas.GeoDataFrame with state shapes.
    Set `geometry = False` to return pandas.DataFrame.
    """
    columns = ['CODE', 'NAME', 'ABBR', 'CONTIGUOUS', 'TERRITORY', 'BEA_REGION_NAME', 'BEA_REGION_CODE', 'ALAND', 'AWATER']
    pq_path = PATH['state'] / f'{scale}.pq'
    if pq_path.exists():
        if geometry:
            return geopandas.read_parquet(pq_path)
        else:
            return pd.read_parquet(pq_path, 'pyarrow', columns)

    src_path = get_state_src(scale)
    df = geopandas.read_file(src_path)
    df = df.rename(columns={'STATEFP': 'CODE', 'STUSPS': 'ABBR'})
    df['CONTIGUOUS'] = ~df['CODE'].isin(['02', '15', '60', '66', '69', '72', '78'])
    df['TERRITORY'] = df['CODE'].isin(['60', '66', '69', '72', '78'])

    assert df.notna().all().all()
    
    df[['BEA_REGION_NAME', 'BEA_REGION_CODE']] = None
    for (region_name, region_code), state_codes in _REGION_STATE['BEA'].items():
        df.loc[df['CODE'].isin(state_codes), 'BEA_REGION_NAME'] = region_name
        df.loc[df['CODE'].isin(state_codes), 'BEA_REGION_CODE'] = region_code
    
    df = df[columns + ['geometry']]
    df = df.sort_values('CODE').reset_index(drop=True)
    

    for c in ['CODE', 'NAME', 'ABBR']:
        assert not df.duplicated(c).any()
    assert len(df) == (52 if scale == '20m' else 56)
    
    pq_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(pq_path)
    if not geometry:
        df = pd.DataFrame(df).drop(columns='geometry')
    return df

def _data_cleanup_state(which: typing.Literal['downloaded', 'processed', 'all']):
    """Remove state data files."""
    if which in ['downloaded', 'all']:
        print('Removing downloaded state files.')
        shutil.rmtree(PATH['source'] / 'state', ignore_errors=True)
    if which in ['processed', 'all']:
        print('Removing processed state files.')
        shutil.rmtree(PATH['state'], ignore_errors=True)


def get_county_src(year=2020, scale='20m'):
    """Download and return path to county boundary shapefile."""

    base = 'https://www2.census.gov/geo/tiger/'
    urls = {
        # 1990 and 2000 files exist only in one scale, probably 20m
        (1990, '20m'):  f'{base}PREVGENZ/co/co90shp/co99_d90_shp.zip',
        (2000, '20m'):  f'{base}PREVGENZ/co/co00shp/co99_d00_shp.zip',
        (2010, '20m'):  f'{base}GENZ2010/gz_2010_us_050_00_20m.zip',
        (2010, '5m'):   f'{base}GENZ2010/gz_2010_us_050_00_5m.zip',
        (2010, '500k'): f'{base}GENZ2010/gz_2010_us_050_00_500k.zip',
        (2013, '20m'):  f'{base}GENZ2013/cb_2013_us_county_20m.zip',
        (2013, '5m'):   f'{base}GENZ2013/cb_2013_us_county_5m.zip',
        (2013, '500k'): f'{base}GENZ2013/cb_2013_us_county_500k.zip',
    }
    urls.update({(y, s): f'{base}GENZ{y}/shp/cb_{y}_us_county_{s}.zip'
                 for y in range(2014, 2021) for s in ['20m', '5m', '500k']})
    
    assert (year, scale) in urls, f'No county shapes in {year}, {scale}.'
    
    local = PATH['source']/f'county/{year}_{scale}.zip'
        
    if not local.exists():
        print(f'File "{local}" not found, attempting download.')
        download_file(urls[(year, scale)], local.parent, local.name)
    return local


def get_county_df(year=2020, geometry=True, scale='20m'):

    path = PATH['county']/f'{year}/{scale}.pq'
    if path.exists():
        if geometry:
            return geopandas.read_parquet(path)
        else:
            return pd.read_parquet(path, 'pyarrow', ['CODE', 'NAME', 'STATE_CODE', 'COUNTY_CODE'])

    p = get_county_src(year, scale)
    df = geopandas.read_file(p)
    if year == 1990:
        df = df.rename(columns={'ST': 'STATE_CODE', 'CO': 'COUNTY_CODE'})
    elif year in [2000, 2010]:
        df = df.rename(columns={'STATE': 'STATE_CODE', 'COUNTY': 'COUNTY_CODE'})
    else:
        df = df.rename(columns={'STATEFP': 'STATE_CODE', 'COUNTYFP': 'COUNTY_CODE'})
    df['CODE'] = df['STATE_CODE'] + df['COUNTY_CODE']
    df = df[['CODE', 'NAME', 'STATE_CODE', 'COUNTY_CODE', 'geometry']]
    
    assert df['CODE'].notna().all()

    # 1990 and 2000 shapefiles have multiple polygon records per non-contiguous county
    if year in [1990, 2000]:
        df = df.dissolve('CODE', as_index=False, sort=False)
    
    assert not df.duplicated('CODE').any()
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    if not geometry:
        df = pd.DataFrame(df).drop(columns='geometry')
    return df


def get_tract_src(year, state_code):
    """Return path to zipped tract shapefile, downloading if missing."""
    url = 'https://www2.census.gov/geo/tiger/'
    if year == 1990:
        url += f'PREVGENZ/tr/tr90shp/tr{state_code}_d90_shp.zip'
    elif year == 2000:
        url += f'PREVGENZ/tr/tr00shp/tr{state_code}_d00_shp.zip'
    elif year == 2010:
        url += f'GENZ2010/gz_2010_{state_code}_140_00_500k.zip'
    elif year == 2020:
        url += f'GENZ2020/shp/cb_2020_{state_code}_tract_500k.zip'
    else:
        raise Exception(f'No tract revisions in {year}.')
    local = PATH['source'] / f'tract/{year}/{state_code}.zip'
        
    if not local.exists():
        print(f'File "{local}" not found, attempting download.')
        download_file(url, local.parent, local.name)
    return local

def get_tract_df(years=None, state_codes=None, geometry=True):
    _years = years or [1990, 2000, 2010, 2020]
    _state_codes = state_codes or get_state_df(geometry=False)['CODE'].tolist()
    for y in _years:
        for sc in _state_codes:
            _prep_tract_df(y, sc)
    
    p = pyarrow.dataset.partitioning(flavor='hive',
        schema=pyarrow.schema([('YEAR', pyarrow.int16()), ('STATE_CODE', pyarrow.string())]))
    f = [] if (years or state_codes) else None
    if years:
        f.append(('YEAR', 'in', years))
    if state_codes:
        f.append(('STATE_CODE', 'in', state_codes))
    if geometry:
        df = geopandas.read_parquet(PATH['tract'], partitioning=p, filters=f)
        # todo: CRS information is not loaded from the dataset, 
        # maybe because frames with missing CRS (1990) are in the mix.
        df = df.set_crs('EPSG:4269')
        return df
    else:
        c = ['YEAR', 'CODE', 'NAME', 'STATE_CODE', 'COUNTY_CODE', 'TRACT_CODE']
        return pyarrow.parquet.read_table(PATH['tract'], columns=c, partitioning=p, filters=f,
                                          use_pandas_metadata=True).to_pandas()

def _prep_tract_df(year, state_code):
    """Download shapefiles for one year and one state, normalize column names and save as parquet partition."""
    path = PATH['tract']/f'YEAR={year}/STATE_CODE={state_code}/part.pq'
    if path.exists(): return

    p = get_tract_src(year, state_code)
    df = geopandas.read_file(p)
    if year == 1990:
        if state_code == '34':
            # 2 records have NA tracts, don't know what it means
            df = df[df['TRACTBASE'].notna()] 
        df = df.rename(columns={'ST': 'STATE_CODE', 'CO': 'COUNTY_CODE'})
        df['TRACT_CODE'] = df['TRACTBASE'] + df['TRACTSUF'].fillna('00')
    elif year == 2000:
        df = df.rename(columns={'STATE': 'STATE_CODE', 'COUNTY': 'COUNTY_CODE'})
        df['TRACT_CODE'] = df['TRACT'].str.pad(6, 'right', '0')
    elif year == 2010:
        df = df.rename(columns={'STATE': 'STATE_CODE', 'COUNTY': 'COUNTY_CODE', 'TRACT': 'TRACT_CODE'})
    elif year == 2020:
        df = df.rename(columns={'STATEFP': 'STATE_CODE', 'COUNTYFP': 'COUNTY_CODE', 'TRACTCE': 'TRACT_CODE'})
    df['CODE'] = df['STATE_CODE'] + df['COUNTY_CODE'] + df['TRACT_CODE']
    assert (df['CODE'].str.len() == 11).all(), f'Tract {year} {state_code}: wrong code length.'
    df['NAME'] = df['TRACT_CODE'].astype('int64').astype('str')
    df['NAME'] = df['NAME'].str[:-2] + '.' + df['NAME'].str[-2:]
    df = df[['CODE', 'NAME', 'geometry', 'COUNTY_CODE', 'TRACT_CODE']]
    
    assert df['CODE'].notna().all()
    # 1990 and 2000 shapefiles have multiple polygon records per non-contiguous tract
    if year in [1990, 2000]:
        df = df.dissolve('CODE', as_index=False, sort=False)

    assert not df.duplicated('CODE').any()
        
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


def get_zcta_src(year: int):
    """Download and return path to ZCTA boundary shapefile."""

    base = 'https://www2.census.gov/geo/tiger/'
    urls = {
        2020: f'{base}GENZ2020/shp/cb_2020_us_zcta520_500k.zip',
        2013: f'{base}GENZ2013/cb_2013_us_zcta510_500k.zip',
        2010: f'{base}GENZ2010/gz_2010_us_860_00_500k.zip',
        2000: f'{base}PREVGENZ/zt/z300shp/z399_d00_shp.zip'
    }
    for y in range(2014, 2020):
        urls[y] = f'{base}GENZ{y}/shp/cb_{y}_us_zcta510_500k.zip'
    
    assert year in urls, f'No ZCTA shapes in {year}.'
    
    local = PATH['source']/f'zcta/{year}.zip'
        
    if not local.exists():
        print(f'File "{local}" not found, attempting download.')
        download_file(urls[year], local.parent, local.name)
    return local


def get_zcta_df(year=2020, geometry=True):

    path = PATH['zcta'] / f'{year}.pq'
    if path.exists():
        if geometry:
            return geopandas.read_parquet(path)
        else:
            return pd.read_parquet(path, 'pyarrow').drop(columns=['geometry'])
    
    # add other years later as needed
    if not (2013 <= year <= 2020):
        raise NotImplementedError(f'Year {year}.')
        
    f = get_zcta_src(year)
    df = geopandas.read_file(f)
    if 2013 <= year <= 2019:
        df = df.rename(columns={'ZCTA5CE10': 'ZCTA', 'ALAND10': 'ALAND', 'AWATER10': 'AWATER'})
    elif year == 2020:
        df = df.rename(columns={'ZCTA5CE20': 'ZCTA', 'ALAND20': 'ALAND', 'AWATER20': 'AWATER'})
        
    df = df[['ZCTA', 'ALAND', 'AWATER', 'geometry']]
    
    assert df['ZCTA'].notna().all()
    assert not df.duplicated('ZCTA').any()
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    if not geometry:
        df = pd.DataFrame(df).drop(columns='geometry')
    return df

