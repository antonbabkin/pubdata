#!/usr/bin/env python
# coding: utf-8

import functools
import typing
import warnings
import shutil
from contextlib import redirect_stdout

import pandas as pd
import geopandas

from .reseng import util
from .reseng.caching import simplecache
from .reseng.monitor import log_start_finish
from .reseng.nbd import Nbd

nbd = Nbd('pubdata')


PATH = {
    'data': nbd.root / 'data/',
    'source_delin': nbd.root / 'data/source/geography_cbsa/delin/',
    'source_shape': nbd.root / 'data/source/geography_cbsa/shape/',
}

def init_dirs():
    """Create necessary directories."""
    for p in PATH.values():
        p.mkdir(parents=True, exist_ok=True)
    
def cleanup_all(remove_downloaded=False):
    cleanup_delin(remove_downloaded)
    cleanup_shape(remove_downloaded)


def get_cbsa_delin_src(year: int):
    """Download and return path to CBSA delineation file.
    When more than one revision exists in year (as in 2003 or 2018),
    the most recent one in that year is used.
    """
    
    init_dirs()

    base = 'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/'
    urls = {
        2020: f'{base}2020/delineation-files/list1_2020.xls',
        2018: f'{base}2018/delineation-files/list1_Sep_2018.xls',
        # 2018-april: f'{base}2018/delineation-files/list1.xls',
        2017: f'{base}2017/delineation-files/list1.xls',
        2015: f'{base}2015/delineation-files/list1.xls',
        2013: f'{base}2013/delineation-files/list1.xls',
        2009: f'{base}2009/historical-delineation-files/list3.xls',
        2008: f'{base}2008/historical-delineation-files/list3.xls',
        2007: f'{base}2007/historical-delineation-files/list3.xls',
        2006: f'{base}2006/historical-delineation-files/list3.xls',
        2005: f'{base}2005/historical-delineation-files/list3.xls',
        2004: f'{base}2004/historical-delineation-files/list3.xls',
        2003: f'{base}2003/historical-delineation-files/0312cbsas-csas.xls',
        # 2003-june: f'{base}2003/historical-delineation-files/030606omb-cbsa-csa.xls',
        1993: f'{base}1993/historical-delineation-files/93mfips.txt'
    }
    
    assert year in urls, f'CBSA delineation not available for {year}.'
    
    url = urls[year]
    ext = url.split('.')[-1]
    local = PATH['source_delin'] / f'{year}.{ext}'
        
    if not local.exists():
        print(f'File "{local}" not found, attempting download.')
        util.download_file(url, local.parent, local.name)
    return local

def get_cbsa_delin_df(year: int):
    f = get_cbsa_delin_src(year)
    
    if year == 1993:
        return _prep_cbsa_delin_df_1993(f)
    
    # number of rows to skip at top and bottom varies by year
    if year in [2003, 2013, 2015, 2017, 2018, 2020]:
        skip_head = 2
    elif year in [2005, 2006, 2007, 2008, 2009]:
        skip_head = 3
    elif year == 2004:
        skip_head = 7

    if year in [2003, 2004]:
        skip_foot = 0
    elif year == 2005:
        skip_foot = 6
    elif year in [2006, 2007, 2008, 2009, 2015, 2017, 2018, 2020]:
        skip_foot = 4
    elif year == 2013:
        skip_foot = 3

    df = pd.read_excel(f, dtype=str, skiprows=skip_head, skipfooter=skip_foot)

    # standardize column names
    if 2003 <= year <= 2009:
        del df['Status, 1=metro 2=micro']
        df['STATE_CODE'] = df['FIPS'].str[:2]
        df['COUNTY_CODE'] = df['FIPS'].str[2:]
        del df['FIPS']
        rename = {
            'CBSA Code': 'CBSA_CODE',
            'Metro Division Code': 'DIVISION_CODE',
            'CSA Code': 'CSA_CODE',
            'CBSA Title': 'CBSA_TITLE',
            'Level of CBSA': 'METRO_MICRO',
            'Metropolitan Division Title': 'DIVISION_TITLE',
            'CSA Title': 'CSA_TITLE',
            'Component Name': 'COUNTY',
            'State': 'STATE'
        }
        if year >= 2007:
            rename.update({'County Status': 'CENTRAL_OUTLYING'})
    elif 2013 <= year <= 2020:
        rename = {
            'CBSA Code': 'CBSA_CODE',
            'CBSA Title': 'CBSA_TITLE',
            'CSA Code': 'CSA_CODE',
            'CSA Title': 'CSA_TITLE',
            'Metropolitan Division Title': 'DIVISION_TITLE',
            'Metropolitan/Micropolitan Statistical Area': 'METRO_MICRO',
            'State Name': 'STATE',
            'County/County Equivalent': 'COUNTY',
            'FIPS State Code': 'STATE_CODE',
            'FIPS County Code': 'COUNTY_CODE',
            'Central/Outlying County': 'CENTRAL_OUTLYING'
        }
        if year == 2013:
            rename.update({'Metro Division Code': 'DIVISION_CODE'})
        else:
            rename.update({'Metropolitan Division Code': 'DIVISION_CODE'})
    
    df = df.rename(columns=rename)
    
    assert df[['STATE_CODE', 'COUNTY_CODE']].notna().all().all()
    assert not df.duplicated(['STATE_CODE', 'COUNTY_CODE']).any()
    assert df['METRO_MICRO'].notna().all()
    
    df['METRO_MICRO'] = df['METRO_MICRO'].map({
        'Metropolitan Statistical Area': 'metro',
        'Micropolitan Statistical Area': 'micro'
    })
    if 'CENTRAL_OUTLYING' in df:
        df['CENTRAL_OUTLYING'] = df['CENTRAL_OUTLYING'].str.lower()
    
    return df


def _prep_cbsa_delin_df_1993(src_file):

    df = pd.read_fwf(src_file, skiprows=22, skipfooter=29, dtype=str, header=None,
                     colspecs=[(0, 4), (8, 12), (16, 18), (24, 26), (26, 29), (32, 33), (40, 45), (48, 106)],
                     names=['MSA_CMSA_CODE', 'PRIMARY_MSA_CODE', 'ALT_CMSA_CODE',
                            'STATE_CODE', 'COUNTY_CODE', 'CENTRAL_OUTLYING',
                            'TOWN_CODE', 'NAME'])

    assert not util.tag_invalid_values(df['MSA_CMSA_CODE'], notna=True, nchar=4, number=True).any()
    assert not util.tag_invalid_values(df['PRIMARY_MSA_CODE'], nchar=4, number=True).any()
    assert not util.tag_invalid_values(df['ALT_CMSA_CODE'], nchar=2, number=True).any()
    assert not util.tag_invalid_values(df['STATE_CODE'], nchar=2, number=True).any()
    assert not util.tag_invalid_values(df['COUNTY_CODE'], nchar=3, number=True).any()
    assert not util.tag_invalid_values(df['CENTRAL_OUTLYING'], cats=['1', '2']).any()
    assert not util.tag_invalid_values(df['TOWN_CODE'], nchar=5, number=True).any()
    assert not util.tag_invalid_values(df['NAME'], notna=True).any()

    df['CENTRAL_OUTLYING'] = df['CENTRAL_OUTLYING'].map({'1': 'central', '2': 'outlying'})

    return df


def cleanup_delin(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['source_delin'], ignore_errors=True)
    
@log_start_finish
def test_get_cbsa_delin_df(redownload=False):
    cleanup_delin(redownload)
    for year in [1993, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2013, 2015, 2017, 2018, 2020]:
        print(year)
        df = get_cbsa_delin_df(year)
        assert len(df) > 0


def _():
    base = 'https://www2.census.gov/geo/tiger/'
    urls = {
        (2010, '20m'):  f'{base}GENZ2010/gz_2010_us_310_m1_20m.zip',
        (2010, '500k'): f'{base}GENZ2010/gz_2010_us_310_m1_500k.zip',
        (2013, '20m'):  f'{base}GENZ2013/cb_2013_us_cbsa_20m.zip',
        (2013, '5m'):   f'{base}GENZ2013/cb_2013_us_cbsa_5m.zip',
        (2013, '500k'): f'{base}GENZ2013/cb_2013_us_cbsa_500k.zip',
    }
    urls.update({(y, s): f'{base}GENZ{y}/shp/cb_{y}_us_cbsa_{s}.zip'
                 for y in range(2014, 2022) for s in ['20m', '5m', '500k']})
    return urls
_urls_cbsa_shape_src = _()


def get_cbsa_shape_src(year=2021, scale='20m'):
    """Download and return path to CBSA boundary shapefile."""
    init_dirs()
    
    assert (year, scale) in _urls_cbsa_shape_src, f'No CBSA shapes in {year}, {scale}.'
    
    local = PATH['source_shape'] / f'{year}_{scale}.zip'
        
    if not local.exists():
        print(f'File "{local}" not found, attempting download.')
        util.download_file(_urls_cbsa_shape_src[(year, scale)], local.parent, local.name)
    return local


def get_cbsa_shape_df(year=2021, 
                    scale: typing.Literal['20m', '5m', '500k'] = '20m',
                    geometry=True):
    """Load CBSA shapefile as geodataframe."""
    f = get_cbsa_shape_src(year, scale)
    df = geopandas.read_file(f)

    if year == 2010:
        df = df.rename(columns={
            'CBSA': 'CBSA_CODE',
            'NAME': 'CBSA_TITLE',
            'LSAD': 'METRO_MICRO',
        })
        df['METRO_MICRO'] = df['METRO_MICRO'].str.lower()
        df = df[['CBSA_CODE', 'CBSA_TITLE', 'METRO_MICRO', 'CENSUSAREA', 'geometry']]
    elif 2013 <= year <= 2021:
        df = df.rename(columns={
            'CBSAFP': 'CBSA_CODE',
            'NAME': 'CBSA_TITLE',
            'LSAD': 'METRO_MICRO',
        })
        df['METRO_MICRO'] = df['METRO_MICRO'].map({'M1': 'metro', 'M2': 'micro'})
        df = df[['CBSA_CODE', 'CBSA_TITLE', 'METRO_MICRO', 'ALAND', 'AWATER', 'geometry']]
    else:
        raise NotImplementedError(f'Year {year}.')

    assert df['CBSA_CODE'].notna().all()
    assert not df['CBSA_CODE'].duplicated().any()

    if not geometry:
        df = pd.DataFrame(df).drop(columns='geometry')
    return df


def cleanup_shape(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['source_shape'], ignore_errors=True)

@log_start_finish
def test_get_cbsa_shape_df(redownload=False):
    cleanup_shape(redownload)
    for year, scale in _urls_cbsa_shape_src.keys():
        for geom in [False, True]:
            print(year, scale, geom)
            get_cbsa_shape_df(year, scale)


def test_all(redownload=False):
    test_get_cbsa_delin_df(redownload)
    test_get_cbsa_shape_df(redownload)

