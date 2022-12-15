#!/usr/bin/env python
# coding: utf-8

import shutil

import numpy as np
import pandas as pd
import pyarrow.parquet
import pyarrow.dataset

from .reseng.util import download_file
from .reseng.monitor import log_start_finish
from .reseng.nbd import Nbd
nbd = Nbd('pubdata')


PATH = {
    'source': nbd.root / 'data/source/agcensus',
    'proc': nbd.root / 'data/agcensus',
    'pq_part': {
        2002: nbd.root / 'data/agcensus/2002/part.pq',
        2007: nbd.root / 'data/agcensus/2007/part.pq',
        2012: nbd.root / 'data/agcensus/2012/part.pq',
        2017: nbd.root / 'data/agcensus/2017/part.pq'
    }
}

def init_dirs():
    """Create necessary directories."""
    PATH['source'].mkdir(parents=True, exist_ok=True)
    PATH['proc'].mkdir(parents=True, exist_ok=True)
    
def cleanup(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['source'], ignore_errors=True)
    print('Removing processed files...')
    shutil.rmtree(PATH['proc'], ignore_errors=True)


def _get_qs_src(year):
    init_dirs()
    url = f'ftp://ftp.nass.usda.gov/quickstats/qs.census{year}.txt.gz'
    file = PATH['source'] / url.split('/')[-1]
    if not file.exists():
        print(f'File "{file}" not found, attempting download...')
        download_file(url, file.parent, file.name)
    return file


@log_start_finish
def _proc_qs_to_pq(year):
    "Convert QuickStats table from source CSV to parquet."

    path = PATH['pq_part'][year]

    print(f'Converting Ag Census {year} to parquet...')
    
    src_path = _get_qs_src(year)
    df = pd.read_csv(src_path, sep='\t', dtype=str)

    # YEAR: stored as folder within parquet dataset
    assert (df['YEAR'] == str(year)).all()
    del df['YEAR']

    # VALUE: convert to numeric and create flag variable
    df['VALUE_F'] = df['VALUE'].astype(pd.CategoricalDtype(['NUM', '(D)', '(Z)'])).fillna('NUM')
    df['VALUE'] = pd.to_numeric(df['VALUE'].str.replace(',', ''), 'coerce')
    assert (df['VALUE'].notna() == (df['VALUE_F'] == 'NUM')).all()

    # CV_%: convert to numeric and create flag variable
    df['CV_%_F'] = df['CV_%'].astype(pd.CategoricalDtype(['NUM', '(H)', '(D)', '(L)']))
    df['CV_%'] = pd.to_numeric(df['CV_%'].str.replace(',', ''), 'coerce')
    df.loc[df['CV_%'].notna(), 'CV_%_F'] = 'NUM'
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine='pyarrow', index=False)
    print(f'Saved {len(df):,d} rows to parquet.')


_fields = [
    ('SOURCE_DESC',
    '60',
    'Source of data (CENSUS or SURVEY). Census program includes the Census  of Ag as well as follow up projects. Survey program includes national, state,  and county surveys.'),
    ('SECTOR_DESC',
    '60',
    'Five high  level, broad categories useful to narrow down choices (Crops, Animals & Products, Economics,  Demographics, and Environmental).'),
    ('GROUP_DESC',
    '80',
    'Subsets within sector (e.g., under sector = Crops, the groups are Field  Crops, Fruit & Tree Nuts, Horticulture, and Vegetables).'),
    ('COMMODITY_DESC',
    '80',
    'The primary subject of interest (e.g., CORN, CATTLE, LABOR, TRACTORS,  OPERATORS).'),
    ('CLASS_DESC',
    '180',
    'Generally a physical attribute (e.g., variety, size, color, gender)  of the commodity.'),
    ('PRODN_PRACTICE_DESC',
    '180',
    'A method of production or action taken on the commodity (e.g., IRRIGATED,  ORGANIC, ON FEED).'),
    ('UTIL_PRACTICE_DESC',
    '180',
    'Utilizations (e.g., GRAIN, FROZEN, SLAUGHTER) or marketing channels  (e.g., FRESH MARKET, PROCESSING, RETAIL).'),
    ('STATISTICCAT_DESC',
    '80',
    'The aspect of a commodity being measured (e.g., AREA HARVESTED, PRICE  RECEIVED, INVENTORY, SALES).'),
    ('UNIT_DESC',
    '60',
    'The unit associated with the statistic category (e.g., ACRES, $ / LB,  HEAD, $, OPERATIONS).'),
    ('SHORT_DESC',
    '512',
    'A concatenation of six columns: commodity_desc, class_desc,  prodn_practice_desc, util_practice_desc, statisticcat_desc, and unit_desc.'),
    ('DOMAIN_DESC',
    '256',
    'Generally another characteristic of operations that produce a  particular commodity (e.g., ECONOMIC CLASS, AREA OPERATED, NAICS  CLASSIFICATION, SALES). For chemical usage data, the domain describes the  type of chemical applied to the commodity. The domain = TOTAL will have no  further breakouts; i.e., the data value pertains completely to the  short_desc.'),
    ('DOMAINCAT_DESC',
    '512',
    'Categories or partitions within a domain (e.g., under domain = Sales, domain categories include  $1,000 TO $9,999, $10,000 TO $19,999, etc).'),
    ('AGG_LEVEL_DESC',
    '40',
    'Aggregation level or geographic granularity of the data (e.g., State, Ag District, County, Region, Zip Code).'),
    ('STATE_ANSI',
    '2',
    'American National Standards Institute (ANSI) standard 2-digit state  codes.'),
    ('STATE_FIPS_CODE',
    '2',
    'NASS 2-digit state codes; include 99 and 98 for US TOTAL and OTHER  STATES, respectively; otherwise match ANSI codes.'),
    ('STATE_ALPHA', '2', 'State abbreviation, 2-character alpha code.'),
    ('STATE_NAME', '30', 'State full name.'),
    ('ASD_CODE',
    '2',
    'NASS defined county groups, unique within a state, 2-digit ag  statistics district code.'),
    ('ASD_DESC', '60', 'Ag statistics district name.'),
    ('COUNTY_ANSI', '3', 'ANSI standard 3-digit county codes.'),
    ('COUNTY_CODE',
    '3',
    'NASS 3-digit county codes; includes 998 for OTHER (COMBINED) COUNTIES  and Alaska county codes; otherwise match ANSI codes.'),
    ('COUNTY_NAME', '30', 'County name.'),
    ('REGION_DESC',
    '80',
    'NASS defined geographic entities not readily defined by other  standard geographic levels. A region can be a less than a state (Sub-State) or a group of states (Multi-State), and may be specific to  a commodity.'),
    ('ZIP_5', '5', 'US Postal Service 5-digit zip code.'),
    ('WATERSHED_CODE',
    '8',
    'US Geological Survey (USGS) 8-digit Hydrologic Unit Code (HUC) for  watersheds.'),
    ('WATERSHED_DESC', '120', 'Name assigned to the HUC.'),
    ('CONGR_DISTRICT_CODE', '2', 'US Congressional District 2-digit code.'),
    ('COUNTRY_CODE',
    '4',
    'US Census Bureau, Foreign Trade Division 4-digit country code, as of  April, 2007.'),
    ('COUNTRY_NAME', '60', 'Country name.'),
    ('LOCATION_DESC', '120', 'Full description for the location dimension.'),
    ('YEAR', '4', 'The numeric year of the data.'),
    ('FREQ_DESC',
    '30',
    'Length of time covered (Annual,  Season, Monthly, Weekly, Point in Time). Monthly often covers more than one month. Point in Time is as of a particular  day.'),
    ('BEGIN_CODE',
    '2',
    'If applicable, a 2-digit code corresponding to the beginning of the  reference period (e.g., for freq_desc = Monthly,  begin_code ranges from 01 (January) to 12 (December)).'),
    ('END_CODE',
    '2',
    'If applicable, a 2-digit code corresponding to the end of the  reference period (e.g., the reference period of Jan thru Mar will have begin_code = 01 and end_code = 03).'),
    ('REFERENCE_PERIOD_DESC',
    '40',
    'The specific time frame, within a freq_desc.'),
    ('WEEK_ENDING', '10', 'Week ending date, used when freq_desc = Weekly.'),
    ('LOAD_TIME',
    '19',
    'Date and time indicating when record was inserted into Quick Stats  database.'),
    ('VALUE', '24', 'Published data value or suppression reason code.'),
    ('VALUE_F', '3', 'VALUE flag: NUM, (D) or (Z)'),
    ('CV_%',
    '7',
    'Coefficient of variation. Available for the 2012 Census of Agriculture only. County-level CVs are generalized.'),
    ('CV_%_F', '3', 'CV_% flag: NUM, (H), (D) or (L)')
]


def get_schema():
    schema = []
    for f in _fields:
        n = f[0]
        if n == 'YEAR':
            t = pyarrow.int32()
        elif n in ['VALUE', 'CV_%']:
            t = pyarrow.float64()
        elif n in ['VALUE_F', 'CV_%_F']:
            t = pyarrow.dictionary(pyarrow.int32(), pyarrow.string())
        else:
            t = pyarrow.string()
        af = pyarrow.field(n, t, metadata={'desc': f[2], 'max_len': f[1]})
        schema.append(af)
    schema = pyarrow.schema(schema)
    return schema


def get_df(years, cols=None, filters=None):
    for year in years:
        if not PATH['pq_part'][year].exists():
            _proc_qs_to_pq(year)
    if filters is None:
        filters = []
    filters.append(('YEAR', 'in', years))
    # convert filters from list of tuples to expression acceptable by dataset.to_table()
    filters = pyarrow.parquet._filters_to_expression(filters)
        
    ds = pyarrow.dataset.dataset(PATH['proc'], 
                                 partitioning=pyarrow.dataset.partitioning(field_names=['YEAR']),
                                 schema=get_schema())

    df = ds.to_table(columns=cols, filter=filters).to_pandas()
    return df

def test_get_df(redownload=False):
    cleanup(redownload)
    d = get_df([2002, 2007, 2012, 2017], ['YEAR', 'SECTOR_DESC', 'VALUE'])
    assert len(d) > 0


def test_all(redownload=False):
    test_get_df(redownload)

