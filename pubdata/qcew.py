#!/usr/bin/env python
# coding: utf-8

import shutil

import pandas as pd
import pyarrow, pyarrow.parquet, pyarrow.dataset

from .reseng.monitor import log_start_finish
from .reseng.util import download_file
from .reseng.caching import simplecache
from .reseng.nbd import Nbd
nbd = Nbd('pubdata')

PATH = {
    'source': nbd.root / 'data/qcew/source',
    'proc': nbd.root / 'data/qcew/qcew.parquet'
}

def _init_dirs():
    """Create necessary directories."""
    PATH['source'].mkdir(parents=True, exist_ok=True)
    PATH['proc'].mkdir(parents=True, exist_ok=True)

def cleanup(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['source'], ignore_errors=True)
    print('Removing processed files...')
    shutil.rmtree(PATH['proc'], ignore_errors=True)


@log_start_finish
def _get_src(year):
    _init_dirs()
    url = f'https://data.bls.gov/cew/data/files/{year}/csv/{year}_annual_singlefile.zip'
    f = download_file(url, PATH['source'])
    return f

def _test_get_src(redownload=False):
    cleanup(redownload)
    for y in range(1990, 2023):
        print(y, end=' ')
        _get_src(y)


_schema_pandas = {
    'area_fips': 'str',
    'own_code': 'str',
    'industry_code': 'str',
    'agglvl_code': 'str',
    'size_code': 'str',
    'year': 'int16',
    'qtr': 'str',
    'disclosure_code': 'str',
    'annual_avg_estabs': 'int64',
    'annual_avg_emplvl': 'int64',
    'total_annual_wages': 'int64',
    'taxable_annual_wages': 'int64',
    'annual_contributions': 'int64',
    'annual_avg_wkly_wage': 'int64',
    'avg_annual_pay': 'int64',
    'lq_disclosure_code': 'str',
    'lq_annual_avg_estabs': 'float64',
    'lq_annual_avg_emplvl': 'float64',
    'lq_total_annual_wages': 'float64',
    'lq_taxable_annual_wages': 'float64',
    'lq_annual_contributions': 'float64',
    'lq_annual_avg_wkly_wage': 'float64',
    'lq_avg_annual_pay': 'float64',
    'oty_disclosure_code': 'str',
    'oty_annual_avg_estabs_chg': 'int64',
    'oty_annual_avg_estabs_pct_chg': 'float64',
    'oty_annual_avg_emplvl_chg': 'int64',
    'oty_annual_avg_emplvl_pct_chg': 'float64',
    'oty_total_annual_wages_chg': 'int64',
    'oty_total_annual_wages_pct_chg': 'float64',
    'oty_taxable_annual_wages_chg': 'int64',
    'oty_taxable_annual_wages_pct_chg': 'float64',
    'oty_annual_contributions_chg': 'int64',
    'oty_annual_contributions_pct_chg': 'float64',
    'oty_annual_avg_wkly_wage_chg': 'int64',
    'oty_annual_avg_wkly_wage_pct_chg': 'float64',
    'oty_avg_annual_pay_chg': 'int64',
    'oty_avg_annual_pay_pct_chg': 'float64',   
}

@log_start_finish
def _build_pq(year):
    path = PATH['proc'] / f'{year}/part.pq'
    if path.exists(): return

    src = _get_src(year)
    df = pd.read_csv(src, dtype=_schema_pandas)
    assert (df['year'] == year).all()
    del df['year']
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine='pyarrow', index=False)


_dt_pd2pq = {
    'str': pyarrow.string(),
    'int16': pyarrow.int16(),
    'int64': pyarrow.int64(),
    'float64': pyarrow.float64()
}

_schema_parquet = pyarrow.schema([pyarrow.field(n, _dt_pd2pq[t]) for n, t in _schema_pandas.items()])

def get_df(years, cols=None, filters=None):
    for year in years:
        part_path = PATH['proc'] / f'{year}/part.pq'
        if not part_path.exists():
            _build_pq(year)
    if filters is None:
        filters = []
    filters.append(('year', 'in', years))
    # convert filters from list of tuples to expression acceptable by dataset.to_table()
    filters = pyarrow.parquet._filters_to_expression(filters)
        
    ds = pyarrow.dataset.dataset(PATH['proc'], 
                                 partitioning=pyarrow.dataset.partitioning(field_names=['year']),
                                 schema=_schema_parquet)

    df = ds.to_table(columns=cols, filter=filters).to_pandas()
    return df

def _test_get_df(redownload=False):
    cleanup(redownload)
    d = get_df(range(1990, 2023), 
               ['year', 'area_fips', 'annual_avg_estabs', 'oty_annual_avg_estabs_pct_chg', 'disclosure_code'],
               [('agglvl_code', '==', '70')])
    assert len(d) > 0


def test_all(redownload=False):
    _test_get_df(redownload)

