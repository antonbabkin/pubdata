---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.16.1
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

```{raw-cell}

---
title: "Quarterly Census of Employment and Wages"
format:
  html:
    code-fold: true
    embed-resources: true
---
```

The [Quarterly Census of Employment and Wages](https://www.bls.gov/cew/) (QCEW) program publishes a quarterly count of employment and wages reported by employers covering more than 95 percent of U.S. jobs, available at the county, MSA, state and national levels by industry.
The program is run by the US Bureau of Labor Statistics (BLS) of the US Department of Labor.

```{code-cell} ipython3
:tags: [nbd-module]

import shutil

import pandas as pd
import pyarrow, pyarrow.parquet, pyarrow.dataset

from pubdata.reseng.monitor import log_start_finish
from pubdata.reseng.util import download_file
from pubdata.reseng.caching import simplecache
from pubdata.reseng.nbd import Nbd
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
```

# Source files

[Data files](https://www.bls.gov/cew/downloadable-data-files.htm) |
[CSV layout](https://www.bls.gov/cew/about-data/downloadable-file-layouts/annual/naics-based-annual-layout.htm)

Using CSVs, Single Files, Annual Averages.

```{code-cell} ipython3
:tags: [nbd-module]

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
```

```{code-cell} ipython3
_test_get_src()
```

# Parquet dataset

Save all years (1990-2022) as a single parquet dataset.

```{code-cell} ipython3
:tags: [nbd-module]

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
```

# Load dataset

`disclosure_code` column exists in all years, but is has values only beginning in 2001.
Parquet dataset reader needs to receive explicit schema to always treat the column as string.

```{code-cell} ipython3
:tags: [nbd-module]

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
```

# Example

Total employment in Dane county, Wisconsin.

```{code-cell} ipython3
get_df(range(1990, 2023), ['year', 'annual_avg_emplvl'], 
       [('agglvl_code', '==', '70'), ('area_fips', '==', '55025')])\
    .set_index('year').plot()
```

# Full test

```{code-cell} ipython3
:tags: [nbd-module]

def test_all(redownload=False):
    _test_get_df(redownload)
```

```{code-cell} ipython3
test_all(redownload=False)
```

# Build this module

```{code-cell} ipython3
nbd.nb2mod('qcew.ipynb')
```
