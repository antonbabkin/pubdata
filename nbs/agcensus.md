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
title: "NASS - Census of Agriculture"
format:
  html:
    code-fold: true
    ipynb-filters:
      - pubdata/reseng/nbd.py filter-docs
---
```

+++ {"tags": ["nbd-docs"]}

The Census of Agriculture is a complete count of U.S. farms and ranches and the people who operate them.
Census is conducted USDA NASS (National Agricultural Statistics Service) every 5 years, and the most recently available is 2017.

[Ag Census homepage](https://www.nass.usda.gov/AgCensus/index.php)

```{code-cell} ipython3
:tags: [nbd-module]

import shutil

import numpy as np
import pandas as pd
import pyarrow.parquet
import pyarrow.dataset

from pubdata.reseng.util import download_file
from pubdata.reseng.monitor import log_start_finish
from pubdata.reseng.nbd import Nbd
nbd = Nbd('pubdata')
```

```{code-cell} ipython3
:tags: [nbd-module]

PATH = {
    'source': nbd.root / 'data/agcensus/source',
    'proc': nbd.root / 'data/agcensus/agcensus.parquet',
    'pq_part': {
        2002: nbd.root / 'data/agcensus/agcensus.parquet/2002/part.pq',
        2007: nbd.root / 'data/agcensus/agcensus.parquet/2007/part.pq',
        2012: nbd.root / 'data/agcensus/agcensus.parquet/2012/part.pq',
        2017: nbd.root / 'data/agcensus/agcensus.parquet/2017/part.pq',
        2022: nbd.root / 'data/agcensus/agcensus.parquet/2022/part.pq'
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
```

+++ {"tags": ["nbd-docs"]}

# Source data

Census tabulations are available in PDF format for [2017](https://www.nass.usda.gov/Publications/AgCensus/2017/index.php) and [all previous years](https://agcensus.library.cornell.edu/).

QuickStats provides access to 2002, 2007, 2012 and 2017 data2002, 2007, 2012 and 2017 data via [browser interface](https://quickstats.nass.usda.gov/), [API](https://quickstats.nass.usda.gov/api/) and [bulk download](https://www.nass.usda.gov/datasets/).

```{code-cell} ipython3
:tags: [nbd-module]

def _get_qs_src(year):
    init_dirs()
    url = f'https://www.nass.usda.gov/datasets/qs.census{year}.txt.gz'
    file = PATH['source'] / url.split('/')[-1]
    if not file.exists():
        print(f'File "{file}" not found, attempting download...')
        download_file(url, file.parent, file.name)
    return file
```

# Save as parquet

Source files are big, e.g. 2017 CSV loaded as str type uses 20 GB of memory.
All QuickStats Census tables have the same layout in each year, and in this module we retrieve and save them as a partitioned parquet dataset.

Flags information from census methodology (2022):

```
The following abbreviations and symbols are used throughout the tables: 
- 	Represents zero. 
(D) 	Withheld to avoid disclosing data for individual farms. 
(H) 	Coefficient of variation is greater than or equal to 99.95 percent or the 
standard error is greater than or equal to 99.95 percent of mean. 
(IC) 	Independent city. 
(L) 	Coefficient of variation is less than 0.05 percent or the standard error 
is less than 0.05 percent of the mean. 
(NA) 	Not available. 
(X) 	Not applicable. 
(Z) 	Less than half of the unit shown.
```

The `VALUE` and `CV_%` columns mix numerical values and character flags.
In the processed dataset `(D)`, `(H)`, `(X)` and `(L)` are replaced with NA, `(Z)` is replaced with zero, and the flag is preseved in an additional columns `VALUE_F` and `CV_%_F`.


Disclosure suppression details are available in census [methodology](https://www.nass.usda.gov/Publications/AgCensus/2022/Full_Report/Volume_1,_Chapter_1_US/usappxa.pdf).

```{code-cell} ipython3
:tags: [nbd-module]

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
    df['VALUE_F'] = df['VALUE'].astype(pd.CategoricalDtype(['NUM', '(D)', '(Z)', '(X)'])).fillna('NUM')
    df['VALUE'] = pd.to_numeric(df['VALUE'].str.replace(',', ''), 'coerce')
    df.loc[df['VALUE_F'] == '(Z)', 'VALUE'] = 0
    assert (df['VALUE'].notna() == df['VALUE_F'].isin(['NUM', '(Z)'])).all()

    # CV_%: convert to numeric and create flag variable
    df['CV_%_F'] = df['CV_%'].astype(pd.CategoricalDtype(['NUM', '(H)', '(D)', '(L)']))
    df['CV_%'] = pd.to_numeric(df['CV_%'].str.replace(',', ''), 'coerce')
    df.loc[df['CV_%'].notna(), 'CV_%_F'] = 'NUM'
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine='pyarrow', index=False)
    print(f'Saved {len(df):,d} rows to parquet.')
```

```{code-cell} ipython3
src_path = _get_qs_src(2022)
df = pd.read_csv(src_path, sep='\t', dtype=str)
```

```{code-cell} ipython3
_proc_qs_to_pq(2022)
```

+++ {"tags": ["nbd-docs"]}

# Schema

+++

Layout of tables is on QuickStats API help page.
Code below parses to prepare fields and their description in the `_fields` list.
Parquet schema is made from this list and then used to load dataset partions correct datatypes.
Some fields are all empty in some years, and this creates inconsistent datatypes when loadning multiple years.
Manually providing schema fixes this.

```{code-cell} ipython3
t = pd.read_html('https://quickstats.nass.usda.gov/api/', header=0)[0]
t = t[t.iloc[:, 0] != t.iloc[:, 1]]
schema = []
for r in t.itertuples(False):
    n, l, d = r
    
    if n == 'CV %':
        n = 'CV_%'
    elif n == 'reference_period_  desc (Period)':
        n = 'REFERENCE_PERIOD_DESC'
    else:
        n = n.split(' ')[0].upper()
    schema.append((n, l, d))
```

```{code-cell} ipython3
---
jupyter:
  source_hidden: true
tags: [nbd-module]
---
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
```

```{code-cell} ipython3
:tags: [nbd-module]

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
```

+++ {"tags": ["nbd-docs"]}

::: {.callout-note collapse=true}

## list of dataset fields

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: [nbd-docs]
---
for f in get_schema():
    print('----')
    print(f.name)
    print(f.metadata[b'desc'].decode())
```

+++ {"tags": ["nbd-docs"]}

:::

+++ {"tags": ["nbd-docs"]}

# Load dataset

Use `get_df()` function to load dataset as pandas dataframe.
`filters` argument should be used to create queries using list of filters formatted as documented for pyarrow filters [here](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html).

```{code-cell} ipython3
:tags: [nbd-module]

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
    d = get_df([2002, 2007, 2012, 2017, 2022], ['YEAR', 'SECTOR_DESC', 'VALUE'])
    assert len(d) > 0
```

```{code-cell} ipython3
test_get_df()
```

+++ {"tags": ["nbd-docs"]}

# Example: hired and contract labor

This example shows how usage of contract and hired labor, measured as percentage of farm expenses, changed over time in California and Wisconsin.

```{code-cell} ipython3
:tags: [nbd-docs]

df = get_df(years=[2002, 2007, 2012, 2017, 2022], cols=['YEAR', 'STATE_NAME', 'SHORT_DESC',  'VALUE'],
            filters=[('DOMAIN_DESC', '==', 'TOTAL'),
                     ('SHORT_DESC', 'in', [
                         'LABOR, HIRED - EXPENSE, MEASURED IN PCT OF OPERATING EXPENSES', 
                         'LABOR, CONTRACT - EXPENSE, MEASURED IN PCT OF OPERATING EXPENSES']),
                     ('STATE_FIPS_CODE', 'in', ['06', '55'])])
df['LABOR_TYPE'] = df['SHORT_DESC'].str.split(expand=True)[1]
df.set_index(['YEAR', 'STATE_NAME', 'LABOR_TYPE'])['VALUE'].unstack(['STATE_NAME', 'LABOR_TYPE'])
```

+++ {"tags": ["nbd-docs"]}

# Example: commodity sales by state

This more detailed example shows how to retrieve state-level values of farm sales by commodity type.
In the map below color indicates commodity with the highest amount of sales.
Hover over states to view full list of sales.

```{code-cell} ipython3
:tags: [nbd-docs]

# load relevant subset of the dataset
df = get_df(years=[2022], cols=['COMMODITY_DESC', 'SHORT_DESC', 'STATE_FIPS_CODE',  'VALUE'],
            filters=[('DOMAIN_DESC', '==', 'TOTAL'),
                     ('STATISTICCAT_DESC', '==', 'SALES'),
                     ('UNIT_DESC', '==', '$'),
                     ('AGG_LEVEL_DESC', '==', 'STATE')])

# select sales items to report
sales_items = [
# 'COMMODITY TOTALS - SALES, MEASURED IN $',
    # 'CROP TOTALS - SALES, MEASURED IN $',
        # 'GRAIN - SALES, MEASURED IN $', 
            'CORN - SALES, MEASURED IN $',
            'WHEAT - SALES, MEASURED IN $',
            'SOYBEANS - SALES, MEASURED IN $',
            'SORGHUM - SALES, MEASURED IN $',
            'BARLEY - SALES, MEASURED IN $',
            'RICE - SALES, MEASURED IN $',
            'GRAIN, OTHER - SALES, MEASURED IN $',
        'TOBACCO - SALES, MEASURED IN $',
        'COTTON, LINT & SEED - SALES, MEASURED IN $',
        'VEGETABLE TOTALS, INCL SEEDS & TRANSPLANTS, IN THE OPEN - SALES, MEASURED IN $',
        'FRUIT & TREE NUT TOTALS - SALES, MEASURED IN $',
            # 'FRUIT & TREE NUT TOTALS, (EXCL BERRIES) - SALES, MEASURED IN $',
            # 'BERRY TOTALS - SALES, MEASURED IN $',
        'HORTICULTURE TOTALS, (EXCL CUT TREES & VEGETABLE SEEDS & TRANSPLANTS) - SALES, MEASURED IN $',
        'CUT CHRISTMAS TREES & SHORT TERM WOODY CROPS - SALES, MEASURED IN $',
            # 'CUT CHRISTMAS TREES - SALES, MEASURED IN $',
            # 'SHORT TERM WOODY CROPS - SALES, MEASURED IN $',
        'FIELD CROPS, OTHER, INCL HAY - SALES, MEASURED IN $',
            # 'MAPLE SYRUP - SALES, MEASURED IN $',
    # 'ANIMAL TOTALS, INCL PRODUCTS - SALES, MEASURED IN $',
        'POULTRY TOTALS, INCL EGGS - SALES, MEASURED IN $',
        'CATTLE, INCL CALVES - SALES, MEASURED IN $',
        'MILK - SALES, MEASURED IN $', 
        'HOGS - SALES, MEASURED IN $',
        'SHEEP & GOATS TOTALS, INCL WOOL & MOHAIR & MILK - SALES, MEASURED IN $',
        'EQUINE, (HORSES & PONIES) & (MULES & BURROS & DONKEYS) - SALES, MEASURED IN $',
        'AQUACULTURE TOTALS - SALES & DISTRIBUTION, MEASURED IN $'
        'SPECIALTY ANIMAL TOTALS, (EXCL EQUINE) - SALES, MEASURED IN $',
]
df = df.query('SHORT_DESC.isin(@sales_items)')

df['COMMODITY_DESC'] = df['COMMODITY_DESC'].str.replace(' TOTALS', '')

df_sales = df

# state shapes for mapping
from pubdata import geography
df = geography.get_state_df(scale='20m')\
    .query('CONTIGUOUS')\
    .rename(columns={'CODE': 'STATE_FIPS_CODE', 'NAME': 'STATE'})\
    [['STATE_FIPS_CODE', 'STATE', 'geometry']]

# top sales commodity for coloring
d = df_sales.sort_values('VALUE', ascending=False)\
    .groupby('STATE_FIPS_CODE')\
    .first()\
    .rename(columns={'COMMODITY_DESC': 'TOP_SALES_COMMODITY'})\
    .reset_index()\
    [['STATE_FIPS_CODE', 'TOP_SALES_COMMODITY']]
df = df.merge(d, how='left', on='STATE_FIPS_CODE')

# commodity sales for popups
d = df_sales.set_index(['STATE_FIPS_CODE', 'COMMODITY_DESC'])['VALUE']
d /= 1000
d = d.unstack().reset_index()
df = df.merge(d, how='left', on='STATE_FIPS_CODE')

# display interactive map
df.explore(column='TOP_SALES_COMMODITY', tiles='CartoDB positron')
```

# Full test

```{code-cell} ipython3
:tags: [nbd-module]

def test_all(redownload=False):
    test_get_df(redownload)
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
---
test_all(redownload=False)
```

# Build this module

```{code-cell} ipython3
nbd.nb2mod('agcensus.ipynb')
```
