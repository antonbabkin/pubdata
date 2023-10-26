---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.4
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

```{raw-cell}
:tags: []

---
title: "Core based statistical area"
format:
  html: 
    code-fold: true
    ipynb-filters:
      - pubdata/reseng/nbd.py filter-docs
---
```

+++ {"tags": ["nbd-docs"]}

[Census page](https://www.census.gov/programs-surveys/metro-micro.html) |
[About](https://www.census.gov/programs-surveys/metro-micro/about.html) |
[Glossary](https://www.census.gov/programs-surveys/metro-micro/about/glossary.html) |
Delineation tables - [recent](https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html),
[historical](https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/historical-delineation-files.html)

The United States Office of Management and Budget (OMB) delineates metropolitan and micropolitan statistical areas according to published standards that are applied to Census Bureau data.
The general concept of a metropolitan or micropolitan statistical area is that of a core area containing a substantial population nucleus, together with adjacent communities having a high degree of economic and social integration with that core.


# Definitions

The term **"core based statistical area" (CBSA)** became effective in 2000 and refers collectively to metropolitan and micropolitan statistical areas.
The 2010 standards provide that each CBSA must contain at least one urban area of 10,000 or more population.
Each **metropolitan statistical area** must have at least one urbanized area of 50,000 or more inhabitants.
Each **micropolitan statistical area** must have at least one urban cluster of at least 10,000 but less than 50,000 population.
**Combined statistical areas (CSAs)** are aggregates of adjacent metropolitan or micropolitan statistical areas that are linked by commuting ties.

Counties or equivalent entities form the geographic "building blocks" for metropolitan and micropolitan statistical areas throughout the United States and Puerto Rico.
Under the standards, the county (or counties) in which at least 50 percent of the population resides within urban areas of 10,000 or more population, or that contain at least 5,000 people residing within a single urban area of 10,000 or more population, is identified as a **"central county"** (counties). 
Additional **"outlying counties"** are included in the CBSA if they meet specified requirements of commuting to or from the central counties.

If specified criteria are met, a metropolitan statistical area containing a single core with a population of 2.5 million or more may be subdivided to form smaller groupings of counties referred to as **"metropolitan divisions."**

The largest city in each metropolitan or micropolitan statistical area is designated a **"principal city."**

In view of the importance of cities and town in New England, the 2010 standards also provide for a set of geographic areas that are delineated using cities and towns in the six New England states.
The **New England city and town areas (NECTAs)** are delineated using the same criteria as metropolitan and micropolitan statistical areas.
Similarly to CBSAs, there are metropolitan and micropolitan NECTAs, combined NECTAs, and NECTA divisions.


# Delineation revisions

A metropolitan or micropolitan statistical area's geographic composition, or list of geographic components at a particular point in time, is referred to as its **"delineation."**
Metropolitan and micropolitan statistical areas are delineated by the U.S. Office of Management and Budget (OMB) and are the result of the application of published standards to Census Bureau data.
The standards for delineating the areas are reviewed and revised once every ten years, prior to each decennial census.
Generally, the areas are delineated using the most recent set of standards following each decennial census.
Between censuses, the delineations are revised to reflect Census Bureau population estimates and--once each decade--updated commuting-to-work data. 
Areas based on the 2010 standards and Census Bureau data were delineated in February of 2013, and updated in July of 2015, August of 2017, April of 2018, September of 2018, and March of 2020.
Currently delineated metropolitan and micropolitan statistical areas are based on application of 2020 standards (which appeared in the Federal Register on July 16, 2021) to 2020 Census and 2016-2020 American Community Survey data, as well as Vintage 2021 Population Estimates Program data. Current metropolitan and micropolitan statistical area delineations were announced by OMB effective July 2023.

Changes in the delineations of these statistical areas since the 1950 census have consisted chiefly of:
- the recognition of new areas as they reached the minimum required urban area or city population, and
- the addition of counties (or cities and towns in New England) to existing areas as new commuting and urban area data showed them to qualify.

In some instances, formerly separate areas have been merged, components of an area have been transferred from one area to another, or components have been dropped from an area. The large majority of changes have taken place on the basis of decennial census (and more recently American Community Survey) data. However, Census Bureau Population Estimates Program and American Community Survey data serve as the basis for intercensal updates in specified circumstances.


:::{.callout-note collapse="true"}

## List of available revisions

| Revision | Census |
|----------|--------|
| Jul 2023 | 2020   |
| Mar 2020 | 2010   |
| Sep 2018 | 2010   |
| Apr 2018 | 2010   |
| Aug 2017 | 2010   |
| Jul 2015 | 2010   |
| Feb 2013 | 2010   |
| Dec 2009 | 2000   |
| Nov 2008 | 2000   |
| Nov 2007 | 2000   |
| Dec 2006 | 2000   |
| Dec 2005 | 2000   |
| Nov 2004 | 2000   |
| Dec 2003 | 2000   |
| Jun 2003 | 2000   |
| Jun 1999 | 2000*  |
| Jun 1993 | 1990** |
| Jun 1990 | 1990*  |
| Jun 1983 | 1980** |
| Jun 1981 | 1980*  |
| Apr 1973 | 1980** |
| Feb 1971 | 1970*  |
| Oct 1963 | 1960** |
| Nov 1960 | 1960*  |
| Oct 1950 | 1950*  |

\* Delineations used for presenting metropolitan area statistics in upcoming Census publications.  
\** Delineations based on application of metropolitan area standards to preceding census data.

:::

```{code-cell} ipython3
:tags: [nbd-module]

import functools
import typing
import warnings
import shutil
from contextlib import redirect_stdout

import pandas as pd
import geopandas

from pubdata.reseng import util
from pubdata.reseng.caching import simplecache
from pubdata.reseng.monitor import log_start_finish
from pubdata.reseng.nbd import Nbd

nbd = Nbd('pubdata')
```

```{code-cell} ipython3
:tags: [nbd-module]

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
```

+++ {"tags": ["nbd-docs"]}

# Delineation tables

Function `get_cbsa_delin_df(year)` returns delineation dataframe for a chosen year.

+++

Delineation tables are rather small files, and so there is no need to cache processed versions.

```{code-cell} ipython3
:tags: [nbd-module]

def get_cbsa_delin_src(year: int):
    """Download and return path to CBSA delineation file.
    When more than one revision exists in year (as in 2003 or 2018),
    the most recent one in that year is used.
    """
    
    init_dirs()

    base = 'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/'
    urls = {
        2023: f'{base}2023/delineation-files/list1_2023.xlsx',
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
    if year in [2003, 2013, 2015, 2017, 2018, 2020, 2023]:
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
    elif year in [2013, 2023]:
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
    elif 2013 <= year <= 2023:
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
```

## 1990s revisions

1993 delineation is hierarchical "staircase" table, CMSA -> MSA -> county -> city.
CMSA = Consolidated Metropolitan Statistical Area, a collection of MSA's.
First column is both for CMSA and MSA, and when it is CMSA, then MSA components are in the PRIMARY_MSA_CODE.

It seems that sometimes MSA boundary is going though the county, and then county NAME has "(pt.)" in it, and is followed by county towns that belong to the MSA. In that case, only towns - and not the county itself - are classified as central or outlying.

```{code-cell} ipython3
:tags: [nbd-module]

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
```

```{code-cell} ipython3
:tags: [nbd-module]

def cleanup_delin(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['source_delin'], ignore_errors=True)
    
@log_start_finish
def test_get_cbsa_delin_df(redownload=False):
    cleanup_delin(redownload)
    for year in [1993, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2013, 2015, 2017, 2018, 2020, 2023]:
        print(year)
        df = get_cbsa_delin_df(year)
        assert len(df) > 0
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_get_cbsa_delin_df()
```

+++ {"tags": ["nbd-docs"]}

## Examples

From 2020 CBSA delineation. 

Combined statistical area (CSA) "Madison-Janesville-Beloit, WI" consists of three core based statistical areas (CBSAs): two metropolitan statistical areas ("Madison, WI" and "Janesville-Beloit, WI") and one micropolitan statistical area ("Baraboo, WI"). No divisions exist within CBSAs, because neither CBSA has core of greater than 2.5 million people.
"Madison, WI" metro area consists of the central Dane county and three adjacent outlying counties - Columbia, Green and Iowa.

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: screen-inset
df = get_cbsa_delin_df(2020)
df.query('CSA_CODE == "357"')\
[['CSA_TITLE', 'CSA_CODE', 'CBSA_TITLE', 'CBSA_CODE', 'METRO_MICRO', 'STATE', 'COUNTY', 'STATE_CODE', 'COUNTY_CODE', 'CENTRAL_OUTLYING']]
```

+++ {"tags": ["nbd-docs"]}

All CBSAs that have divisions.

```{code-cell} ipython3
:tags: [nbd-docs]

df = get_cbsa_delin_df(2020)
d = df.query('DIVISION_CODE.notna()')[['CBSA_TITLE', 'CBSA_CODE', 'DIVISION_CODE']]
pd.concat([
    d.groupby(['CBSA_TITLE', 'CBSA_CODE']).size(),
    d.drop_duplicates().groupby(['CBSA_TITLE', 'CBSA_CODE']).size()
], axis=1).reset_index().rename(columns={0: 'counties', 1: 'divisions'})
```

```{code-cell} ipython3
:tags: []

#| label: tbl-cbsa-counts
#| tbl-cap: "Number of counties in CBSA delineation tables"
#| layout-nrow: 2
t0 = {}
t1 = {}
for year in [1993, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2013, 2015, 2017, 2018, 2020, 2023]:
    df = get_cbsa_delin_df(year)
    if year == 1993:
        df = df.dropna(subset='COUNTY_CODE').drop_duplicates(['STATE_CODE', 'COUNTY_CODE'])
        df['METRO_MICRO'] = 'metro'
    t0[year] = df['METRO_MICRO'].value_counts(dropna=False)
    if 'CENTRAL_OUTLYING' in df:
        t1[year] = df[['METRO_MICRO', 'CENTRAL_OUTLYING']].value_counts(dropna=False)
t0 = pd.concat(t0, axis=1)
t0 = t0.fillna(0).astype(int)
display(t0)
t1 = pd.concat(t1, axis=1).sort_index().fillna(0).astype(int)
t1
```

+++ {"tags": ["nbd-docs"]}

# Geographic shapefiles

Boundary shapefiles, both TIGER (high res) and catrographic (low res) are available from Census Bureau [geography program](https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html).

```{code-cell} ipython3
:tags: [nbd-module]

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
```

Before 2010 there is a column `CENSUSAREA` - "Area of entity before generalization in square miles". After 2010 there are `ALAND` and `AWATER`. For most entries `CENSUSAREA` equals `ALAND` after conversion from square miles to square meters, but not always.

```{code-cell} ipython3
:tags: []

df = {}
for y, s in _urls_cbsa_shape_src.keys():
    if s != '20m': continue
    f = get_cbsa_shape_src(y, '20m')
    d = geopandas.read_file(f, rows=0)
    df[y] = pd.Series(True, index=d.columns)
df = pd.concat(df, axis=1).fillna(False).replace({False: '', True: 'X'})
df
```

+++ {"tags": ["nbd-docs"]}

Function `get_cbsa_shape_df()` returns a geodataframe of a given year and scale.

```{code-cell} ipython3
:tags: [nbd-module]

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
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_get_cbsa_shape_df(redownload=False)
```

+++ {"tags": ["nbd-docs"]}

Example dataframe head.

```{code-cell} ipython3
:tags: [nbd-docs]

get_cbsa_shape_df(year=2021, scale='20m', geometry=True).head(3)
```

```{code-cell} ipython3
:tags: [nbd-docs]

#| fig-cap: "CBSAs of Wisconsin, 2021."
df = get_cbsa_shape_df(year=2021, scale='20m', geometry=True)
df.query('CBSA_TITLE.str.contains("WI")').explore()
```

+++ {"tags": ["nbd-docs"]}

# New England

New England city and town areas (NECTAs) are an alternative delineation available for the six states of New England that uses *county subdivisions* as building blocks.
Consequently, NECTA boundaries can cross county boundaries, as shown in the map below.

```{code-cell} ipython3
:tags: []

def get_necta_delin_src():
    url = 'https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list3_2020.xls'
    f = util.download_file(url, nbd.tmp)
    return f

def get_necta_delin_df():
    f = get_necta_delin_src()
    df = pd.read_excel(f, skiprows=2, skipfooter=4, dtype=str)
    df = df.rename(columns={
        'NECTA Code': 'NECTA_CODE',
        'NECTA Title': 'NECTA_TITLE',
        'FIPS State Code': 'STATE_CODE',
        'FIPS County Code': 'COUNTY_CODE',
        'FIPS County Subdivision Code': 'COUSUB_CODE',
        'Metropolitan/Micropolitan NECTA': 'METRO_MICRO'
    })
    return df

def get_cousub_src():
    url = 'https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_cousub_500k.zip'
    f = util.download_file(url, nbd.tmp)
    return f

def get_cousub_shp():
    f = get_cousub_src()
    df = geopandas.read_file(f)
    df = df.rename(columns={
        'STATEFP': 'STATE_CODE',
        'COUNTYFP': 'COUNTY_CODE',
        'COUSUBFP': 'COUSUB_CODE'
    })
    return df
```

```{code-cell} ipython3
:tags: []

# CT county subdivision shapes
ds = get_cousub_shp()
ds = ds.query('STATE_CODE == "09"')
```

```{code-cell} ipython3
:tags: []

# CT county shapes
from pubdata import geography
dc = geography.get_county_df(scale='500k')
dc = dc.query('STATE_CODE == "09"')
```

```{code-cell} ipython3
:tags: []

# CT NECTA shapes
def get_necta_shape_src():
    url = 'https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_necta_500k.zip'
    f = util.download_file(url, nbd.tmp)
    return f

def get_necta_shape():
    f = get_necta_shape_src()
    df = geopandas.read_file(f)
    df = df.rename(columns={
        'GEOID': 'NECTA_CODE',
    })
    return df


dn = get_necta_delin_df()
dn = dn.query('STATE_CODE == "09"')
dn = dn[['NECTA_CODE', 'NECTA_TITLE', 'METRO_MICRO']].drop_duplicates()

d = get_necta_shape()[['NECTA_CODE', 'geometry']]
dn = d.merge(dn)
```

```{code-cell} ipython3
:tags: [nbd-docs]

import folium
m = dc.explore(name='county', color='red')
ds.explore(m=m, name='subdivision', color='blue')
dn.explore(m=m, name='NECTA', color='green')
folium.LayerControl(collapsed=False).add_to(m)
m
```

# Full test

```{code-cell} ipython3
:tags: [nbd-module]

def test_all(redownload=False):
    test_get_cbsa_delin_df(redownload)
    test_get_cbsa_shape_df(redownload)
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_all(redownload=False)
```

+++ {"tags": []}

# Build this module

```{code-cell} ipython3
:tags: []

nbd.nb2mod('geography_cbsa.ipynb')
```
