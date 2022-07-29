---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

```{raw-cell}

---
title: "Geographic gazetteer"
format:
  html:
    code-fold: true
execute:
  echo: false
jupyter: python3
---
```

Describe, retrieve and prepare dataframes with geographic boundaries of various geographic units of the USA.

```{code-cell} ipython3
:tags: [nbd-module]

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
import pyarrow.dataset, pyarrow.parquet

from pubdata.reseng.util import download_file
from pubdata.reseng.nbd import Nbd

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
```

```{code-cell} ipython3
:tags: [nbd-module]

# in geopandas 0.8, parquet support is still experimental
# https://geopandas.org/docs/user_guide/io.html#apache-parquet-and-feather-file-formats
import warnings
warnings.filterwarnings('ignore', message='.*initial implementation of Parquet.*')
```

```{code-cell} ipython3
:tags: []

import ipywidgets as widgets
import matplotlib as mpl
import matplotlib.pyplot as plt
import folium
```

+++ {"tags": []}

# Source files

Source data files are downloaded from web and cached locally.

Census Bureau:
- [TIGER Data Products Guide](https://www.census.gov/programs-surveys/geography/guidance/tiger-data-products-guide.html): Which Product Should I Use?
- Cartographic Boundary Files. [2018 and before](https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html), [2019 and after](https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html). Simplified representations of selected geographic areas from the Census Bureau’s MAF/TIGER geographic database. Small scale (limited detail) spatial files clipped to shoreline.
- [TIGER/Line shapefiles](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html). Most comprehensive geographic dataset in full detail.
- [Relationship files](https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.html). These text files describe geographic relationships. There are two types of relationship files; those that show the relationship between the same type of geography over time (comparability) and those that show the relationship between two types of geography for the same time period.
- [LSAD codes](https://www.census.gov/library/reference/code-lists/legal-status-codes.html). Legal/Statistical Area Description Codes and Definitions
- [FIPS codes](https://www.census.gov/geographies/reference-files/2019/demo/popest/2019-fips.html)
- [Gazeteer reference files](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html)
- [Character encoding](https://www.census.gov/programs-surveys/geography/technical-documentation/user-note/special-characters.html). Files from 2014 and earlier use "ISO-8859-1", 2015 and after use "UTF-8".

## Shapefile format

[Wikipedia](https://en.wikipedia.org/wiki/Shapefile)

Census Bureau shapefiles come as zipped folders and can be read directly with geopandas.

### XML metadata

Most zipped shapefile folders contain XML documents with metadata. Helper functions here parse these files for inspection. In later years files ending with `.iso.xml` adhere to ISO standards and can be more easily parsed for feature descriptions.

```{code-cell} ipython3
:tags: []

xml.etree.ElementTree.register_namespace('xlink', 'http://www.w3.org/1999/xlink')
xml.etree.ElementTree.register_namespace('gmd', 'http://www.isotc211.org/2005/gmd')
xml.etree.ElementTree.register_namespace('gco', 'http://www.isotc211.org/2005/gco')
xml.etree.ElementTree.register_namespace('gml', 'http://www.opengis.net/gml/3.2')
xml.etree.ElementTree.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')
xml.etree.ElementTree.register_namespace('gmi', 'http://www.isotc211.org/2005/gmi')
xml.etree.ElementTree.register_namespace('gmx', 'http://www.isotc211.org/2005/gmx')
xml.etree.ElementTree.register_namespace('gfc', 'http://www.isotc211.org/2005/gfc')

def _xml_tag(element):
    """Return tag with namespace URI replaced with it's shorthand."""
    tag = element.tag
    for uri, key in xml.etree.ElementTree._namespace_map.items():
        if key in tag:
            tag = tag.replace('{'+uri+'}', f'{key}:')
            break
    return tag

def _traverse(element, depth):
    """Recursively traverse XML tree and print element tags and text."""
    text = element.text
    text = text.strip() if text else ''
    text = f': {text}' if text else ''
    print('-' * depth, _xml_tag(element), text, sep='')
    for child in element.findall('*'):
        _traverse(child, depth + 1)
        

def print_shp_xml(shp_zip):
    """Print compact contents of all XML files within zipped shapefile."""
    with zipfile.ZipFile(shp_zip) as zip_file:
        for file in zip_file.infolist():
            if file.filename.endswith('.xml'):
                print('=' * 16, file.filename, '=' * 16)
                with zip_file.open(file) as file_obj:
                    parsed_xml = xml.etree.ElementTree.parse(file_obj)
                    _traverse(parsed_xml.getroot(), 0)

def extract_definitions_from_xml(shp_zip):
    """Return feature definitions from XML files within zipped shapefile."""
    definitions = {}
    with zipfile.ZipFile(shp_zip) as zip_file:
        for file in zip_file.infolist():
            if file.filename.endswith('.iso.xml'):
                with zip_file.open(file) as file_obj:
                    parsed_xml = xml.etree.ElementTree.parse(file_obj)
                    ns = {v:k for k, v in xml.etree.ElementTree._namespace_map.items()}
                    for attr in parsed_xml.iterfind('.//gfc:FC_FeatureAttribute', ns):
                        name = attr.find('gfc:memberName/*', ns).text
                        defn = attr.find('gfc:definition/*', ns).text
                        definitions[name] = defn
    return definitions
```

## Scale

Shapefiles are available in different scale. TIGER is the most precise, then follows 1:500,000, then 1:5,000,000, and 1:20,000,000 is the lowest resolution.

Shapefile revisions change from year to year. Between year differences are clearly visible in all scales except TIGER.

```{code-cell} ipython3
:tags: []

df = []

f = download_file('https://www2.census.gov/geo/tiger/TIGER2010/COUNTY/2010/tl_2010_09_county10.zip', nbd.root/'tmp')
d = geopandas.read_file(f).query('COUNTYFP10 == "013"')[['geometry']]
d['scale'] = 'tiger 2010'
df.append(d)

f = download_file('https://www2.census.gov/geo/tiger/TIGER2020/COUNTY/tl_2020_us_county.zip', nbd.root/'tmp')
d = geopandas.read_file(f).query('STATEFP == "09" and COUNTYFP == "013"')[['geometry']]
d['scale'] = 'tiger 2020' 
df.append(d)


f = download_file('https://www2.census.gov/geo/tiger/GENZ2010/gz_2010_us_050_00_500k.zip', nbd.root/'tmp')
d = geopandas.read_file(f).query('STATE == "09" and COUNTY == "013"')[['geometry']]
d['scale'] = '500k 2010'
df.append(d)

f = download_file('https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip', nbd.root/'tmp')
d = geopandas.read_file(f).query('STATEFP == "09" and COUNTYFP == "013"')[['geometry']]
d['scale'] = '500k 2020'
df.append(d)

f = download_file('https://www2.census.gov/geo/tiger/GENZ2010/gz_2010_us_050_00_5m.zip', nbd.root/'tmp')
d = geopandas.read_file(f).query('STATE == "09" and COUNTY == "013"')[['geometry']]
d['scale'] = '5m 2010'
df.append(d)

f = download_file('https://www2.census.gov/geo/tiger/GENZ2010/gz_2010_us_050_00_20m.zip', nbd.root/'tmp')
d = geopandas.read_file(f).query('STATE == "09" and COUNTY == "013"')[['geometry']]
d['scale'] = '20m 2010'
df.append(d)

df = pd.concat(df, ignore_index=True)
```

Table below compares boundaries of Tolland County, Connecticut, taken from shapefiles in different years and scales. "Length" column is is boundary length in shape units (degrees), and "points" is the total number of points in the polygon.

```{code-cell} ipython3
:tags: []

with warnings.catch_warnings():
    warnings.filterwarnings('ignore', message='.*Geometry is in a geographic CRS.*')
    d = df.copy()
    d.geometry = d.boundary
    d['length'] = d.geometry.length
    d['points'] = [len(x.coords) for x in d.geometry]
    d = d.drop(columns='geometry')
    d = d.set_index('scale')
    display(d)
```

Map below visualizes boundary differences.

```{code-cell} ipython3
:tags: []

d = df.copy()
d.geometry = d.boundary
d.explore(column='scale', tiles='CartoDB positron')
```

+++ {"tags": []}

# Regions

Regions are groups of states.
Multiple definitions of regions exist ([Wikipedia](https://en.wikipedia.org/wiki/List_of_regions_of_the_United_States)).

**Census Bureau** defined four regions (Northeast, Midwest, South and West), further subdivided into nine divisions.
Region and division shapefiles are available from CB geography program.

**Bureau of Economic Analysis** defines [eight regions](https://apps.bea.gov/regional/docs/msalist.cfm?mlist=2) (New England, Mideast, Great Lakes, Plains, Southeast, Southwest, Rocky Mountain and Far West).

```{code-cell} ipython3
:tags: [nbd-module]

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
```

+++ {"tags": []}

# States

[Wikipedia](https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States)

There are 50 states, a federal district (Washington, D.C.) and five major island territories.
Shapefile revisions are available for many years, but we use the most recent for simplicity.

Four scales are available: 1:20,000,000, 1:5,000,000, 1:500,000 and TIGER.
Four island territories (all except Puerto Rico) are not included in the 1:20m scale, likely because of small size.

Use `TERRITORY` and `CONTIGUOUS` bool columns to select commonly used subsets.

```{code-cell} ipython3
:tags: [nbd-module]

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
```

```{code-cell} ipython3
:tags: []

# test: download and prepare all state dataframes
# _data_cleanup_state('all')
_data_cleanup_state('processed')
for s in ['20m', '5m', '500k', 'tiger']:
    print(s)
    get_state_df(geometry=True, scale=s)
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
get_state_df(geometry=False)
```

```{code-cell} ipython3
:tags: []

get_state_df(scale='20m').query('CONTIGUOUS').explore(tiles='CartoDB positron')
```

+++ {"tags": []}

# Counties


**Source data**

[Cartographic Boundary Files](https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html) are available for 1990, 2000, 2010, 2013 and every year after that.

[TIGER/Line Shapefiles](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) in shapefile format are available for 2000, 2007 and every year after that. 1992 and 2006 are available in legacy format.


**Changes**

County changes happen whenever decided by local authoritities. Annually released boundary files reflect boundaries effective January 1 of the reference year. List of changes [here](https://www.census.gov/programs-surveys/geography/technical-documentation/county-changes.html).

> Substantial county boundary changes are those affecting an estimated population of 200 or more; changes of at least one square mile where an estimated population number was not available, but research indicated that 200 or more people may have been affected; and annexations of unpopulated territory of at least 10 square miles.

```{code-cell} ipython3
:tags: [nbd-module]

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
```

CRS in 1990 and 2000 is unknown, created dataframes have "naive geometries".

```{code-cell} ipython3
:tags: [nbd-module]

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
```

```{code-cell} ipython3
:tags: []

year = 2020
state = '55'
df = get_county_df(year).query('STATE_CODE == @state')
df.explore(tiles='CartoDB positron')
```

+++ {"tags": []}

# Census Tracts

Code is 11 digits: 2 state, 5 county, 4+2 tract.

[Reference](https://www2.census.gov/geo/pdfs/education/CensusTracts.pdf)

```{code-cell} ipython3
:tags: [nbd-module]

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
```

```{code-cell} ipython3
:tags: []

state = '55'
county = '025'
df = get_tract_df([2020], [state]).query('COUNTY_CODE == @county')
df.explore(tiles='CartoDB positron')
```

## Changes over time

Major changes to tract codes and shapes change after decennial censuses, with smaller changes in between years.

The first four digits of the tract code are "permanent." 
When tracks get large (+8000 residents), tracts are split and 2 digit tag is used (same with the split of splits):

|1990|2000|2010| 
|----|----|----|
|1000|1000.01|1000.03|
|1000|1000.01|1000.04|
|1000|1000.02|1000.05| 
|1000|1000.02|1000.06|
        
The naming convention for merges (population falls below 1,200) and boundary revisions are less clear-cut.

When changes (splits, merges, redefinitions) occur, the relationship of new tracts to old tracts is crosswalked.

There is a master file, as well as two files that provided the identifiers of tracts that were "substantially changed" between decennials. The two files of significantly changed census tracts consist only of a list of census tracts that exhibited a change of 2.5-percent or greater. Tract relationships may be one-to-one, many-to-one, one-to-many, or many-to-many.

Relationship files are currently available for 2010 (relative to 2000) and 2000 (relative to 1990).

- 2010: [data](https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.2010.html), [metadata](https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/2010-census-tract-record-layout.html).
- 2000: [data](https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.2000.html), [metadata](https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/2000-tract-relationship-record-layout.html).

```{code-cell} ipython3
:tags: [nbd-module]

def _get_tract_xwalk_time_src(y1: typing.Literal[2000, 2010]):
    """National relationship file for tract change between `y1` and `y1`-10."""
    urls = {
        2000: 'https://www2.census.gov/geo/relfiles/tract/us/us2kpop.txt',
        2010: 'https://www2.census.gov/geo/docs/maps-data/data/rel/trf_txt/us2010trf.txt'
    }
    local = PATH['source'] / f'tract_xwalk_time/{y1}.txt'
        
    if not local.exists():
        print(f'File "{local}" not found, attempting download.')
        download_file(urls[y1], local.parent, local.name)
    return local

def _get_tract_xwalk_time_meta(y1: typing.Literal[2000, 2010]):
    """Metadata for national relationship file for tract change between `y1` and `y1`-10."""
    base = 'https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/'
    urls = {
        2000: f'{base}2000-tract-relationship-record-layout.html',
        2010: f'{base}2010-census-tract-record-layout.html'
    }
    meta = pd.read_html(urls[y1])[0]
    return meta

def get_tract_xwalk_time_df(y1: typing.Literal[2000, 2010]):
    """Dataframe of national relationship table for tract change between `y1` and `y1`-10."""
    cache_path = PATH['geo'] / f'tract_xwalk_time/{y1}.pq'
    if cache_path.exists():
        return pd.read_parquet(cache_path)
    
    src = _get_tract_xwalk_time_src(y1)
    meta = _get_tract_xwalk_time_meta(y1)
    col_desc = {}
    y0 = y1 - 10
    if y1 == 2000:
        df = pd.read_fwf(src, encoding='ISO-8859-1', dtype='str', header=None,
                         widths=meta['Field Length'].tolist())
        df.columns = meta['Field Description']

        c = col_desc['ALAND'] = 'Land area of the record (1000 sq.meters)'
        df['ALAND'] = df[c].astype(int)
        c = col_desc['POP_2000'] = '2000 population of the area covered by the record'
        df['POP_2000'] = df[c].astype(int)    
        for y in [y0, y1]:
            col_desc[f'TRACT_{y}'] = f'{y} state + county + tract FIPS code'
            df[f'TRACT_{y}'] = df[f'{y} state FIPS code'] + df[f'{y} county FIPS code'] + df[f'{y} census tract base'] + df[f'{y} census tract suffix']
            col_desc[f'PART_{y}'] = f'{y} census tract part flag'
            df[f'PART_{y}'] = (df[f'{y} census tract part flag'] == 'P')
            col_desc[f'POP_PCT_{y}'] = f'Percentage of {y} census tract population'
            df[f'POP_PCT_{y}'] = df[f'Percentage of {y} census tract population*'].astype(int) / 10
    elif y1 == 2010:
        df = pd.read_csv(src, encoding='ISO-8859-1', dtype='str', header=None)
        df.columns = meta['Column Name']

        col_desc['ALAND'] = 'Land Area for the record'
        df['ALAND'] = df['AREALANDPT'].astype('int64')
        col_desc['POP_2010'] = 'Calculated 2010 Population for the relationship record'
        df['POP_2010'] = df['POP10PT'].astype(int)    
        for y in [y0, y1]:
            yy = str(y)[-2:]
            col_desc[f'TRACT_{y}'] = f'{y} state + county + tract FIPS code'
            df[f'TRACT_{y}'] = df[f'GEOID{yy}']
            col_desc[f'PART_{y}'] = f'{y} Tract Part Flag'
            df[f'PART_{y}'] = (df[f'PART{yy}'] == 'P')
            col_desc[f'POP_PCT_{y}'] = f'Calculated Percentage of the {y} population this record contains'
            df[f'POP_PCT_{y}'] = df[f'POPPCT{yy}'].astype(float)

    df.columns.name = None
    df = df[['ALAND', f'POP_{y1}',
             f'TRACT_{y0}', f'PART_{y0}', f'POP_PCT_{y0}',
             f'TRACT_{y1}', f'PART_{y1}', f'POP_PCT_{y1}']]
    
    # cache to parquet
    sch = pyarrow.Schema.from_pandas(df)
    for i, f in enumerate(sch):
        sch = sch.set(i, f.with_metadata({'description': col_desc[f.name]}))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, engine='pyarrow', index=False, schema=sch)
    
    return df
```

```{code-cell} ipython3
def plot_tract_change(y0, t0, y1, t1):
    state_code = t0[0][:2]
    ts0 = get_tract_df([y0], [state_code]).query('CODE.isin(@t0)')
    ts1 = get_tract_df([y1], [state_code]).query('CODE.isin(@t1)')

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.set_aspect('equal')
    ts0.boundary.plot(ax=ax, color='red', linewidth=3, alpha=0.4)
    for name, shape in ts0[['NAME', 'geometry']].itertuples(False):
        ax.annotate(name+'\n', shape.centroid.coords[0], horizontalalignment='center', color='red')
    ts1.boundary.plot(ax=ax, color='blue', linewidth=1)
    for name, shape in ts1[['NAME', 'geometry']].itertuples(False):
        ax.annotate('\n'+name, shape.centroid.coords[0], horizontalalignment='center', color='blue')

    ax.legend([mpl.lines.Line2D([0], [0], linewidth=3, alpha=0.4, color='red'),
                mpl.lines.Line2D([0], [0], linewidth=1, alpha=1, color='blue')],
               [y0, y1])

    state = get_state_df(False).query('CODE == @state_code')['NAME'].values[0]
    county_code = t0[0][:5]
    county = get_county_df(y1, False).query('CODE == @county_code')['NAME'].values[0]
    ax.set_title(f'{state}, {county} county ({county_code})')
    plt.close()
    return fig
```

::: {.hidden}
### Split

All tracts in $y_1$ are a partition of a single tract in $y_0$.

:::

```{code-cell} ipython3
:tags: []

#| output: false
def show_random_tract_split():
    w_y1 = widgets.Dropdown(description='Years', value=2010,
                            options=[('1990-2000', 2000), ('2000-2010', 2010)])
    w_state = widgets.Dropdown(description='State', value='00',
                               options=[['United States', '00']] + sorted(get_state_df(False)[['NAME', 'CODE']].values.tolist()))
    w_out = widgets.Output()
    w_show = widgets.Button(description='Show random split')
    def _prep():
        y1 = w_y1.value
        y0 = y1 - 10
        df = get_tract_xwalk_time_df(y1)
        if w_state.value != '00':
            df = df[df[f'TRACT_{y0}'].str[:2] == w_state.value]
        d = df.groupby(f'TRACT_{y0}')[f'PART_{y1}'].agg(['size', 'sum'])
        d = d.query('size > 1 and sum == 0')
        t0 = d.sample(1).index[0]
        d = df.query(f'TRACT_{y0} == @t0').reset_index(drop=True)
        t1 = d[f'TRACT_{y1}'].values.tolist()
        fig = plot_tract_change(y0, [t0], y1, t1)
        return d, fig
    def _show(_):
        with w_out:
            w_out.clear_output()
            print('Working...')
            d, f = _prep()
            w_out.clear_output()
            display(d)
            display(f)
    w_show.on_click(_show)
    display(widgets.VBox([widgets.HBox([w_y1, w_state, w_show]), w_out]))
    
show_random_tract_split()
```

::: {.hidden}
### Join

All tracts in $y_0$ are a partition of a single tract in $y_1$.

:::

```{code-cell} ipython3
:tags: []

#| output: false
def show_random_tract_join():
    w_y1 = widgets.Dropdown(description='Years', value=2010,
                            options=[('1990-2000', 2000), ('2000-2010', 2010)])
    w_state = widgets.Dropdown(description='State', value='00',
                               options=[['United States', '00']] + sorted(get_state_df(False)[['NAME', 'CODE']].values.tolist()))
    w_out = widgets.Output()
    w_show = widgets.Button(description='Show random join')
    def _prep():
        y1 = w_y1.value
        y0 = y1 - 10
        df = get_tract_xwalk_time_df(y1)
        if w_state.value != '00':
            df = df[df[f'TRACT_{y0}'].str[:2] == w_state.value]
        d = df.groupby(f'TRACT_{y1}')[f'PART_{y0}'].agg(['size', 'sum'])
        d = d.query('size > 1 and sum == 0')
        t1 = d.sample(1).index[0]
        d = df.query(f'TRACT_{y1} == @t1').reset_index(drop=True)
        t0 = d[f'TRACT_{y0}'].values.tolist()
        fig = plot_tract_change(y0, t0, y1, [t1])
        return d, fig
    def _show(_):
        with w_out:
            w_out.clear_output()
            print('Working...')
            d, f = _prep()
            w_out.clear_output()
            display(d)
            display(f)
    w_show.on_click(_show)
    display(widgets.VBox([widgets.HBox([w_y1, w_state, w_show]), w_out]))
    
show_random_tract_join()
```

::: {.hidden}
### Other reshape

Arbitrary change in boundaries between two or more adjacent tracts. May include multiple splits, joins and boundary shifts.

:::

```{code-cell} ipython3
:tags: []

#| output: false
def show_random_tract_reshape():
    w_y1 = widgets.Dropdown(description='Years', value=2010,
                            options=[('1990-2000', 2000), ('2000-2010', 2010)])
    w_state = widgets.Dropdown(description='State', value='00',
                               options=[['United States', '00']] + sorted(get_state_df(False)[['NAME', 'CODE']].values.tolist()))
    w_out = widgets.Output()
    w_show = widgets.Button(description='Show random reshape',
                            layout=widgets.Layout(width='auto'))
    def _prep():
        y1 = w_y1.value
        y0 = y1 - 10
        df = get_tract_xwalk_time_df(y1)
        if w_state.value != '00':
            df = df[df[f'TRACT_{y0}'].str[:2] == w_state.value]
        # Identification of reshaping cluster:
        # Start by picking an area that had it's tract number changed.
        # Than keep adding tracts that were affected by this change. 
        # Alternatively use package that finds connected components in bipartite graph,
        # where nodes on two sides are tracts, and edges are records in the change table.
        d = df.query(f'(TRACT_{y0} != TRACT_{y1}) and PART_{y0} and PART_{y1}')
        t0, t1 = d.sample(1)[[f'TRACT_{y0}', f'TRACT_{y1}']].values[0]
        t0, t1 = [t0], [t1]
        n = 1
        while True:
            d = df.query(f'TRACT_{y0}.isin(@t0) or TRACT_{y1}.isin(@t1)')
            if len(d) == n:
                break
            t0 = d[f'TRACT_{y0}'].unique().tolist()
            t1 = d[f'TRACT_{y1}'].unique().tolist()
            n = len(d)
        fig = plot_tract_change(y0, t0, y1, t1)
        return d.reset_index(drop=True), fig
    def _show(_):
        with w_out:
            w_out.clear_output()
            print('Working...')
            d, f = _prep()
            w_out.clear_output()
            display(d)
            display(f)
    w_show.on_click(_show)
    display(widgets.VBox([widgets.HBox([w_y1, w_state, w_show]), w_out]))
    
show_random_tract_reshape()
```

# ZIP Code Tabulation Area (ZCTA)

[Homepage](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/zctas.html)

> ZIP Code Tabulation Areas (ZCTAs) are generalized areal representations of United States Postal Service (USPS) ZIP Code service areas. The USPS ZIP Codes identify the individual post office or metropolitan area delivery station associated with mailing addresses. USPS ZIP Codes are not areal features but a collection of mail delivery routes.

ZCTAs are build from census block, thus blocks can be used as cross-walk to other geographics that partition into blocks. [Relationship files](https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.html) are available for blocks, counties, county subdivisions, places, tracts, and for ZCTA changes over time.

[Cartographic boundary](https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html) and [TIGER](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) shapefiles. 2000 files are only 3-digit codes. For some reason, the 2010 CB file is almost x10 bigger than other years - 527mb.

```{code-cell} ipython3
:tags: [nbd-module]

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
```

```{code-cell} ipython3
:tags: []

#| tbl-cap: "ZCTA shapefile columns over time"
df = {}
for y in [2000, 2010, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]:
    f = get_zcta_src(y)
    d = geopandas.read_file(f, rows=0)
    df[y] = pd.Series(True, index=d.columns)
df = pd.concat(df, axis=1).fillna(False).replace({False: '', True: 'X'})
df
```

```{code-cell} ipython3
:tags: [nbd-module]

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
```

```{code-cell} ipython3
:tags: []

#| label: fig-zcta-dane
#| fig-cap: "ZCTAs in Dane county, WI from 2013 and 2020 revisions."

dane_cty = get_county_df().query('CODE == "55025"').geometry.iloc[0]

y0, y1 = 2013, 2020

d = get_zcta_df(y0)
d = d[d.intersects(dane_cty)]
m = d.explore(name=y0, color='red', tiles='CartoDB positron')

d = get_zcta_df(y1)
d = d[d.intersects(dane_cty)]
d.explore(m=m, name=y1, color='blue')

tile_layer = [x for x in m._children.values() if isinstance(x, folium.raster_layers.TileLayer)][0]
tile_layer.control = False
folium.LayerControl(collapsed=False).add_to(m)

m
```

# Congressional Districts

A geographical and political division in which voters elect representatives to the U.S. House of Representatives. Each state establishes its congressional districts based on population counts, with the goal of having districts as equal in population as possible. ([ESRI dictionary](https://support.esri.com/en/other-resources/gis-dictionary/term/ae341e9d-cc60-4f96-93ee-c95c739df5df))

**About Congressional Districts** ([Census](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/congressional-dist.html))
- All congressional districts population are supposed to be equal throughout the state to equally be able to elect the representative
- They don’t cross state lines, but may cross all other classifications such as Census tracts.
- They DO cross county boundaries 
- [Map of CT](https://www2.census.gov/geo/maps/cong_dist/cd115/st_based/CD115_CT.pdf) for reference
- Closer breakdown of [District 1 in CT](https://www2.census.gov/geo/maps/cong_dist/cd115/cd_based/ST09/CD115_CT01.pdf)
- States are required to redraw the district lines every 10 years after the Census is released (except single district states)

In 33 states, state legislatures play the dominant role in congressional redistricting. In eight states, commissions draw congressional district lines. In two states, hybrid systems are used, in which the legislatures share redistricting authority with commissions. The remaining states comprise one congressional district each, rendering redistricting unnecessary (AK, DE, DC, MT, ND, SD, VT, WY). [Link](https://ballotpedia.org/State-by-state_redistricting_procedures)

Gerrymandering can and often does occur with congressional districts lines to help whomever the in power party is to make them stay in power. [Examples](https://www.theguardian.com/us-news/2020/dec/15/gerrymandering-republicans-map-charts-states)

+++

# School Districts

The U.S. has more than 13,000 geographically defined public school districts. These include districts that are administratively and fiscally independent of any other government, as well as public school systems that lack sufficient autonomy to be counted as separate governments and are classified as a dependent agency of some other government—a county, municipal, township, or state. Most public school systems are Unified districts that operate regular, special, and/or vocational programs for children in Prekindergarten through 12th grade.

- School districts are complex and have almost no consistency from state to state because they are formulated by the local town government in most public school cases.
- [Boundary files](https://nces.ed.gov/programs/edge/Geographic/DistrictBoundaries)
- Since they vary by local government then changes happen every year in many places throughout the US.

+++

# Native American Reservations

[About](https://www.census.gov/programs-surveys/geography/about/partnerships/aian.html) |
[Definitions](https://www.census.gov/programs-surveys/geography/about/glossary/aian-definitions.html) |
[Data](https://catalog.data.gov/dataset/tiger-line-shapefile-2018-nation-u-s-current-american-indian-alaska-native-native-hawaiian-area)

Diffrent breakdowns are avalible, going as small as tracts and block groups ([link](https://www.census.gov/newsroom/blogs/random-samplings/2014/08/understanding-geographic-relationships-american-indian-areas.html)).

+++

# Build this module

```{raw-cell}
:tags: []

nbd.nb2mod('geography.ipynb')
```
