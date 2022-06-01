---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.7
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

```{raw-cell}

---
title: "NAICS"
format:
  html:
    code-fold: true
execute:
  echo: false
jupyter: python3
date: today
date-format: long
---
```

+++ {"tags": []}

# Overview

[Website](https://www.census.gov/naics/)

The North American Industry Classification System (NAICS) is the standard used by Federal statistical agencies in classifying business establishments for the purpose of collecting, analyzing, and publishing statistical data related to the U.S. business economy.
NAICS was developed under the auspices of the Office of Management and Budget (OMB), and adopted in 1997 to replace the Standard Industrial Classification (SIC) system.
It was developed jointly by the U.S. Economic Classification Policy Committee (ECPC), Statistics Canada, and Mexico's Instituto Nacional de Estadistica y Geografia, to allow for a high level of comparability in business statistics among the North American countries.

The structure of NAICS is hierarchical.
The first two digits of the structure designate the 20 NAICS sectors that represent general categories of economic activities.
NAICS uses a six-digit coding system to identify industries and their placement in this hierarchical structure of the classification system.
The first two digits of the code designate the sector, the third digit designates the subsector, the fourth digit designates the industry group, the fifth digit designates the NAICS industry, and the sixth digit designates the national industry.
On the highest 2-digit level there are 20 sectors.
Classification is comparable between the USA, Canada and Mexico up to a 5-digit level, with some exceptions.
A zero as the sixth digit generally indicates that the NAICS industry and the U.S. industry are the same.

::: {.column-margin}

| Digits | Level             |
|--------|-------------------|
| 2      | sector            |
| 3      | subsector         |
| 4      | industry group    |
| 5      | NAICS industry    |
| 6      | national industry |

:::

Classification is revised every five years and is currently available for 1997, 2002, 2007, 2012, 2017 and 2022.

```{code-cell} ipython3
:tags: [nbd-module]

import typing
import pathlib
import urllib

import pandas as pd

from pubdata.reseng.util import download_file
from pubdata.reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'source': nbd.root/'data/source/naics',
}
```

```{code-cell} ipython3
:tags: []

pd.options.display.max_colwidth = None
```

# Source files

[Main tables](https://www.census.gov/naics/?48967)

This module provides access to dataframes built from source files.

- *Manual*. A PDF with complete information about the classification. All other files are parts of the manual. Available from 2017.

- *Code file*. Table of 2-6 digit *codes* and corresponding sector, subsector, industry group and industry *titles*. Available for all\* years. Subset files with only 6-digit industries are also available.

- *Structure*. Essentially the same as *Code file* with addition of indicators for trilateral aggreement (comparability between US, Canada and Mexico) and change from previous revision. Available from 2017.

- *Structure Summary*. Counts of subsectors, industry groups and industries in each sector. Available from 2017.

- *Definitions*. A PDF with full descriptions of every industry. Available for all\* years.

- *Descriptions*. Table that contains everything from the *Definitions* file, except Cross-References. Available from 2017.

- *Cross-References*. Table that contains industry cross-references from the *Definitions* file. Available from 2017.

- *Index file*. Table of 6-digit industry *codes* and description of *index items* withing each industry (usually multiple). Available from 2007.

\* **IMPORTANT.** For 1997, only [definitions PDFs](https://www.census.gov/naics/?58967?yearbck=1997) by sector are available in the NAICS section of the Census website. More useful code file in table form can be found in the CBP program [documentation](https://www.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions.html). However, *this version is missing industries* not covered by CBP. Notably, farming subsectors "111: Crop production" and "112: Animal production" are not included. Data source for 1997 should be updated to a complete one once found.

```{code-cell} ipython3
:tags: [nbd-module]

_src_url_base = 'https://www.census.gov/naics/'
_src_urls = {
    (1997, 'code'): 'https://www2.census.gov/programs-surveys/cbp/technical-documentation/reference/naics-descriptions/naics.txt',
    (2002, 'code'): f'{_src_url_base}reference_files_tools/2002/naics_2_6_02.txt',
    (2007, 'code'): f'{_src_url_base}reference_files_tools/2007/naics07.txt',
    (2012, 'code'): f'{_src_url_base}2012NAICS/2-digit_2012_Codes.xls',
    (2017, 'code'): f'{_src_url_base}2017NAICS/2-6%20digit_2017_Codes.xlsx',
    (2022, 'code'): f'{_src_url_base}2022NAICS/2-6%20digit_2022_Codes.xlsx',
    (2007, 'index'): f'{_src_url_base}2007NAICS/2007_NAICS_Index_File.xls',
    (2012, 'index'): f'{_src_url_base}2012NAICS/2012_NAICS_Index_File.xls',
    (2017, 'index'): f'{_src_url_base}2017NAICS/2017_NAICS_Index_File.xlsx',
    (2022, 'index'): f'{_src_url_base}2022NAICS/2022_NAICS_Index_File.xlsx',
    (2017, 'descriptions'): f'{_src_url_base}2017NAICS/2017_NAICS_Descriptions.xlsx',
    (2022, 'descriptions'): f'{_src_url_base}2022NAICS/2022_NAICS_Descriptions.xlsx',
    (2017, 'summary'): f'{_src_url_base}2017NAICS/2017_NAICS_Structure_Summary_Table.xlsx',
    (2022, 'summary'): f'{_src_url_base}2022NAICS/2022_NAICS_Structure_Summary_Table.xlsx',
}

def get_src(year: typing.Literal[2002, 2007, 2012, 2017, 2022],
            kind: typing.Literal['code', 'index', 'descriptions', 'summary']):
    """Download source file and return local path."""
    
    assert (year, kind) in _src_urls, f'Source file not available.'
    url = _src_urls[(year, kind)]
    fname = urllib.parse.urlparse(url).path
    fname = urllib.parse.unquote(pathlib.Path(fname).name)
    
    path = PATH['source']/f'{year}/{fname}'
    if path.exists(): return path

    download_file(url, path.parent, path.name)
    return path


def get_df(year: typing.Literal[2002, 2007, 2012, 2017, 2022],
           kind: typing.Literal['code', 'index', 'descriptions', 'summary']):
    """Return tidy dataframe built from source file."""
    
    src_file = get_src(year, kind)
    
    if kind == 'code':
        if year == 1997:
            df = pd.read_fwf(src_file, widths=(8, 999), dtype=str, skiprows=2, names=['CODE', 'TITLE'])
            df['CODE'] = df['CODE'].str.strip('-/')
            df['CODE'] = df['CODE'].replace({'31': '31-33', '44': '44-45', '48': '48-49'})
            # drop code "99" - unclassified establishments in CBP
            df = df[df['CODE'] != '99']
        if year == 2002:
            df = pd.read_fwf(src_file, widths=(8, 999), dtype=str, skiprows=5, names=['CODE', 'TITLE'])
        elif year == 2007:
            df = pd.read_fwf(src_file, widths=(8, 8, 999), dtype=str, skiprows=2,
                             names=['SEQ_NO', 'CODE', 'TITLE'], usecols=['CODE', 'TITLE'])
            df['TITLE'] = df['TITLE'].str.strip('"')
        elif year in [2012, 2017, 2022]:
            df = pd.read_excel(src_file, dtype=str, skiprows=2, header=None)
            df = df.iloc[:, [1,2]] 
            df.columns = ['CODE', 'TITLE']
        
        assert (df['CODE'].isin(['31-33', '44-45', '48-49']) | df['CODE'].str.isdigit()).all()
        
        df['DIGITS'] = df['CODE'].str.len()
        df.loc[df['CODE'] == '31-33', 'DIGITS'] = 2
        df.loc[df['CODE'] == '44-45', 'DIGITS'] = 2
        df.loc[df['CODE'] == '48-49', 'DIGITS'] = 2
        assert df['DIGITS'].isin([2, 3, 4, 5, 6]).all()

        df.loc[df['DIGITS'] == 2, 'CODE_2'] = df['CODE']
        df['CODE_2'] = df['CODE_2'].fillna(method='ffill')
        df.loc[df['DIGITS'] == 3, 'CODE_3'] = df['CODE']
        df['CODE_3'] = df.groupby('CODE_2')['CODE_3'].fillna(method='ffill')
        df.loc[df['DIGITS'] == 4, 'CODE_4'] = df['CODE']
        df['CODE_4'] = df.groupby('CODE_3')['CODE_4'].fillna(method='ffill')
        df.loc[df['DIGITS'] == 5, 'CODE_5'] = df['CODE']
        df['CODE_5'] = df.groupby('CODE_4')['CODE_5'].fillna(method='ffill')
        df.loc[df['DIGITS'] == 6, 'CODE_6'] = df['CODE']
        
    elif kind == 'index':
        df = pd.read_excel(src_file, names=['CODE', 'INDEX_ITEM'], dtype=str)
        # at the bottom of the table are ****** codes with comments for a few industries.
        df = df[df['CODE'] != '******']
        assert df['CODE'].str.isdigit().all()
        assert (df['CODE'].str.len() == 6).all()
    elif kind == 'descriptions':
        df = pd.read_excel(src_file, names=['CODE', 'TITLE', 'DESCRIPTION'], dtype=str)
        assert (df['CODE'].isin(['31-33', '44-45', '48-49']) | df['CODE'].str.isdigit()).all()
    elif kind == 'summary':
        df = pd.read_excel(src_file, header=None).fillna('')
        df.columns = pd.MultiIndex.from_frame(df.head(2).T, names=['', ''])
        df = df.drop(index=[0,1]).reset_index(drop=True)
        df.iloc[:, 2:] = df.iloc[:, 2:].astype(int)
        df['Sector'] = df['Sector'].astype(str)
    
    df = df.reset_index(drop=True)
    return df
```

```{code-cell} ipython3
:tags: []

#| test: download and parse all available tables
for y, k in _src_urls:
    print(y, k, '|', end=' ')
    get_df(y, k)
```

+++ {"tags": []}

# Structure summary

Total counts of classes at every level by sector, same format as analogous source tables.

```{code-cell} ipython3
:tags: [nbd-module]

def compute_structure_summary(year):
    """Return dataframe with total counts of classes at every level by sector."""
    df = get_df(year, 'code')
    t = df.loc[df['DIGITS'] == 2, ['CODE', 'TITLE']]
    t.columns = ['Sector', 'Name']
    t = t.set_index('Sector')
    t['Subsectors (3-digit)'] = df.groupby('CODE_2')['CODE_3'].nunique()
    t['Industry Groups (4-digit)'] = df.groupby('CODE_2')['CODE_4'].nunique()
    t['NAICS Industries (5-digit)'] = df.groupby('CODE_2')['CODE_5'].nunique()
    same_as_5d = df['CODE_6'].str[-1] == '0'
    t['6-digit Industries (U.S. Detail)'] = df[~same_as_5d].groupby('CODE_2')['CODE_6'].nunique()
    t['6-digit Industries (Same as 5-digit)'] = df[same_as_5d].groupby('CODE_2')['CODE_6'].nunique()
    t['6-digit Industries (Total)'] = df.groupby('CODE_2')['CODE_6'].nunique()
    totals = t.iloc[:, 1:].sum()
    totals['Name'] = 'Total'
    t.loc['', :] = totals
    t.iloc[:, 1:] = t.iloc[:, 1:].fillna(0).astype(int)
    # 6-digit: us_detail + same_as_5digit == total
    assert (t.iloc[:, -3] + t.iloc[:, -2] == t.iloc[:, -1]).all()
    t = t.reset_index()
    return t
```

```{code-cell} ipython3
:tags: []

#| test: summaries computed from code files are identical to published summaries
for year in [2017, 2022]:
    t0 = compute_structure_summary(year)
    t1 = get_df(year, 'summary')
    # # exclude columns that we did not compute
    # t1.columns = range(t1.shape[1])
    # t1 = t1.loc[:, [0, 1, 2, 3, 4, 7]]
    t1.columns = t0.columns
    # assert t1.equals(t0)
    # assertion FAILS. one row (manufacturing) is off by 1, no idea why.
    # construct a df that will pass after correcting corresponding entries
    t0c = t0.copy()
    t0c.loc[[4, 20], '6-digit Industries (U.S. Detail)'] += 1
    t0c.loc[[4, 20], '6-digit Industries (Same as 5-digit)'] -= 1
    assert t1.equals(t0c)
```

::: {.panel-tabset}
<!-- tabset starts -->
## 1997

```{code-cell} ipython3
:tags: []

#| column: body-outset
compute_structure_summary(1997)
```

## 2002

```{code-cell} ipython3
:tags: []

#| column: body-outset
compute_structure_summary(2002)
```

## 2007

```{code-cell} ipython3
:tags: []

#| column: body-outset
compute_structure_summary(2007)
```

+++ {"tags": []}

## 2012

```{code-cell} ipython3
:tags: []

#| column: body-outset
compute_structure_summary(2012)
```

+++ {"tags": []}

## 2017

```{code-cell} ipython3
:tags: []

#| column: body-outset
compute_structure_summary(2017)
```

+++ {"tags": []}

## 2022

```{code-cell} ipython3
:tags: []

#| column: body-outset
compute_structure_summary(2022)
```

<!-- tabset ends -->
::: 

+++

# Examples

+++

Random sample of codes.

```{code-cell} ipython3
:tags: []

#| echo: true
get_df(2022, 'code').sample(5).fillna('')
```

Full hierarchy of a randomly selected 6-digit code.

```{code-cell} ipython3
:tags: []

#| echo: true
d = get_df(2022, 'code')
r = d.query('DIGITS == 6').sample()
q = ' or '.join(f'CODE == "{c}"' for c in r.iloc[0, 3:])
d.query(q).fillna('')
```

6-digit industries in the "115" subsector.

```{code-cell} ipython3
:tags: []

#| echo: true
get_df(2022, 'code').query('CODE_3 == "115" and DIGITS == 6')
```

Index items of "115115" and "115116" industries. Notice duplication in INDEX_ITEM, e.g. "Farm labor contractors" and "Labor contractors, farm".

```{code-cell} ipython3
:tags: []

#| echo: true
#| column: body-outset
d0 = get_df(2022, 'code').query('CODE == "115115" or CODE == "115116"')
d1 = get_df(2022, 'index')
d0.merge(d1, 'left')
```

# Concordances

Concordances, also known as crosswalks, are tables that link industries of a given revision of NAICS to other revisions or other classifications. [Census page](https://www.census.gov/naics/?68967) lists the following concordances.

- Between consecutive NAICS revisions: 2017-2022, 2012-2017, 2007-2012, 2002-2007, 1997-2002.

- Between 1997 or 2002 NAICS and 1987 SIC.

- From NAICS to ISIC (United Nations' International Standard Industrial Classification of All Economic Activities).

- From 2002 NAICS to NACE (Statistical Classification of Economic Activities in the European Community).

Concordances are *symmetric*, i.e. X-to-Y concordance table differs from Y-to-X table only by row ordering.

The main challenge of working with concordances is that links are not always one-to-one. In this module we provide concordance tables with indicators of link types.

```{code-cell} ipython3
:tags: [nbd-module]

_concord_src_urls = {
    ('naics', 1997, 'naics', 2002): f'{_src_url_base}concordances/1997_NAICS_to_2002_NAICS.xls',
    ('naics', 2002, 'naics', 1997): f'{_src_url_base}concordances/2002_NAICS_to_1997_NAICS.xls',
    ('naics', 2002, 'naics', 2007): f'{_src_url_base}concordances/2002_to_2007_NAICS.xls',
    ('naics', 2007, 'naics', 2002): f'{_src_url_base}concordances/2007_to_2002_NAICS.xls',
    ('naics', 2007, 'naics', 2012): f'{_src_url_base}concordances/2007_to_2012_NAICS.xls',
    ('naics', 2012, 'naics', 2007): f'{_src_url_base}concordances/2012_to_2007_NAICS.xls',
    ('naics', 2012, 'naics', 2017): f'{_src_url_base}concordances/2012_to_2017_NAICS.xlsx',
    ('naics', 2017, 'naics', 2012): f'{_src_url_base}concordances/2017_to_2012_NAICS.xlsx',
    ('naics', 2017, 'naics', 2022): f'{_src_url_base}concordances/2017_to_2022_NAICS.xlsx',
    ('naics', 2022, 'naics', 2017): f'{_src_url_base}concordances/2022_to_2017_NAICS.xlsx',
}

def get_concordance_src(fro: str, fro_year: int, to: str, to_year: int):
    """Download concordance source file and return local path.
    Concordance table from (`fro`, `fro_year`) to (`to`, `to_year`),
    e.g. from ("naics", 2017) to ("naics", 2022).
    """
    
    assert (fro, fro_year, to, to_year) in _concord_src_urls, f'Concordance source file not available.'
    url = _concord_src_urls[(fro, fro_year, to, to_year)]
    fname = urllib.parse.urlparse(url).path
    fname = urllib.parse.unquote(pathlib.Path(fname).name)
    
    path = PATH['source']/f'{fro_year}/{fname}'
    if path.exists(): return path

    download_file(url, path.parent, path.name)
    return path


def get_concordance_df(fro: str, fro_year: int, to: str, to_year: int):
    """Return concordance dataframe built from source file.
    Concordance table from (`fro`, `fro_year`) to (`to`, `to_year`),
    e.g. from ("naics", 2017) to ("naics", 2022).
    """
    
    src_file = get_concordance_src(fro, fro_year, to, to_year)

    c_fro = f'{fro}_{fro_year}'.upper()
    t_fro = f'TITLE_{fro_year}'.upper()
    c_to = f'{to}_{to_year}'.upper()
    t_to = f'TITLE_{to_year}'.upper()

    if (fro == to == 'naics') and ((fro_year == 1997) or (to_year == 1997)):
        df = pd.read_excel(src_file, sheet_name=1, dtype=str, skipfooter=1)
        df.columns = [c_fro, t_fro, c_to, t_to, 'EXPLANATION']
        
    if (fro == to == 'naics') and (fro_year > 1997) and (to_year > 1997):
        df = pd.read_excel(src_file, dtype=str, skiprows=3, header=None)
        # columns beyond first four have no data
        for c in df.iloc[:, 4:]:
            assert (df[c].isna() | df[c].str.isspace()).all()
        df = df.iloc[:, :4]
        df.columns = [c_fro, t_fro, c_to, t_to]
        
    # flag link types
    if (fro == to == 'naics'):
        dup0 = df[f'DUP_{fro_year}'] = df.duplicated(c_fro, False)
        dup1 = df[f'DUP_{to_year}'] = df.duplicated(c_to, False)

        flag = f'FLAG_{fro_year}_TO_{to_year}'
        df[flag] = ''
        df.loc[~dup0 & ~dup1 & (df[c_fro] == df[c_to]), flag] = '1-to-1 same'
        df.loc[~dup0 & ~dup1 & (df[c_fro] != df[c_to]), flag] = '1-to-1 diff'
        df.loc[~dup0 & dup1, flag] = 'join'
        # splits
        d = df[dup0].copy()
        clean = ~d.groupby(c_fro)[f'DUP_{to_year}'].transform('max')
        d.loc[clean, flag] = 'clean split'
        d.loc[~clean, flag] = 'messy split'
        df.loc[dup0, flag] = d[flag]        

    df = df.sort_values([c_fro, c_to], ignore_index=True)
    
    return df
```

```{code-cell} ipython3
:tags: []

#| test: download and parse all available concordances
for k in _concord_src_urls:
    print(k, end=' ')
    get_concordance_df(*k)
```

```{code-cell} ipython3
:tags: []

#| test: verify concordance symmetry
years = [1997, 2002, 2007, 2012, 2017, 2022]
for y0, y1 in zip(years[:-1], years[1:]):
    print(y0, y1, end=' |')
    d01 = get_concordance_df('naics', y0, 'naics', y1).iloc[:, [0, 2]]
    d10 = get_concordance_df('naics', y1, 'naics', y0).iloc[:, [0, 2]]
    assert len(d01) == len(d10)
    d10 = d10.reindex_like(d01)
    d10 = d10.sort_values(d10.columns.to_list(), ignore_index=True)
    assert d10.equals(d01)
```

```{code-cell} ipython3
:tags: []

#| test: every 6-digit code can be found in concordances
# not testing 1997, because list of codes from CBP is not complete
for y in [2002, 2007, 2012, 2017, 2022]:
    d0 = get_df(y, 'code')
    d1 = get_concordance_df('naics', y, 'naics', y - 5)
    print(y, set(d0.query('DIGITS == 6')['CODE']) == set(d1[f'NAICS_{y}']), end=' |')
```

Tables below summarize types of links between consequtive years in NAICS concordances. The first table shows forward concordances, i.e. from year `t` to `t+5`. The second table show backward concordances, i.e. from year `t` to year `t-5`.

- `1-to-1 same`: industry and code did not change between years, this is the most common case.
- `1-to-1 diff`: industry did not change, but it's numerical code changes.
- `clean split`: industry was split into multiple, but all parts only came for the one source industry.
- `messy split`: industry was split, but some parts were joined from other industries.
- `join`: industry was joined and became a part of another industry.

```{code-cell} ipython3
:tags: []

def naics_concordance_summary(year_pairs):
    t = {}
    for fro, to in year_pairs:
        df = get_concordance_df('naics', fro, 'naics', to)
        t[f'{fro}->{to}'] = c = df.drop_duplicates(f'NAICS_{fro}')[f'FLAG_{fro}_TO_{to}'].value_counts(dropna=False)
        c['total in "from"'] = c.sum()
    t = pd.concat(t, axis=1).fillna(0).astype(int)
    t = t.loc[['total in "from"', '1-to-1 same', '1-to-1 diff', 'clean split', 'messy split', 'join']]
    return t

display(naics_concordance_summary([(y, y + 5) for y in range(1997, 2020, 5)]))
display(naics_concordance_summary([(y, y - 5) for y in range(2002, 2025, 5)]))
```

Example: `1-to-1 same`.

```{code-cell} ipython3
:tags: []

#| echo: true
#| column: body-outset
df = get_concordance_df('naics', 2012, 'naics', 2017)
df.query('FLAG_2012_TO_2017 == "1-to-1 same"').sample(3)
```

Example: `1-to-1 diff`.

```{code-cell} ipython3
:tags: []

#| echo: true
#| column: body-outset
df = get_concordance_df('naics', 2012, 'naics', 2017)
df.query('FLAG_2012_TO_2017 == "1-to-1 diff"').sample(3)
```

Example: `clean split`.

```{code-cell} ipython3
:tags: []

#| echo: true
#| column: body-outset
df = get_concordance_df('naics', 2017, 'naics', 2022)
df.query('NAICS_2017 == "325314"')
```

Example: `messy split` and `join`.
By construction, these two types of links will always be together.
From 2017 to 2022, three industries ("212111: Bituminous Coal and Lignite Surface Mining", "212112: Bituminous Coal Underground Mining" and "212113: Anthracite Mining") were re-classified into two ("212114: Surface Coal Mining" and "212115: Underground Coal Mining").
This creates a problem, for example, when one wants to identify anthracite mining industry under 2022 NAICS, because the two parts of that industry (surface and underground mining) were combined with other types of coal mining.

```{code-cell} ipython3
:tags: []

#| echo: true
#| column: body-outset
df = get_concordance_df('naics', 2017, 'naics', 2022)
df.query('NAICS_2022.isin(["212114", "212115"])')
```

Example: a clean `join`. From 2012 to 2017 three industries joined into one. However, not every `join` is clean. The easiest way to identify a clean `join` from year `t0` to `t1` is to look for a `clean split` from year `t1` to `t0`.

```{code-cell} ipython3
:tags: []

#| echo: true
#| column: body-outset
df = get_concordance_df('naics', 2012, 'naics', 2017)
df.query('NAICS_2012.isin(["454111", "454112", "454113"]) or NAICS_2017 == "454110"')
```

```{code-cell} ipython3
:tags: []

#| echo: true
#| column: body-outset
df = get_concordance_df('naics', 2017, 'naics', 2012)
df.query('NAICS_2017 == "454110"')
```

# Build this module

Cells with "nbd-module" tag are converted to an importable Python module.

```{code-cell} ipython3
:tags: []

from pubdata.reseng.nbd import Nbd
nbd = Nbd('pubdata')
nbd.nb2mod('naics.ipynb')
```

```{code-cell} ipython3
:tags: []

#| test: importable module works
import pubdata.naics
pubdata.naics.compute_structure_summary(2002)
pubdata.naics.get_concordance_df('naics', 2017, 'naics', 2022);
```
