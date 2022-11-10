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
:tags: []

---
title: "Input-Output Accounts"
format:
  html: 
    code-fold: true
    ipynb-filters:
      - pubdata/reseng/nbd.py filter-docs
---
```

+++ {"tags": ["nbd-docs"]}

A series of detailed tables showing how industries interact with each other and with the rest of the economy.
**Supply tables** show the goods and services produced by domestic industries as well as imports of these goods and services.
**Use tables** show who uses these goods and services, including other industries.
**Requirements tables** summarize the full supply chain, including direct and indirect inputs.

[Data page](https://www.bea.gov/data/industries/input-output-accounts-data)

```{code-cell} ipython3
:tags: [nbd-module]

import zipfile
import typing
import shutil
from contextlib import redirect_stdout

import pandas as pd

from pubdata.reseng.util import download_file
from pubdata.reseng.monitor import log_start_finish
from pubdata.reseng.nbd import Nbd
from pubdata import naics, cbp

nbd = Nbd('pubdata')

NAICS_REV = 2012
```

```{code-cell} ipython3
:tags: []

pd.options.display.max_rows = 40
pd.options.display.max_columns = 40
pd.options.display.max_colwidth = None
```

```{code-cell} ipython3
:tags: [nbd-module]

PATH = {
    'source': nbd.root / 'data/source/bea_io/',
    'proc': nbd.root / 'data/bea_io/',
    'naics_codes': nbd.root / 'data/bea_io/naics_codes.csv'
}

def init_dirs():
    """Create necessary directories."""
    PATH['source'].mkdir(parents=True, exist_ok=True)
    PATH['proc'].mkdir(parents=True, exist_ok=True)
    
def cleanup(remove_downloaded=False):
    if remove_downloaded:
        print('Removing downloaded files...')
        shutil.rmtree(PATH['source'])
    print('Removing processed files...')
    shutil.rmtree(PATH['proc'])
```

+++ {"tags": ["nbd-docs"]}

# Source files

[Bulk download](https://apps.bea.gov/iTable/?isuri=1&reqid=151&step=1) of data underlying BEA's [Interactive Tables](https://www.bea.gov/itable/).
Single archive `AllTablesSUP.zip` contains all tables in Excel format.
This section downloads and unpacks the source tables.

```{code-cell} ipython3
:tags: [nbd-module]

def get_source_files():
    init_dirs()
    src_dir = PATH['source'] / 'AllTablesSUP'
    if src_dir.exists(): return

    print('Downloading source files...')
    url = 'https://apps.bea.gov/industry/iTables Static Files/AllTablesSUP.zip'
    f = download_file(url, PATH['source'])
    with zipfile.ZipFile(f) as z:
        z.extractall(src_dir)
    print('Source files downloaded and extracted.')
```

```{code-cell} ipython3
:tags: []

# test: data download
cleanup(True)
get_source_files()
```

All tables share similar layout, and `_read_table()` function is used to read a table from a spreadsheet.

```{code-cell} ipython3
:tags: [nbd-module]

def _read_table(file, sheet, level, labels, skip_head, skip_foot):
    get_source_files()
    
    src_file = PATH['source'] / 'AllTablesSUP' / file
    
    df = pd.read_excel(src_file, sheet, dtype=str, header=None, 
                       skiprows=skip_head, skipfooter=skip_foot)
    
    # swap code and label rows for consistency with sec and sum
    if level == 'det':
        df.iloc[[0, 1], :] = df.iloc[[1, 0], :].values
    if labels:
        rows = df.iloc[2:, 1]
        cols = df.iloc[1, 2:]
    else:
        rows = df.iloc[2:, 0]
        cols = df.iloc[0, 2:]

    df = pd.DataFrame(df.iloc[2:, 2:].values, index=rows, columns=cols)
    df = df.replace('...', None).astype('float64')

    assert not df.index.duplicated().any()
    assert not df.columns.duplicated().any()

    return df
```

+++ {"tags": ["nbd-docs"]}

# Supply tables

The supply and make tables present the commodities that are produced by each industry.
The **supply table** extends the framework, showing supply from domestic and foreign producers that are available for use in the domestic economy in both basic and purchasers’ prices.

Function `get_sup()` provides dataframes read from "The Supply Tables":

- `Supply_Tables_1997-2021_SEC.xlsx`: sector, 1997-2021
- `Supply_Tables_1997-2021_SUM.xlsx`: summary, 1997-2021
- `Supply_2007_2012_DET.xlsx`: detail, 2007 and 2021.

```{code-cell} ipython3
:tags: [nbd-module]

def get_sup(year: int,
            level: typing.Literal['sec', 'sum', 'det'],
            labels: bool = False):
    """Return dataframe from "Supply_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()
    
    if level == 'sec':
        df = _read_table('Supply_Tables_1997-2021_SEC.xlsx', str(year), level, labels, 5, 0)
    elif level == 'sum':
        df = _read_table('Supply_Tables_1997-2021_SUM.xlsx', str(year), level, labels, 5, 0)
    elif level == 'det':
        df = _read_table('Supply_2007_2012_DET.xlsx', str(year), level, labels, 4, 2)

    df.index.name = 'commodity'
    df.columns.name = 'industry'
    
    return df

@log_start_finish
def test_get_sup(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_sup(year, level, labels)
                assert len(d) > 0
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_get_sup()
```

```{raw-cell}
:tags: [nbd-docs]

::: {.callout-note appearance=minimal collapse=true}

## 2021 Supply table, sector level
```

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: screen-inset
get_sup(2021, 'sec', True).apply(lambda c: c.apply(lambda x: '{:,.0f}'.format(x) if pd.notna(x) else ''))
```

```{raw-cell}
:tags: [nbd-docs]

:::
```

+++ {"tags": ["nbd-docs"]}

# Use tables

The supply and make tables present the commodities that are produced by each industry.
The supply table extends the framework, showing supply from domestic and foreign producers that are available for use in the domestic economy in both basic and purchasers’ prices.
The **use table** shows the use of this supply by domestic industries as intermediate inputs and by final users as well as value added by industry. 

Function `get_use()` provides dataframes read from "The Use Table (Supply-Use Framework)" tables:

- `Use_SUT_Framework_1997-2021_SECT.xlsx`: sector, 1997-2021
- `Use_SUT_Framework_1997-2021_SUM.xlsx`: summary, 1997-2021
- `Use_SUT_Framework_2007_2012_DET.xlsx`: detail, 2007 and 2021.

```{code-cell} ipython3
:tags: [nbd-module]

def get_use(year: int,
                level: typing.Literal['sec', 'sum', 'det'],
                labels: bool = False):
    """Return dataframe from "Use_SUT_Framework_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()
    
    if level == 'sec':
        df = _read_table('Use_SUT_Framework_1997-2021_SECT.xlsx', str(year), level, labels, 5, 0)
    elif level == 'sum':
        df = _read_table('Use_SUT_Framework_1997-2021_SUM.xlsx', str(year), level, labels, 5, 0)
    elif level == 'det':
        df = _read_table('Use_SUT_Framework_2007_2012_DET.xlsx', str(year), level, labels, 4, 2)

    df.index.name = 'commodity'
    df.columns.name = 'industry'
    
    return df


@log_start_finish
def test_get_use(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_use(year, level, labels)
                assert len(d) > 0
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_get_use()
```

```{raw-cell}
:tags: [nbd-docs]

::: {.callout-note appearance=minimal collapse=true}

## 2021 Use table, sector level
```

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: screen-inset
get_use(2021, 'sec', True).apply(lambda c: c.apply(lambda x: '{:,.0f}'.format(x) if pd.notna(x) else ''))
```

```{raw-cell}
:tags: [nbd-docs]

:::
```

+++ {"tags": ["nbd-docs"]}

Example: 2012 dollar value of top 10 detail level commodities used is inputs to Grain farming.

```{code-cell} ipython3
:tags: [nbd-docs]

get_use(2012, 'det', True)['Grain farming']\
    .head(405).sort_values(ascending=False).head(10)\
    .astype(int).to_frame()
```

+++ {"tags": ["nbd-docs"]}

Use table subtotals satisfy the following accounting identity:

$$
\text{Total industry output} = \text{Total Intermediate} + \text{Compensation of employees} + \text{Gross operating surplus} + \text{Net taxes}
$$

Table below shows percentage shares of total output by industry in 2021 at the sector level.

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: body-outset
d = get_use(2021, 'sec', True)

d = d.loc[['Total Intermediate', 'Compensation of employees',
       'Other taxes on production', 'Less: Other subsidies on production',
       'Gross operating surplus', 'Total industry output (basic prices)'], :]\
    .iloc[:, :15].fillna(0).T
d.columns.name = None
d['Less: Other subsidies on production'] *= -1
d = d.apply(lambda c: c / d['Total industry output (basic prices)']) * 100
d = d.round(1)
d
```

+++ {"tags": ["nbd-docs"]}

Labor share by industry over time, computed as $\frac{Compensation\ of\ employees}{Total\ industry\ output\ (basic\ prices)}$.

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: body-outset
t = {}
for y in range(1997, 2022):
    d = get_use(y, 'sec', True)
    t[y] = d.loc['Compensation of employees', :][:15] / d.loc['Total industry output (basic prices)', :][:15]
t = pd.concat(t, axis=1).T * 100

ax = t.plot(figsize=(16, 6))
ax.legend(title=None, ncol=3, loc='upper center', bbox_to_anchor=(0.5, -0.05));
```

+++ {"tags": ["nbd-docs"]}

# Total requirements tables

The four requirements tables are derived from the use and make tables.
The direct requirements table shows the amount of a commodity that is required by an industry to produce a dollar of the industry's output.
The three **Total Requirements** tables show the production that is required, directly and indirectly, from each industry and each commodity to deliver a dollar of a commodity to final users.

## Industry-by-Industry 

Function `get_ixi()` provides dataframes read from "Industry-by-Industry Total Requirements, After Redefinitions" tables:

- `IxI_TR_1997-2021_PRO_SEC.xlsx`: sector, 1997-2021
- `IxI_TR_1997-2021_PRO_SUM.xlsx`: summary, 1997-2021
- `IxI_TR_2007_2012_PRO_DET.xlsx`: detail, 2007 and 2021.

```{code-cell} ipython3
:tags: [nbd-module]

def get_ixi(year: typing.Literal[2007, 2012],
            level: typing.Literal['sec', 'sum', 'det'],
            labels: bool = False):
    """Return dataframe from "IxI_TR_*_PRO_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()

    if level == 'sec':
        df = _read_table('IxI_TR_1997-2021_PRO_SEC.xlsx', str(year), level, labels, 5, 2)
    elif level == 'sum':
        df = _read_table('IxI_TR_1997-2021_PRO_SUM.xlsx', str(year), level, labels, 5, 2)
    elif level == 'det':
        df = _read_table('IxI_TR_2007_2012_PRO_DET.xlsx', str(year), level, labels, 3, 0)

    df.index.name = 'industry'
    df.columns.name = 'industry'
    
    return df

@log_start_finish
def test_get_ixi(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_ixi(year, level, labels)
                assert len(d) > 0
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_get_ixi()
```

```{raw-cell}
:tags: [nbd-docs]

::: {.callout-note appearance=minimal collapse=true}

## 2021 Industry-by-Industry Total Requirements table, sector level
```

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: screen-inset
get_ixi(2021, 'sec', True).round(3)
```

```{raw-cell}
:tags: [nbd-docs]

:::
```

+++ {"tags": ["nbd-docs"]}

## Industry-by-Commodity

Function `get_ixc()` provides dataframes read from "Industry-by-Commodity Total Requirements, After Redefinitions" tables:

- `IxC_TR_1997-2021_PRO_SEC.xlsx`: sector, 1997-2021
- `IxC_TR_1997-2021_PRO_SUM.xlsx`: summary, 1997-2021
- `IxC_TR_2007_2012_PRO_DET.xlsx`: detail, 2007 and 2021.

```{code-cell} ipython3
:tags: [nbd-module]

def get_ixc(year: typing.Literal[2007, 2012],
            level: typing.Literal['sec', 'sum', 'det'],
            labels: bool = False):
    """Return dataframe from "IxC_TR_*_PRO_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()

    if level == 'sec':
        df = _read_table('IxC_TR_1997-2021_PRO_SEC.xlsx', str(year), level, labels, 5, 2)
    elif level == 'sum':
        df = _read_table('IxC_TR_1997-2021_PRO_SUM.xlsx', str(year), level, labels, 5, 2)
    elif level == 'det':
        df = _read_table('IxC_TR_2007_2012_PRO_DET.xlsx', str(year), level, labels, 3, 0)

    df.index.name = 'industry'
    df.columns.name = 'commodity'
    
    return df    


@log_start_finish
def test_get_ixc(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_ixc(year, level, labels)
                assert len(d) > 0
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_get_ixc()
```

```{raw-cell}
:tags: [nbd-docs]

::: {.callout-note appearance=minimal collapse=true}

## 2021 Industry-by-Commodity Total Requirements table, sector level
```

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: screen-inset
get_ixc(2021, 'sec', True).round(3)
```

```{raw-cell}
:tags: [nbd-docs]

:::
```

+++ {"tags": ["nbd-docs"]}

## Commodity-by-Commodity

Function `get_cxc()` provides dataframes read from "Commodity-by-Commodity Total Requirements, After Redefinitions" tables:

- `CxC_TR_1997-2021_PRO_SEC.xlsx`: sector, 1997-2021
- `CxC_TR_1997-2021_PRO_SUM.xlsx`: summary, 1997-2021
- `CxC_TR_2007_2012_PRO_DET.xlsx`: detail, 2007 and 2021.

```{code-cell} ipython3
:tags: [nbd-module]

def get_cxc(year: typing.Literal[2007, 2012],
            level: typing.Literal['sec', 'sum', 'det'],
            labels: bool = False):
    """Return dataframe from "CxC_TR_*_PRO_*.xlsx" files.
    `level` to choose industry classification from "sector", "summary" or "detail".
    `labels=True` will set long labels as axes values, otherwise short codes.
    """
    
    get_source_files()
    
    if level == 'sec':
        df = _read_table('CxC_TR_1997-2021_PRO_SEC.xlsx', str(year), level, labels, 5, 2)
    elif level == 'sum':
        df = _read_table('CxC_TR_1997-2021_PRO_SUM.xlsx', str(year), level, labels, 5, 2)
    elif level == 'det':
        df = _read_table('CxC_TR_2007_2012_PRO_DET.xlsx', str(year), level, labels, 3, 0)

    df.index.name = 'commodity'
    df.columns.name = 'commodity'
    
    return df

@log_start_finish
def test_get_cxc(redownload=False):
    cleanup(redownload)
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                print(year, level, labels)
                d = get_cxc(year, level, labels)
                assert len(d) > 0
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_get_cxc()
```

```{raw-cell}
:tags: [nbd-docs]

::: {.callout-note appearance=minimal collapse=true}

## 2021 Commodity-by-Commodity Total Requirements table, sector level
```

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: screen-inset
get_cxc(2021, 'sec', True).round(3)
```

```{raw-cell}
:tags: [nbd-docs]

:::
```

+++ {"tags": ["nbd-docs"]}

# IO-NAICS concordance

BEA uses industry classification that is different from NAICS.
Crosswalk is provided in every detail level spreadsheet.
"NAICS Codes" sheet is parsed so that at the lowest classification level ("detail") each row corresponds to a single NAICS code.
Detail industries with multiple NAICS are split into multiple rows.
Levels about "detail" have their separate rows.
This resulting table is returned by `get_naics_df()` function.

```{code-cell} ipython3
:tags: [nbd-module]

def get_naics_df():
    path = PATH['naics_codes']
    if path.exists():
        return pd.read_csv(path, dtype=str)
    
    get_source_files()
    df = pd.read_excel(PATH['source']/'AllTablesSUP/Use_SUT_Framework_2007_2012_DET.xlsx',
                       sheet_name='NAICS Codes',
                       skiprows=4,
                       skipfooter=6,
                       dtype=str)

    df.columns = ['sector', 'summary', 'u_summary', 'detail', 'description', 'notes', 'naics']
    df = df.drop(columns='notes')
    df = df.dropna(how='all')

    # move descriptions to single column
    df['description'].fillna(df['detail'], inplace=True)
    df['description'].fillna(df['u_summary'], inplace=True)
    df['description'].fillna(df['summary'], inplace=True)

    df.loc[df['sector'].notna(), 'summary'] = None
    df.loc[df['summary'].notna(), 'u_summary'] = None
    df.loc[df['u_summary'].notna(), 'detail'] = None

    assert (df[['sector', 'summary', 'u_summary', 'detail']].notna().sum(1) == 1).all(),\
        'Code in more than one column'
    assert df['description'].notna().all()

    # pad higher level codes
    df['sector'] = df['sector'].fillna(method='ffill')
    df['summary'] = df.groupby('sector')['summary'].fillna(method='ffill')
    df['u_summary'] = df.groupby(['sector', 'summary'])['u_summary'].fillna(method='ffill')

    df['naics'] = df['naics'].str.strip().apply(_split_codes)
    df = df.explode('naics', ignore_index=True)
    
    # drop non-existent NAICS codes, created from expanding ranges like "5174-9"
    feasible_naics_codes = ['23*', 'n.a.'] + naics.get_df(NAICS_REV, 'code')['CODE'].to_list()
    df = df[df['naics'].isna() | df['naics'].isin(feasible_naics_codes)]
    
    df[df.isna()] = None
    df = df.reset_index(drop=True)
    df = df.rename(columns=str.upper)
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return df

def _split_codes(codes):
    if pd.isna(codes):
        return [codes]
    def expand_dash(codes):
        if '-' in codes:
            first, last = codes.split('-')
            assert len(last) == 1
            last = int(first[:-1] + last)
            first = int(first)
            return [str(c) for c in range(first, last+1)]
        else:
            return [codes]

    codes = codes.split(', ')
    codes = sum((expand_dash(c) for c in codes), [])
    return codes

def test_get_naics_df(redownload=False):
    cleanup(redownload)
    
    assert _split_codes('1') == ['1']
    assert _split_codes('1, 2') == ['1', '2']
    assert _split_codes('1-3') == ['1', '2', '3']
    assert _split_codes('1-3, 5') == ['1', '2', '3', '5']
    assert _split_codes('1-3, 5-7') == ['1', '2', '3', '5', '6', '7']
    
    d = get_naics_df()
    assert len(d) > 0
```

```{code-cell} ipython3
:tags: []

test_get_naics_df()
```

+++ {"tags": ["nbd-docs"]}

Example: all sector level industries.

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: body-outset
get_naics_df().query('SUMMARY.isna()')
```

+++ {"tags": ["nbd-docs"]}

Example: Information sector.

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: body-outset
d = naics.get_df(NAICS_REV, 'code').rename(columns={'CODE': 'NAICS', 'TITLE': 'NAICS_TITLE'})[['NAICS', 'NAICS_TITLE']]
get_naics_df().query('SECTOR == "51"').merge(d, 'left').fillna('')
```

+++ {"tags": ["nbd-docs"]}

## Concordance properties

No overlaps between branches of the SECTOR -> SUMMARY -> U_SUMMARY -> DETAIL hierarchy.
  - No code in one branch exists in another branch.
  - From a code in one level, can unambiguously go to upper levels.

```{code-cell} ipython3
:tags: []

df = get_naics_df()\
    .query('SUMMARY.notna()')\
    .drop_duplicates(subset=['SECTOR', 'SUMMARY'])
assert not df['SUMMARY'].duplicated().any()

df = get_naics_df()\
    .query('U_SUMMARY.notna()')\
    .drop_duplicates(subset=['SECTOR', 'SUMMARY', 'U_SUMMARY'])
assert not df['U_SUMMARY'].duplicated().any()

df = get_naics_df()\
    .query('DETAIL.notna()')\
    .drop_duplicates(subset=['SECTOR', 'SUMMARY', 'U_SUMMARY', 'DETAIL'])
assert not df['DETAIL'].duplicated().any()
```

+++ {"tags": ["nbd-docs"]}

Construction (IO sector "23", NAICS sector "23") matches exactly on sector level, i.e. no part of IO "23" maps to NAICS outside of "23", and no part of NAICS "23" maps to IO outside of "23". But match below sector level is impossible. Explanation from IO footnote:

> Construction data published by BEA at the detail level do not align with 2012 NAICS industries.  In NAICS, industries are classified based on their production processes, whereas BEA construction is classified by type of structure.  For example, activity by the 2012 NAICS Roofing contractors industry would be split among many BEA construction categories because roofs are built on many types of structures.

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
df = get_naics_df()
assert list(df.loc[df['SECTOR'] == '23', 'NAICS'].dropna().unique()) == ['23*']
assert list(df.loc[df['NAICS'].str[:2] == '23', 'SECTOR'].unique()) == ['23']
df.query('SECTOR == "23"')
```

+++ {"tags": ["nbd-docs"]}

16 IO detail industries do not map into any NAICS.
Most of them are in sector "G" (Goverment).

```{code-cell} ipython3
:tags: []

get_naics_df().query('NAICS == "n.a."')
```

+++ {"tags": ["nbd-docs"]}

An ambiguity exists in IO sector "53" (REAL ESTATE AND RENTAL AND LEASING). Two detail level industries ("531HST" and "531ORE") map into the same 3-digit NAICS "531" (Real Estate). There are no other NAICS duplicates.

```{code-cell} ipython3
:tags: []

df = get_naics_df()
df = df.query('NAICS.notna() and NAICS != "n.a." and NAICS != "23*"')
assert set(df.loc[df['NAICS'].duplicated(), 'NAICS']) == {'531'}

get_naics_df().query('SECTOR == "53"')
```

+++ {"jp-MarkdownHeadingCollapsed": true, "tags": ["nbd-docs"]}

With the exception of NAICS "23", "n.a." and "531" *relationship between IO DETAIL and NAICS is one-to-many*.

```{code-cell} ipython3
:tags: []

d = get_naics_df()\
    [['DETAIL', 'NAICS']]\
    .query('DETAIL.notna() and NAICS != "n.a." and NAICS != "23*" and NAICS != "531"')
assert not d['NAICS'].duplicated().any()
```

+++ {"tags": ["nbd-docs"]}

## NAICS coverage

Which NAICS industries are covered by the IO industries?

In sum, every industry in the NAICS is covered and no industry is covered more than once with these caveats:
- Sector "23" (construction) only maps at the sector level.
- Government (sector "G" in IO and "92" in NAICS) likely maps at the sector level.
- NAICS subsector "531" (real estate) is covered twice.

+++ {"tags": ["nbd-docs"]}

Every NAICS code present in IO concordance is a valid code that exists in NAICS table.

```{code-cell} ipython3
:tags: []

dfna = naics.get_df(2012, 'code')
dfio = get_naics_df()
assert dfio.query('NAICS.notna() and NAICS != "n.a." and NAICS != "23*"')['NAICS'].isin(dfna['CODE']).all()
```

```{code-cell} ipython3
:tags: []

dfio = get_naics_df()\
    [['DETAIL', 'NAICS']]\
    .query('NAICS.notna() and NAICS != "n.a." and NAICS != "23*"')\
    .drop_duplicates(subset=['NAICS']) # NAICS 531 dupe
```

```{code-cell} ipython3
:tags: []

df = naics.get_df(2012, 'code')
for digit in [2, 3, 4, 5, 6]:
    ind = f'MERGE_{digit}'
    df = df.merge(dfio, 'left', left_on=f'CODE_{digit}', right_on='NAICS', indicator=ind)\
        .drop(columns='NAICS')\
        .rename(columns={'DETAIL': f'DETAIL_{digit}'})
    df[ind] = df[ind].map({'both': 1, 'left_only': 0})
df.loc[df['CODE_2'] == '23', 'MERGE_2'] = 1
```

+++ {"tags": ["nbd-docs"]}

No NAICS code matches on more than one level.
I.e. there is no situation when some lower NAICS level is covered more than once because it's parent is include someplace else.

```{code-cell} ipython3
:tags: []

df['MERGE_SUM'] = df[[f'MERGE_{d}' for d in [2,3,4,5,6]]].sum(1)
assert df['MERGE_SUM'].isin([0, 1]).all()
```

+++ {"tags": ["nbd-docs"]}

Every industry outside of sector "92" is covered.
Nothing is covered in sector "92".

```{code-cell} ipython3
:tags: []

assert (df.query('CODE_2 != "92" and DIGITS == 6')['MERGE_SUM'] == 1).all()
assert (df.query('CODE_2 == "92" and DIGITS == 6')['MERGE_SUM'] == 0).all()
```

+++ {"tags": ["nbd-docs"]}

NAICS sector "92" (Public Administration) is similar to IO sector "G" (GOVERNMENT).
This looks similar to the construction sector "23": although sectors are alike, lower level division is done on a different principle.
Although unlike "23", the crosswalk is not explicitly stating that NAICS "92" can be mapped to IO "G" on a sector level.
Maybe because of the "Postal service" industry (IO detail "491000", part of sector "G") that corresponds to NAICS subsector "491".

```{code-cell} ipython3
:tags: []

get_naics_df().query('SECTOR == "G" and SUMMARY.notna() and U_SUMMARY.isna()')[['SECTOR', 'SUMMARY', 'DESCRIPTION']]
```

```{code-cell} ipython3
:tags: []

df.query('CODE_2 == "92" and DIGITS <= 3')[['CODE', 'TITLE']]
```

+++ {"tags": ["nbd-docs"]}

## 3-digit crosswalk

BEA "summary" can be compared to NAICS "subsector". BEA->NAICS crosswalk is often one-to-many, which is not a problem when we convert NAICS-based data to match BEA. But BEA->NAICS is also many-to-one for three NAICS subsectors (336, 531, 541).

```{code-cell} ipython3
:tags: []

df = get_naics_df()
df = df[['SUMMARY', 'DETAIL', 'NAICS']].dropna().query('NAICS != "n.a."')
df['NAICS3'] = df['NAICS'].str[:3]
df = df[['SUMMARY', 'NAICS3']].drop_duplicates()
df['DUP_IO'] = df['SUMMARY'].duplicated(False)
df['DUP_N3'] = df['NAICS3'].duplicated(False)
df[df['DUP_N3']]
```

+++ {"tags": []}

# Example: merge CBP to IO

Because we know that IO-NAICS concordance fully covers NAICS with no double-counting, we can simply merge it to the CBP.
Only need to take care of "23", "G" and "531".

```{code-cell} ipython3
:tags: []

df = get_naics_df()
# ignore construction and government sectors for simplicity
df = df.query('SECTOR != "23" and SECTOR != "G"')

d = cbp.get_df('us', 2014)\
    .query('lfo == "-"')\
    [['industry', 'est', 'emp', 'ap']]\
    .rename(columns={'industry': 'NAICS'})

df = df.merge(d, 'left', 'NAICS', indicator=True)
# split "531" between "531HST" and "531ORE" with equal weights
df.loc[df['DETAIL'] == '531HST', ['est', 'emp', 'ap']] *= 0.5
df.loc[df['DETAIL'] == '531ORE', ['est', 'emp', 'ap']] *= 0.5
```

```{code-cell} ipython3
:tags: []

df = get_naics_df()
# ignore construction and government sectors for simplicity
df = df.query('SECTOR != "23" and SECTOR != "G"')

d = cbp.get_df('county', 2014)\
    .query('fipstate == "01" and fipscty == "003"')\
    [['industry', 'est', 'emp', 'ap']]\
    .rename(columns={'industry': 'NAICS'})

df = df.merge(d, 'left', 'NAICS', indicator=True)
# split "531" between "531HST" and "531ORE" with equal weights
df.loc[df['DETAIL'] == '531HST', ['est', 'emp', 'ap']] *= 0.5
df.loc[df['DETAIL'] == '531ORE', ['est', 'emp', 'ap']] *= 0.5
```

```{code-cell} ipython3
:tags: []

df.head(10)
```

```{code-cell} ipython3
:tags: []

d = df.loc[df['DETAIL'].notna(), ['DETAIL', 'ap']]
d = d.groupby('DETAIL').sum()
d_us = d
```

```{code-cell} ipython3
:tags: []

d = df.loc[df['SUMMARY'].notna(), ['SUMMARY', 'ap']]
d = d.groupby('SUMMARY').sum()
d
```

```{code-cell} ipython3
:tags: []

d = df.loc[df['DETAIL'].notna(), ['DETAIL', 'ap']]
d = d.groupby('DETAIL').sum()
d_01001 = d
```

```{code-cell} ipython3
:tags: []

d = df.loc[df['DETAIL'].notna(), ['DETAIL', 'ap']]
d = d.groupby('DETAIL').sum()
d_01003 = d
```

```{code-cell} ipython3
:tags: []

pd.concat([d_us, d_01001, d_01003], axis=1)
```

```{code-cell} ipython3
:tags: []

df.query('DETAIL == "33329A"')
```

```{code-cell} ipython3
:tags: []

df.query('DETAIL == "33329A"').groupby('DETAIL')['ap'].sum()
```

Merged totals are almost equal to CBP totals.
Small difference is likely due to CBP noise (in CBP itself total != sum of 6-digit rows).

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
t = {}
t['merged'] = df[['est', 'emp', 'ap']].sum()
t['total - 23'] = d.query('NAICS == "-"').iloc[0, 1:] - d.query('NAICS == "23"').iloc[0, 1:]
t['ratio'] = t['merged'] / t['total - 23']
pd.concat(t, axis=1)
```

IO industries that remain unmatched are "111" and "112" (farming, not covered by CBP), "482" (rail transportation) and "814" (Private households).

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
df.query('DETAIL.notna() and ap.isna()')
```

Payroll distribution across IO sectors.

```{code-cell} ipython3
:tags: []

t = df.groupby('SECTOR')[['est', 'emp', 'ap']].sum()
t['ap'].plot(figsize=(16, 4))
```

# Full test

```{code-cell} ipython3
:tags: [nbd-module]

@log_start_finish
def test_all(redownload=False):
    test_get_sup(redownload)
    test_get_use(redownload)
    test_get_ixi(redownload)
    test_get_ixc(redownload)
    test_get_cxc(redownload)
    test_get_naics_df(redownload)
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_all(redownload=False)
```

# Build this module

```{code-cell} ipython3
:tags: []

nbd.nb2mod('bea_io.ipynb')
```
