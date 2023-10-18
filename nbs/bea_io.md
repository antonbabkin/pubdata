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
title: "Input-Output Accounts"
format:
  html: 
    code-fold: true
    toc: true
    df-print: paged
    embed-resources: true
    ipynb-filters:
      - pubdata/reseng/nbd.py filter-docs
---
```

+++ {"tags": ["nbd-docs"]}

A series of detailed tables showing how industries interact with each other and with the rest of the economy.
**Supply tables** show the goods and services produced by domestic industries as well as imports of these goods and services.
**Use tables** show who uses these goods and services, including other industries.
**Requirements tables** summarize the full supply chain, including direct and indirect inputs.

[Home page](https://www.bea.gov/data/industries/input-output-accounts-data)

```{code-cell} ipython3
:tags: [nbd-module, nbd-docs]

import sys
import zipfile
import typing
import shutil
import logging
from contextlib import redirect_stdout

import pandas as pd

from pubdata.reseng.util import download_file
from pubdata.reseng.monitor import log_start_finish
from pubdata.reseng.nbd import Nbd
from pubdata import naics, cbp

nbd = Nbd('pubdata')


log = logging.getLogger('pubdata.bea_io')
log.handlers.clear()
log.addHandler(logging.StreamHandler(sys.stdout))


log.setLevel('INFO')
cbp.log.setLevel('INFO')

PATH = {
    'src': nbd.root / 'data/bea_io/src',
    'proc': nbd.root / 'data/bea_io/'
}
```

```{code-cell} ipython3
:tags: [nbd-docs]

def dispall(df):
    with pd.option_context('display.max_columns', None, 'display.max_rows', None, 'display.max_colwidth', None):
        display(df)
```

+++ {"tags": ["nbd-docs"]}

# Synopsis

```{code-cell} ipython3
:tags: [nbd-docs]

from IPython.display import Markdown

def func_doc(f):
    arg = ', '.join(f.__code__.co_varnames[:f.__code__.co_argcount])
    sig = f'{f.__name__}({arg})'
    doc = f.__doc__.replace('\n', '  \n')
    return f'**{sig}**  \n{doc}'

def funcs_doc(*fs):
    return '\n\n'.join(func_doc(f) for f in fs)

from pubdata import bea_io
```

```{code-cell} ipython3
:tags: [nbd-docs]

Markdown(funcs_doc(
    bea_io.get_sup,
    bea_io.get_use,
    bea_io.get_ixi,
    bea_io.get_ixc,
    bea_io.get_cxc,
    bea_io.get_naics_concord
))
```

+++ {"tags": ["nbd-docs"]}

# Source files

In this section, source files are identified and downloaded.

Current I-O tables can be accessed as interactive [web-based tables](https://apps.bea.gov/iTable/?reqid=150&step=2&isuri=1&categories=Io), individual [Excel spreadsheets](https://www.bea.gov/industry/input-output-accounts-data) or a zipped [bulk download](https://apps.bea.gov/iTable/?isuri=1&reqid=151&step=1).

Older tables are available in the [Data Archive](https://apps.bea.gov/histdata/histChildLevels.cfm?HMI=8).
Tables get revised over time.
For example, 2012 detail level tables were first published as 2018, Q2 Comprehensive vintage, but 2022, Q2 Annual vintage contains the more recent revision.
When multiple revitions exist for the same data year, we use the most recent.

Revisions used in this module:
- current, Supply-Use: Detail tables 2017. Sector and summary tables 2017-2022.
- 2022, Q2 (September-29-2022), Supply-Use: Detail tables 2007, 2012. Sector and summary tables 1997-2016.

Detail tables for 2002 and earlier are available [here](https://www.bea.gov/industry/historical-benchmark-input-output-tables).

```{code-cell} ipython3
:tags: [nbd-module, nbd-docs]

def _get_src(year):
    if year == 2022:
        url = 'https://apps.bea.gov/histdata/Releases/Industry/2022/GDP_by_Industry/Q2/Annual_September-29-2022/AllTablesSUP.zip'
        fnm = 'AllTablesSUP_2022q2.zip'
    elif year == 2023:
        url = 'https://apps.bea.gov/industry/iTables%20Static%20Files/AllTablesSUP.zip'
        fnm = 'AllTablesSUP_2023.zip'
    path = PATH['src'] / fnm
    if path.exists():
        log.debug(f'Source file already exists: {path}')
        return path
    log.debug(f'File {fnm} not found, attempting download from {url}')
    path.parent.mkdir(parents=True, exist_ok=True)
    download_file(url, PATH['src'], fnm)
    log.debug(f'File downloaded to {path}')
    # tables are read directly from Zip archive, without explicitcly extracting all files
    return path
```

```{code-cell} ipython3
:tags: []

_get_src(2023)
```

+++ {"tags": ["nbd-docs"]}

table reader

All tables share similar layout, and `_read_table()` function is used to read a table from a spreadsheet.

```{code-cell} ipython3
:tags: [nbd-module, nbd-docs]

def _read_table(src, spreadsheet, sheet, level, labels, skip_head, skip_foot):
    
    log.debug(f'Reading table from {src.name}/{spreadsheet}/{sheet}')
    
    with zipfile.ZipFile(src) as z:
        df = pd.read_excel(
            z.open(spreadsheet),
            sheet_name=sheet,
            header=None,
            dtype=str,
            skiprows=skip_head,
            skipfooter=skip_foot
        )
    
    # swap code and label rows for consistency with sec and sum
    if level == 'det':
        df.iloc[[0, 1], :] = df.iloc[[1, 0], :].values    

    row_names = df.iloc[2:, :2].values.tolist()
    col_names = df.iloc[:2, 2:].values.T.tolist()
    df.columns = df.iloc[1, :] if labels else df.iloc[0, :]
    df.index = df.iloc[:, 1] if labels else df.iloc[:, 0]
    df = df.iloc[2:, 2:]
    df = df.replace('...', None).astype('float64')
    
    return dict(table=df, row_names=row_names, col_names=col_names)
```

+++ {"tags": ["nbd-docs"]}

# Supply tables

The supply and make tables present the commodities that are produced by each industry.
The **supply table** extends the framework, showing supply from domestic and foreign producers that are available for use in the domestic economy in both basic and purchasers’ prices.

```{code-cell} ipython3
:tags: [nbd-module, nbd-docs]

def get_sup(year, level, labels=False):
    """Supply table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2022 for "sec" and "sum"; 2007, 2012 or 2017 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """

    y = str(year)
    if year < 2017:
        src = _get_src(2022)
        if level == 'sec':
            x = _read_table(src, 'Supply_Tables_1997-2021_SEC.xlsx', y, level, labels, 5, 0)
        elif level == 'sum':
            x = _read_table(src, 'Supply_Tables_1997-2021_SUM.xlsx', y, level, labels, 5, 0)
        elif level == 'det':
            x = _read_table(src, 'Supply_2007_2012_DET.xlsx', y, level, labels, 4, 2)
    else:
        src = _get_src(2023)
        if level == 'sec':
            x = _read_table(src, 'Supply_Tables_2017-2022_Sector.xlsx', y, level, labels, 5, 0)
        elif level == 'sum':
            x = _read_table(src, 'Supply_Tables_2017-2022_Summary.xlsx', y, level, labels, 5, 0)
        elif level == 'det':
            x = _read_table(src, 'Supply_2017_DET.xlsx', y, level, labels, 4, 2)

    x['table'].index.name = 'commodity'
    x['table'].columns.name = 'industry'
    
    return x

@log_start_finish
def test_get_sup():
    for year in range(1997, 2023):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012, 2017]:
                continue
            for labels in [False, True]:
                x = get_sup(year, level, labels)
                print(year, level, labels, x['table'].shape)
                assert len(x['table']) > 0
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

## 2022 Supply table, sector level
```

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: screen-inset
dispall(get_sup(2022, 'sec', True)['table'].apply(lambda c: c.apply(lambda x: '{:,.0f}'.format(x) if pd.notna(x) else '')))
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

```{code-cell} ipython3
:tags: [nbd-module, nbd-docs]

def get_use(year, level, labels=False):
    """Use table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2022 for "sec" and "sum"; 2007, 2012 or 2017 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """
    
    y = str(year)
    if year < 2017:
        src = _get_src(2022)
        if level == 'sec':
            x = _read_table(src, 'Use_SUT_Framework_1997-2021_SECT.xlsx', y, level, labels, 5, 0)
        elif level == 'sum':
            x = _read_table(src, 'Use_SUT_Framework_1997-2021_SUM.xlsx', y, level, labels, 5, 0)
        elif level == 'det':
            x = _read_table(src, 'Use_SUT_Framework_2007_2012_DET.xlsx', y, level, labels, 4, 2)
    else:
        src = _get_src(2023)
        if level == 'sec':
            x = _read_table(src, 'Use_Tables_Supply-Use_Framework_2017-2022_Sector.xlsx', y, level, labels, 5, 0)
        elif level == 'sum':
            x = _read_table(src, 'Use_Tables_Supply-Use_Framework_2017-2022_Summary.xlsx', y, level, labels, 5, 0)
        elif level == 'det':
            x = _read_table(src, 'Use_SUT_Framework_2017_DET.xlsx', y, level, labels, 4, 2)

    x['table'].index.name = 'commodity'
    x['table'].columns.name = 'industry'
    
    return x


@log_start_finish
def test_get_use():
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012, 2017]:
                continue
            for labels in [False, True]:
                x = get_use(year, level, labels)
                print(year, level, x['table'].shape)
                assert len(x['table']) > 0
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

## 2022 Use table, sector level
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: [nbd-docs]
---
#| column: screen-inset
d = get_use(2022, 'sec', True)['table']
dispall(d.apply(lambda c: c.apply(lambda x: '{:,.0f}'.format(x) if pd.notna(x) else '')))
```

```{raw-cell}
:tags: [nbd-docs]

:::
```

+++ {"tags": ["nbd-docs"]}

Example: 2017 dollar value of top 10 detail level commodities used is inputs to Grain farming.

```{code-cell} ipython3
:tags: [nbd-docs]

x = get_use(2017, 'det')
x['table']\
    .loc[:'S00900', ['1111B0']]\
    .sort_values('1111B0', ascending=False)\
    .head(10)\
    .rename(index=dict(x['row_names']), columns=dict(x['col_names']))\
    .astype(int)
```

+++ {"tags": ["nbd-docs"]}

Use table subtotals satisfy the following accounting identity:

$$
\text{Total industry output} = \text{Total Intermediate} + \text{Compensation of employees} + \text{Gross operating surplus} + \text{Net taxes}
$$

Table below shows percentage shares of total output by industry in 2022 at the sector level.

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: [nbd-docs]
---
#| column: body-outset
d = get_use(2022, 'sec', True)['table']

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
---
jupyter:
  outputs_hidden: true
tags: [nbd-docs]
---
#| column: body-outset
t = {}
for y in range(1997, 2023):
    d = get_use(y, 'sec', True)['table']
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

```{code-cell} ipython3
:tags: [nbd-module, nbd-docs]

def get_ixi(year, level, labels):
    """Industry-by-industry Total requirements table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2021 for "sec" and "sum"; 2007 or 2012 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """
    
    src = _get_src(2022)
    y = str(year)
    
    if level == 'sec':
        x = _read_table(src, 'IxI_TR_1997-2021_PRO_SEC.xlsx', y, level, labels, 5, 2)
    elif level == 'sum':
        x = _read_table(src, 'IxI_TR_1997-2021_PRO_SUM.xlsx', y, level, labels, 5, 2)
    elif level == 'det':
        x = _read_table(src, 'IxI_TR_2007_2012_PRO_DET.xlsx', y, level, labels, 3, 0)

    x['table'].index.name = 'industry'
    x['table'].columns.name = 'industry'
    
    return x

@log_start_finish
def test_get_ixi():
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                d = get_ixi(year, level, labels)['table']
                print(year, level, labels, d.shape)
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
get_ixi(2021, 'sec', True)['table'].round(3)
```

```{raw-cell}
:tags: [nbd-docs]

:::
```

+++ {"tags": ["nbd-docs"]}

## Industry-by-Commodity

```{code-cell} ipython3
:tags: [nbd-module, nbd-docs]

def get_ixc(year, level, labels):
    """Industry-by-commodity Total requirements table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2021 for "sec" and "sum"; 2007 or 2012 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """
    
    src = _get_src(2022)
    y = str(year)
    
    if level == 'sec':
        x = _read_table(src, 'IxC_TR_1997-2021_PRO_SEC.xlsx', y, level, labels, 5, 2)
    elif level == 'sum':
        x = _read_table(src, 'IxC_TR_1997-2021_PRO_SUM.xlsx', y, level, labels, 5, 2)
    elif level == 'det':
        x = _read_table(src, 'IxC_TR_2007_2012_PRO_DET.xlsx', y, level, labels, 3, 0)

    x['table'].index.name = 'industry'
    x['table'].columns.name = 'commodity'
    
    return x

@log_start_finish
def test_get_ixc():
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                d = get_ixc(year, level, labels)['table']
                print(year, level, labels, d.shape)
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
---
jupyter:
  outputs_hidden: true
tags: [nbd-docs]
---
#| column: screen-inset
get_ixc(2021, 'sec', True)['table'].round(3)
```

```{raw-cell}
:tags: [nbd-docs]

:::
```

+++ {"tags": ["nbd-docs"]}

## Commodity-by-Commodity

```{code-cell} ipython3
:tags: [nbd-module, nbd-docs]

def get_cxc(year, level, labels):
    """Commodity-by-commodity Total requirements table (Supply-Use Framework) as a dataframe, along with row and column labels.
    `level` can be "sec", "sum" or "det".
    `year` can be 1997-2021 for "sec" and "sum"; 2007 or 2012 for "det".
    `labels` True to use commodity/industry names instead of columns as row/column labels.
    Returns dict with keys "table", "row_names" and "col_names".
    """
    
    src = _get_src(2022)
    y = str(year)
    
    if level == 'sec':
        x = _read_table(src, 'CxC_TR_1997-2021_PRO_SEC.xlsx', y, level, labels, 5, 2)
    elif level == 'sum':
        x = _read_table(src, 'CxC_TR_1997-2021_PRO_SUM.xlsx', y, level, labels, 5, 2)
    elif level == 'det':
        x = _read_table(src, 'CxC_TR_2007_2012_PRO_DET.xlsx', y, level, labels, 3, 0)

    x['table'].index.name = 'commodity'
    x['table'].columns.name = 'commodity'
    
    return x

@log_start_finish
def test_get_cxc():
    for year in range(1997, 2022):
        for level in ['sec', 'sum', 'det']:
            if level == 'det' and year not in [2007, 2012]:
                continue
            for labels in [False, True]:
                d = get_cxc(year, level, labels)['table']
                print(year, level, labels, d.shape)
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
---
jupyter:
  outputs_hidden: true
tags: [nbd-docs]
---
#| column: screen-inset
get_cxc(2021, 'sec', True)['table'].round(3)
```

```{raw-cell}
:tags: [nbd-docs]

:::
```

+++ {"tags": ["nbd-docs"]}

# BEA-NAICS concordance

BEA uses industry classification that is different from NAICS.
Crosswalk is provided in every detail level spreadsheet.
"NAICS Codes" sheet is parsed so that at the lowest classification level ("detail") each row corresponds to a single NAICS code.
Detail industries with multiple NAICS are split into multiple rows.
Levels about "detail" have their separate rows.

```{code-cell} ipython3
:tags: [nbd-module, nbd-docs]

def get_naics_concord(year):
    """Return dataframe with BEA-NAICS concordance table.
    `year` can be 2012 or 2017.
    """
    
    if year == 2012:
        src = _get_src(2022)
        spreadsheet = 'Use_SUT_Framework_2007_2012_DET.xlsx'
    elif year == 2017:
        src = _get_src(2023)
        spreadsheet = 'Use_SUT_Framework_2017_DET.xlsx'
    sheet = 'NAICS Codes'
    log.debug(f'Reading table from {src.name}/{spreadsheet}/{sheet}')
    
    with zipfile.ZipFile(src) as z:
        df = pd.read_excel(
            z.open(spreadsheet),
            sheet_name=sheet,
            dtype=str,
            skiprows=4,
            skipfooter=6
        )

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
    df['summary'] = df.groupby('sector', sort=False)['summary'].fillna(method='ffill')
    df['u_summary'] = df.groupby(['sector', 'summary'], sort=False)['u_summary'].fillna(method='ffill')

    df['naics'] = df['naics'].str.strip().apply(_split_codes)
    df = df.explode('naics', ignore_index=True)
    
    # drop non-existent NAICS codes, created from expanding ranges like "5174-9"
    feasible_naics_codes = ['23*', 'n.a.'] + naics.get_df(year, 'code')['CODE'].to_list()
    df = df[df['naics'].isna() | df['naics'].isin(feasible_naics_codes)]
    
    df[df.isna()] = None
    df = df.reset_index(drop=True)
    
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

def test_get_naics_concord():
    
    assert _split_codes('1') == ['1']
    assert _split_codes('1, 2') == ['1', '2']
    assert _split_codes('1-3') == ['1', '2', '3']
    assert _split_codes('1-3, 5') == ['1', '2', '3', '5']
    assert _split_codes('1-3, 5-7') == ['1', '2', '3', '5', '6', '7']
    
    d = get_naics_concord(2012)
    assert len(d) > 0
    d = get_naics_concord(2017)
    assert len(d) > 0
```

```{code-cell} ipython3
:tags: []

test_get_naics_concord()
```

+++ {"tags": ["nbd-docs"]}

Example: all sector level industries.

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: body-outset
dispall(get_naics_concord(2017).query('summary.isna()'))
```

+++ {"tags": ["nbd-docs"]}

Example: Information sector.

```{code-cell} ipython3
:tags: [nbd-docs]

#| column: body-outset
d = naics.get_df(2017, 'code').rename(columns={'CODE': 'naics', 'TITLE': 'naics_title'})[['naics', 'naics_title']]
dispall(get_naics_concord(2017).query('sector == "51"').merge(d, 'left').fillna(''))
```

+++ {"tags": ["nbd-docs"]}

## Concordance properties

No overlaps between branches of the SECTOR -> SUMMARY -> U_SUMMARY -> DETAIL hierarchy.

- No code in one branch exists in another branch.
- From a code in one level, can unambiguously go to upper levels.

```{code-cell} ipython3
:tags: [nbd-docs]

y = 2017
df = get_naics_concord(y)\
    .query('summary.notna()')\
    .drop_duplicates(subset=['sector', 'summary'])
assert not df['summary'].duplicated().any()

df = get_naics_concord(y)\
    .query('u_summary.notna()')\
    .drop_duplicates(subset=['sector', 'summary', 'u_summary'])
assert not df['u_summary'].duplicated().any()

df = get_naics_concord(y)\
    .query('detail.notna()')\
    .drop_duplicates(subset=['sector', 'summary', 'u_summary', 'detail'])
assert not df['detail'].duplicated().any()
```

+++ {"tags": ["nbd-docs"]}

Construction (IO sector "23", NAICS sector "23") matches exactly on sector level, i.e. no part of IO "23" maps to NAICS outside of "23", and no part of NAICS "23" maps to IO outside of "23". But match below sector level is impossible. Explanation from IO footnote:

> Construction data published by BEA at the detail level do not align with 2012 NAICS industries.  In NAICS, industries are classified based on their production processes, whereas BEA construction is classified by type of structure.  For example, activity by the 2012 NAICS Roofing contractors industry would be split among many BEA construction categories because roofs are built on many types of structures.

```{code-cell} ipython3
:tags: [nbd-docs]

df = get_naics_concord(2017)
assert list(df.loc[df['sector'] == '23', 'naics'].dropna().unique()) == ['23*']
assert list(df.loc[df['naics'].str[:2] == '23', 'sector'].unique()) == ['23']
dispall(df.query('sector == "23"'))
```

+++ {"tags": ["nbd-docs"]}

16 IO detail industries do not map into any NAICS.
Most of them are in sector "G" (Goverment).

```{code-cell} ipython3
:tags: [nbd-docs]

dispall(get_naics_concord(2017).query('naics == "n.a."'))
```

+++ {"tags": ["nbd-docs"]}

An ambiguity exists in IO sector "53" (REAL ESTATE AND RENTAL AND LEASING). Two detail level industries ("531HST" and "531ORE") map into the same 3-digit NAICS "531" (Real Estate). There are no other NAICS duplicates.

```{code-cell} ipython3
:tags: [nbd-docs]

df = get_naics_concord(2017)
df = df.query('naics.notna() and naics != "n.a." and naics != "23*"')
assert set(df.loc[df['naics'].duplicated(), 'naics']) == {'531'}

dispall(get_naics_concord(2017).query('sector == "53"'))
```

+++ {"jp-MarkdownHeadingCollapsed": true, "tags": ["nbd-docs"]}

With the exception of NAICS "23", "n.a." and "531" *relationship between IO DETAIL and NAICS is one-to-many*.

```{code-cell} ipython3
:tags: [nbd-docs]

d = get_naics_concord(2017)[['detail', 'naics']]\
    .query('detail.notna() and naics != "n.a." and naics != "23*" and naics != "531"')
assert not d['naics'].duplicated().any()
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
:tags: [nbd-docs]

dfna = naics.get_df(2012, 'code')
dfio = get_naics_concord(2012)
assert dfio.query('naics.notna() and naics != "n.a." and naics != "23*"')['naics'].isin(dfna['CODE']).all()
```

+++ {"tags": ["nbd-docs"]}

No NAICS code matches on more than one level.
I.e. there is no situation when some lower NAICS level is covered more than once because it's parent is include someplace else.

```{code-cell} ipython3
:tags: [nbd-docs]

dfio = get_naics_concord(2012)[['detail', 'naics']]\
    .query('naics.notna() and naics != "n.a." and naics != "23*"')\
    .drop_duplicates(subset=['naics']) # NAICS 531 dupe

df = naics.get_df(2012, 'code').rename(columns=str.lower)
for digit in [2, 3, 4, 5, 6]:
    ind = f'merge_{digit}'
    df = df.merge(dfio, 'left', left_on=f'code_{digit}', right_on='naics', indicator=ind)\
        .drop(columns='naics')\
        .rename(columns={'detail': f'detail_{digit}'})
    df[ind] = df[ind].map({'both': 1, 'left_only': 0})
df.loc[df['code_2'] == '23', 'merge_2'] = 1

df['merge_sum'] = df[[f'merge_{d}' for d in [2,3,4,5,6]]].sum(1)
assert df['merge_sum'].isin([0, 1]).all()
```

+++ {"tags": ["nbd-docs"]}

Every industry outside of sector "92" is covered.
Nothing is covered in sector "92".

```{code-cell} ipython3
:tags: [nbd-docs]

assert (df.query('code_2 != "92" and digits == 6')['merge_sum'] == 1).all()
assert (df.query('code_2 == "92" and digits == 6')['merge_sum'] == 0).all()
```

+++ {"tags": ["nbd-docs"]}

NAICS sector "92" (Public Administration) is similar to IO sector "G" (GOVERNMENT).
This looks similar to the construction sector "23": although sectors are alike, lower level division is done on a different principle.
Although unlike "23", the crosswalk is not explicitly stating that NAICS "92" can be mapped to IO "G" on a sector level.
Maybe because of the "Postal service" industry (IO detail "491000", part of sector "G") that corresponds to NAICS subsector "491".

```{code-cell} ipython3
:tags: [nbd-docs]

get_naics_concord(2012).query('sector == "G" and summary.notna() and u_summary.isna()')[['sector', 'summary', 'description']]
```

```{code-cell} ipython3
:tags: [nbd-docs]

dispall(df.query('code_2 == "92" and digits <= 3')[['code', 'title']])
```

+++ {"tags": ["nbd-docs"]}

## 3-digit crosswalk

BEA "summary" can be compared to NAICS "subsector". BEA->NAICS crosswalk is often one-to-many, which is not a problem when we convert NAICS-based data to match BEA. But BEA->NAICS is also many-to-one for three NAICS subsectors (336, 531, 541).

```{code-cell} ipython3
:tags: [nbd-docs]

df = get_naics_concord(2012)
df = df[['summary', 'detail', 'naics']].dropna().query('naics != "n.a."')
df['naics3'] = df['naics'].str[:3]
df = df[['summary', 'naics3']].drop_duplicates()
df['dup_io'] = df['summary'].duplicated(False)
df['dup_n3'] = df['naics3'].duplicated(False)
df[df['dup_n3']]
```

+++ {"tags": ["nbd-docs"]}

# Example: merge CBP to IO

Because we know that IO-NAICS concordance fully covers NAICS with no double-counting, we can simply merge it to the CBP.
Only need to take care of "23", "G" and "531".

```{code-cell} ipython3
:tags: [nbd-docs]

df = get_naics_concord(2012)
# ignore government sector
df = df.query('sector != "G"')
# merge construction on sector level
df = df.query('(sector != "23") or summary.isna()')
df.loc[df['sector'] == '23', 'naics'] = '23'

d = cbp.get_cbp_df('us', 2012)\
    .query('lfo == "-"')\
    [['naics', 'est', 'emp', 'ap']]
d['naics'] = d['naics'].str.replace('-', '').str.replace('/', '')

df = df.merge(d, 'left', 'naics', indicator=True)
# split "531" between "531HST" and "531ORE" with equal weights
df.loc[df['detail'] == '531HST', ['est', 'emp', 'ap']] *= 0.5
df.loc[df['detail'] == '531ORE', ['est', 'emp', 'ap']] *= 0.5
```

+++ {"tags": ["nbd-docs"]}

Detail aggregates, example - U.Summary 3132: Beverage manufacturing.

```{code-cell} ipython3
:tags: [nbd-docs]

t = df.groupby(['sector', 'summary', 'u_summary', 'detail', 'description'])[['est', 'emp', 'ap']].sum().reset_index()
t.query('u_summary == "3121"')
```

+++ {"tags": ["nbd-docs"]}

Summary aggregates, example - Sector 31ND: Nondurable Goods.

```{code-cell} ipython3
:tags: [nbd-docs]

t = df.groupby(['sector', 'summary'])[['est', 'emp', 'ap']].sum().reset_index()\
    .merge(df.query('u_summary.isna()')[['summary', 'description']], 'left')
t.query('sector == "31ND"')
```

+++ {"tags": ["nbd-docs"]}

Sector aggregates.

```{code-cell} ipython3
:tags: [nbd-docs]

dispall(df.groupby(['sector'])[['est', 'emp', 'ap']].sum().reset_index()\
    .merge(df.query('summary.isna()')[['sector', 'description']], 'left'))
```

+++ {"tags": ["nbd-docs"]}

Merged totals are almost equal to CBP totals.
Small difference is likely due to CBP noise (in CBP itself total != sum of 6-digit rows).

```{code-cell} ipython3
:tags: [nbd-docs]

t = {}
t['merged'] = df[['est', 'emp', 'ap']].sum()
t['total - 23'] = d.query('naics == ""').iloc[0, 1:] - d.query('naics == "23"').iloc[0, 1:]
t['ratio'] = t['merged'] / t['total - 23']
pd.concat(t, axis=1)
```

+++ {"tags": ["nbd-docs"]}

IO industries that remain unmatched are "111" and "112" (farming, not covered by CBP), "482" (rail transportation) and "814" (Private households).

```{code-cell} ipython3
:tags: [nbd-docs]

dispall(
    df.query('detail.notna() and (naics != "n.a.") and ap.isna()')\
    .drop(columns=['est', 'emp', 'ap', '_merge'])\
    .merge(naics.get_df(2012, 'code').rename(columns=str.lower)[['code', 'title']].rename(columns={'code': 'naics', 'title': 'naics_title'}), 'left')
)
```

+++ {"tags": ["nbd-docs"]}

Payroll distribution across IO sectors.

```{code-cell} ipython3
:tags: [nbd-docs]

t = df.groupby('sector')[['est', 'emp', 'ap']].sum()
t['ap'].plot.bar(figsize=(16, 4));
```

# Full test

```{code-cell} ipython3
:tags: [nbd-module]

@log_start_finish
def test_all():
    test_get_sup()
    test_get_use()
    test_get_ixi()
    test_get_ixc()
    test_get_cxc()
    test_get_naics_concord()
```

```{code-cell} ipython3
---
jupyter:
  outputs_hidden: true
tags: []
---
test_all()
```

# Build this module

```{code-cell} ipython3
:tags: []

nbd.nb2mod('bea_io.ipynb')
```
