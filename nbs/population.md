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

+++ {"tags": []}

# Population

Annual population by county 1970-2019. Intercensal estimates from Census Population Estimates Program ([PEP](https://www.census.gov/programs-surveys/popest.html)).

In this notebook we download 10-year blocks of data, harmonize format and combine into a single dataframe. Full table is returned by `pop()`.
National totals have state code `00`, and state totals have county code `000`.

[Character encoding](https://www.census.gov/programs-surveys/geography/technical-documentation/user-note/special-characters.html) of plain text files: newer files use `"UTF-8"`, older use `"ISO-8859-1"`.

```{code-cell} ipython3
:tags: [nbd-module]

import io

import pandas as pd
import pyarrow
import pyarrow.dataset

from pubdata.reseng.util import download_file
from pubdata.reseng.nbd import Nbd

nbd = Nbd('pubdata')
PATH = {
    'root': nbd.root,
    'source': nbd.root/'data/source/population/',
    'parquet': nbd.root/'data/population/population.pq'
}
```

```{code-cell} ipython3
:tags: []

pd.options.display.max_rows = 120
```

## Download and prepare 10-year blocks

[2010-2019](https://www.census.gov/data/datasets/time-series/demo/popest/2010s-counties-total.html) data are in a CSV file that needs minimal processing.

```{code-cell} ipython3
:tags: [nbd-module]

def prep_2010_2019():
    url = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv'
    f = download_file(url, PATH['source'])
    cols = ['STATE', 'COUNTY'] + [f'POPESTIMATE{y}' for y in range(2010, 2020)]
    df = pd.read_csv(f, encoding='ISO-8859-1', dtype='str', usecols=cols)
    df = pd.wide_to_long(df, 'POPESTIMATE', ['STATE', 'COUNTY'], 'year')
    df = df.reset_index().rename(columns={'STATE': 'st', 'COUNTY': 'cty', 'POPESTIMATE': 'pop'})
    df['pop'] = df['pop'].astype('int')
    # add US total
    d = df.query('cty == "000"').groupby('year')['pop'].sum().to_frame('pop').reset_index()
    d[['st', 'cty']] = ['00', '000']
    df = pd.concat([df, d])
    return df
```

[2000-2010](https://www.census.gov/data/datasets/time-series/demo/popest/intercensal-2000-2010-counties.html) data are in a CSV file that needs minimal processing.

```{code-cell} ipython3
:tags: [nbd-module]

def prep_2000_2009():
    url = 'https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/co-est00int-tot.csv'
    f = download_file(url, PATH['source'])
    dt = {'STATE': str, 'COUNTY': str}
    cols = ['STATE', 'COUNTY'] + [f'POPESTIMATE{y}' for y in range(2000, 2010)]
    df = pd.read_csv(f, encoding='ISO-8859-1', usecols=cols, dtype='str')
    df = pd.wide_to_long(df, 'POPESTIMATE', ['STATE', 'COUNTY'], 'year')
    df = df.reset_index().rename(columns={'STATE': 'st', 'COUNTY': 'cty', 'POPESTIMATE': 'pop'})
    df['st'] = df['st'].str.pad(2, fillchar='0')
    df['cty'] = df['cty'].str.pad(3, fillchar='0')
    df['pop'] = df['pop'].astype('int')
    # add US total
    d = df.query('cty == "000"').groupby('year')['pop'].sum().to_frame('pop').reset_index()
    d[['st', 'cty']] = ['00', '000']
    df = pd.concat([df, d])
    return df
```

1990-2000 intercensal county tables are in [PDF files arranged by state](https://www.census.gov/data/tables/time-series/demo/popest/intercensal-1990-2000-state-and-county-totals.html). Another format found on [FTP server](https://www2.census.gov/programs-surveys/popest/tables/1990-2000/estimates-and-change-1990-2000/) is a long text file, which is parsed in `pop_1990_1999()`. Table with state and county population has `"1"` as the first character in every line. We use this to read necessary lines into a temporary string buffer, and then parse the buffer into a dataframe.

```{code-cell} ipython3
:tags: [nbd-module]

def prep_1990_1999():
    url = 'https://www2.census.gov/programs-surveys/popest/tables/1990-2000/estimates-and-change-1990-2000/2000c8_00.txt'
    f = download_file(url, PATH['source'])
    with open(f, encoding='ISO-8859-1') as file:
        data = io.StringIO()
        in_table = False
        for line in file:
            if in_table:
                if line[0] == '1':
                    data.write(line)
                else:
                    break
            else:
                if line[0] == '1':
                    in_table = True
                    data.write(line)

    data.seek(0)
    df = pd.read_fwf(data, dtype='str', header=None)
    # keep fips and popest cols
    df = df.iloc[:, 1:13]
    df.iloc[0, 0] = '00000' # US total
    df.columns = ['fips'] + [f'pop{y}' for y in range(2000, 1989, -1)]
    df['fips'] = df['fips'].str.pad(5, 'right', '0')
    df['st'] = df['fips'].str[:2]
    df['cty'] = df['fips'].str[2:]
    df = df.drop(columns=['pop2000', 'fips'])
    df = pd.wide_to_long(df, 'pop', ['st', 'cty'], 'year')
    df = df.reset_index()
    df['pop'] = pd.to_numeric(df['pop'].str.replace(',', '', regex=False)).astype('int')

    return df
```

[1980-1989](https://www2.census.gov/programs-surveys/popest/tables/1980-1990/counties/totals/) data are in Excel spreadsheet, arranged as alternating blocks of 1980-1984 and 1984-1989 columns.

```{code-cell} ipython3
:tags: [nbd-module]

def prep_1980_1989():
    url = 'https://www2.census.gov/programs-surveys/popest/tables/1980-1990/counties/totals/e8089co.xls'
    f = download_file(url, PATH['source'])
    df = pd.read_excel(f, skiprows=13, header=None)
    
    # create indicator of alternating blocks of 1980-1984 and 1984-1989 columns
    df['block'] = df.loc[df[0] == 'FIPS Code', 2]
    df = df.dropna(how='all')
    df['block'] = df['block'].fillna(method='ffill')
    
    # stack blocks side by side
    d0 = df.query('block == "Census 1980"').reset_index(drop=True)
    d1 = df.query('block == "Estimate 1985"').reset_index(drop=True)
    assert d0.shape == d1.shape
    df = pd.concat([d0, d1], axis=1, ignore_index=True)
    assert df[0].equals(df[8]), 'FIPS codes on two sides do not match'

    df = df.drop([1, 7, 8, 9, 15], axis=1)
    df.columns = ['fips'] + [f'pop{y}' for y in range(1980, 1990)]
    df = df.query('fips != "FIPS Code"')
    df = pd.wide_to_long(df, ['pop'], 'fips', 'year').reset_index()
    df['pop'] = df['pop'].astype('int64')
    df['st'] = df['fips'].str[:2]
    df['cty'] = df['fips'].str[2:]
    del df['fips']
    return df
```

[1970-1979](https://www2.census.gov/programs-surveys/popest/tables/1900-1980/counties/totals/) data are in a text file, arranged as alternating blocks of 1970-1974 and 1975-1979 columns.

```{code-cell} ipython3
:tags: [nbd-module]

def prep_1970_1979():
    url = 'https://www2.census.gov/programs-surveys/popest/tables/1900-1980/counties/totals/e7079co.txt'
    f = download_file(url, PATH['source'])
    df = pd.read_fwf(f, widths=[6, 16, 10, 10, 10, 10, 10], header=None, skiprows=26)
    df = df[df[0] != 'FIPS']
    
    # join multi-line rows that happen when county name is longer than column width
    assert ((df[0].isna().astype(int) + df[2].isna().astype(int)) <= 1).all(), 'FIPS and pop columns should not be both NA'
    df[[2, 3, 4, 5, 6]] = df[[2, 3, 4, 5, 6]].fillna(method='bfill')
    df = df[df[0].notna()]

    # create indicator of alternating blocks of 1970-1974 and 1975-1979 columns
    df['block'] = df.loc[df[0] == 'Code', 2]
    df['block'] = df['block'].fillna(method='ffill')

    # stack blocks side by side
    d0 = df.query('block == "1970"').reset_index(drop=True)
    d1 = df.query('block == "1975"').reset_index(drop=True)
    assert d0.shape == d1.shape
    df = pd.concat([d0, d1], axis=1, ignore_index=True)
    assert df[0].equals(df[8]), 'FIPS codes on two sides do not match'

    df = df.drop([1, 7, 8, 9, 15], axis=1)
    df.columns = ['fips'] + [f'pop{y}' for y in range(1970, 1980)]
    df = df.query('fips != "Code"')
    df = pd.wide_to_long(df, ['pop'], 'fips', 'year').reset_index()
    df['pop'] = df['pop'].astype('int64')
    df['st'] = df['fips'].str[:2]
    df['cty'] = df['fips'].str[2:]
    del df['fips']
    return df
```

## Combine all years

In `pop()` we call all functions for 10 year blocks and combine all into a single frame. Final dataframe is cached as parquet file.

```{code-cell} ipython3
:tags: []

nbd.root.mkdir?
```

```{code-cell} ipython3
:tags: [nbd-module]

def get_df():
    df_file = PATH['parquet']
    if df_file.exists():
        return pd.read_parquet(df_file)
    
    df = pd.concat([prep_1970_1979(), prep_1980_1989(), prep_1990_1999(), prep_2000_2009(), prep_2010_2019()], ignore_index=True)

    df = df.sort_values(['st', 'cty', 'year']).reset_index()
    df = df[['st', 'cty', 'year', 'pop']]

    df_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(df_file, engine='pyarrow', index=False)
    return df
```

## Data preview

+++

There is a notable discontinuity between 1999 and 2000, visible both at state and national level.

```{code-cell} ipython3
:tags: []

df = get_df()
s = ['00'] + list(df.query('cty == "000"')['st'].sample(4))
d = df.query('cty == "000" and st.isin(@s)').set_index(['year', 'st'])['pop'].unstack().sort_index()
d.apply(lambda row: row / d.loc[2000, :], 1).plot(title='Population in US and random states, normalized to 1 in 2000', grid=True, figsize=(9, 6));
```

One likely explanation of the jump is that 1999 estimates, built from the 1990 base in the 1990-1999 file, were below actual population numbers later revealed by census, as can be seen in [this](https://www2.census.gov/programs-surveys/popest/tables/1990-2000/estimates-2000-change-1990-2000/2000c4-00.txt) report:
```
                                            4/1/2000    4/1/2000
FIPS State/County Code and Area Name          Census    Estimate
      United States..................... 281,421,906 274,608,346
```

Notably, on county level the 2000 jump is not always present.

```{code-cell} ipython3
:tags: []

df = get_df()
s = list(df.query('st == "55"')['cty'].sample(5))
d = df.query('st == "55" and cty.isin(@s)').set_index(['year', 'cty'])['pop'].unstack().sort_index()
d.apply(lambda row: row / d.loc[2000, :], 1).plot(title='Population in random Wisconsin counties, normalized to 1 in 2000', grid=True, figsize=(9, 6));
```

+++ {"tags": []}

## Alternative source: Census API

PEP data are also available from [API](https://www.census.gov/data/developers/data-sets/popest-popproj/popest.html), but:
- it's only available from 1990
- every 10 years are separate APIs
- 2010s are not available as time series, only year by year

+++

## Build this module

```{code-cell} ipython3
:tags: []

nbd.nb2mod('population.ipynb')
```
