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

# Input-Output Tables

```{code-cell} ipython3
:tags: []

import zipfile

import pandas as pd

from pubdata.reseng.util import download_file
from pubdata.reseng.nbd import Nbd
from pubdata import naics, cbp

nbd = Nbd('pubdata')
PATH = {
    'source': nbd.root/'data/source/bea_io/',
    'naics_codes': nbd.root/'data/bea_io/naics_codes.csv'
}
NAICS_REV = 2012
```

```{code-cell} ipython3
:tags: []

def get_source_files():
    if (PATH['source'] / 'AllTablesSUP').exists(): return
    url = 'https://apps.bea.gov/industry/iTables Static Files/AllTablesSUP.zip'
    f = download_file(url, PATH['source'])
    with zipfile.ZipFile(f) as z:
        z.extractall(PATH['source'])
```

"NAICS Codes" table is parsed so that at the lowest classification level ("detail") each row corresponds to a single NAICS code. Detail industries with multiple NAICS are split into multiple rows. Levels about "detail" have their separate rows.

```{code-cell} ipython3
:tags: []

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

    df['naics'] = df['naics'].str.strip().apply(split_codes)
    df = df.explode('naics', ignore_index=True)
    
    # drop non-existent NAICS codes, created from expanding ranges like "5174-9"
    feasible_naics_codes = ['23*', 'n.a.'] + naics.get_df(NAICS_REV, 'code')['CODE'].to_list()
    df = df[df['naics'].isna() | df['naics'].isin(feasible_naics_codes)]
    
    df[df.isna()] = None
    df = df.reset_index(drop=True)
    
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return df

def split_codes(codes):
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

assert split_codes('1') == ['1']
assert split_codes('1, 2') == ['1', '2']
assert split_codes('1-3') == ['1', '2', '3']
assert split_codes('1-3, 5') == ['1', '2', '3', '5']
assert split_codes('1-3, 5-7') == ['1', '2', '3', '5', '6', '7']
```

Example: all sector level industries.

```{code-cell} ipython3
:tags: []

get_naics_df().query('summary.isna()')
```

Example: Information sector.

```{code-cell} ipython3
:tags: []

d = naics.get_df(NAICS_REV, 'code').rename(columns={'CODE': 'naics', 'TITLE': 'naics_title'})[['naics', 'naics_title']]
get_naics_df().query('sector == "51"').merge(d, 'left')
```

## Merging IO tables with CBP via NAICS

```{code-cell} ipython3
:tags: []

def get_cbp_df():
    df = cbp.get_df('us', 2014)\
        .query('lfo == "-" and industry != "-"')\
        .rename(columns={'industry': 'naics'})\
        [['naics', 'ind_digits', 'emp', 'est']]\
        .copy()\
        .reset_index(drop=True)
    
    d = naics.get_df(NAICS_REV, 'code').drop(columns='TITLE')
    # clip combined codes to match CBP convention: "31-33" -> "31" etc
    d.loc[d['DIGITS'] == 2, 'CODE'] = d['CODE'].str[:2]
    df = df.merge(d, 'left', left_on='naics', right_on='CODE')\
        .drop(columns=['CODE', 'DIGITS'])

    return df
```

```{code-cell} ipython3
:tags: []

get_cbp_df().head()
```

Construction (IO sector "23", NAICS sector "23") matches exactly on sector level, i.e. no part of IO "23" maps to NAICS outside of "23", and no part of NAICS "23" maps to IO outside of "23". But match below sector level is impossible.

From IO footnote:
> Construction data published by BEA at the detail level do not align with 2012 NAICS industries.  In NAICS, industries are classified based on their production processes, whereas BEA construction is classified by type of structure.  For example, activity by the 2012 NAICS Roofing contractors industry would be split among many BEA construction categories because roofs are built on many types of structures.

```{code-cell} ipython3
:tags: []

df = get_naics_df()
assert list(df.loc[df['sector'] == '23', 'naics'].dropna().unique()) == ['23*']
assert list(df.loc[df['naics'].str[:2] == '23', 'sector'].unique()) == ['23']
```

A few IO detail industries do not map into any NAICS.

```{code-cell} ipython3
:tags: []

get_naics_df().query('naics == "n.a."')
```

An ambiguity exists in IO sector "53" (REAL ESTATE AND RENTAL AND LEASING). Two detail level industries ("531HST" and "531ORE") map into the same 3-digit NAICS "531" (Real Estate).

Other ambiguities could arise across different levels, i.e. one industry maps to lower level NAICS, while its higher level NAICS parent also maps to some other IO industry. I did not check for this.

```{code-cell} ipython3
:tags: []

df = get_naics_df()
df = df.query('naics.notna() and naics != "n.a." and naics != "23*"')
df[df['naics'].duplicated(False)]
```

```{code-cell} ipython3
:tags: []

di = get_naics_df()
dc = get_cbp_df()

dc.loc[dc['CODE_2'] == '23', 'match'] = True
io_naics = di.query('naics.notna() and naics != "n.a." and naics != "23*"')['naics']
for n in [2, 3, 4, 5, 6]:
    dc.loc[dc[f'CODE_{n}'].isin(io_naics), 'match'] = True

di.loc[di['sector'] == '23', 'match'] = True
di.loc[di['naics'].isin(dc['naics']), 'match'] = True
```

All CBP NAICS codes can be matched to the IO table.

```{code-cell} ipython3
:tags: []

assert dc.loc[dc['ind_digits'] == 6, 'match'].all()
```

Table below shows percentage of detail industries in IO sectors that can be fully matched to CBP.

```{code-cell} ipython3
:tags: []

t = di.loc[di['summary'].isna(), ['sector', 'description']].set_index('sector')
df = di.query('detail.notna()').drop_duplicates('detail')
t['total'] = df.groupby('sector').size()

df = di.query('detail.notna()').copy()
df['match'] = df['match'].fillna(False)
df = df.groupby('detail').agg({'sector': 'first', 'match': 'min'}).reset_index()
t['match'] = df.groupby('sector')['match'].sum()

t.loc['TOTAL', 'description'] = 'ALL SECTORS'
t.loc['TOTAL', ['total', 'match']] = t[['total', 'match']].sum()
t['% match'] = t['match'] / t['total']
t = t.reset_index()
with pd.option_context('display.max_colwidth', 300):
    display(t.style.format({'total': '{:.0f}', 'match': '{:.0f}', '% match': '{:.1%}'}))
```

All unmatched IO detail industries are listed below. Mainly farming and government - they are not covered by CBP.

```{code-cell} ipython3
:tags: []

unmatched_detail = di.query('naics.notna() and match != True')['detail']
with pd.option_context('display.max_colwidth', 300):
    display(di[di['detail'].isin(unmatched_detail)].fillna(False).reset_index(drop=True))
```

# 3-digit crosswalk

BEA "summary" can be compared to NAICS "subsector". BEA->NAICS crosswalk is often one-to-many, which is not a problem when we convert NAICS-based data to match BEA. But BEA->NAICS is also many-to-one for three NAICS subsectors (336, 531, 541).

```{code-cell} ipython3
:tags: []

df = get_naics_df()
df = df[['summary', 'detail', 'naics']].dropna()
df['naics3'] = df['naics'].str[:3]
df = df[['summary', 'naics3']].drop_duplicates()
df['dup_io'] = df['summary'].duplicated(False)
df['dup_n3'] = df['naics3'].duplicated(False)
df[df['dup_n3']]
```
