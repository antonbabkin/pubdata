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

pd.options.display.max_rows = None
pd.options.display.max_colwidth = None
```

+++ {"tags": []}

# Source files

```{code-cell} ipython3
:tags: []

def get_source_files():
    if (PATH['source'] / 'AllTablesSUP').exists(): return
    url = 'https://apps.bea.gov/industry/iTables Static Files/AllTablesSUP.zip'
    f = download_file(url, PATH['source'])
    with zipfile.ZipFile(f) as z:
        z.extractall(PATH['source'])
```

# IO-NAICS concordance

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
    df = df.rename(columns=str.upper)
    
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
---
jupyter:
  outputs_hidden: true
tags: []
---
get_naics_df().query('SUMMARY.isna()')
```

Example: Information sector.

```{code-cell} ipython3
:tags: []

d = naics.get_df(NAICS_REV, 'code').rename(columns={'CODE': 'NAICS', 'TITLE': 'NAICS_TITLE'})[['NAICS', 'NAICS_TITLE']]
get_naics_df().query('SECTOR == "51"').merge(d, 'left')
```

# Concordance properties

+++

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

16 IO detail industries do not map into any NAICS.
Most of them are in sector "G" (Goverment).

```{code-cell} ipython3
:tags: []

get_naics_df().query('NAICS == "n.a."')
```

An ambiguity exists in IO sector "53" (REAL ESTATE AND RENTAL AND LEASING). Two detail level industries ("531HST" and "531ORE") map into the same 3-digit NAICS "531" (Real Estate). There are no other NAICS duplicates.

```{code-cell} ipython3
:tags: []

df = get_naics_df()
df = df.query('NAICS.notna() and NAICS != "n.a." and NAICS != "23*"')
assert set(df.loc[df['NAICS'].duplicated(), 'NAICS']) == {'531'}

get_naics_df().query('SECTOR == "53"')
```

+++ {"jp-MarkdownHeadingCollapsed": true, "tags": []}

With the exception of NAICS "23", "n.a." and "531" *relationship between IO DETAIL and NAICS is one-to-many*.

```{code-cell} ipython3
:tags: []

d = get_naics_df()\
    [['DETAIL', 'NAICS']]\
    .query('DETAIL.notna() and NAICS != "n.a." and NAICS != "23*" and NAICS != "531"')
assert not d['NAICS'].duplicated().any()
```

+++ {"tags": []}

## NAICS coverage

Which NAICS industries are covered by the IO industries?

In sum, every industry in the NAICS is covered and no industry is covered more than once with these caveats:
- Sector "23" (construction) only maps at the sector level.
- Government (sector "G" in IO and "92" in NAICS) likely maps at the sector level.
- NAICS subsector "531" (real estate) is covered twice.

+++

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

No NAICS code matches on more than one level.
I.e. there is no situation when some lower NAICS level is covered more than once because it's parent is include someplace else.

```{code-cell} ipython3
:tags: []

df['MERGE_SUM'] = df[[f'MERGE_{d}' for d in [2,3,4,5,6]]].sum(1)
assert df['MERGE_SUM'].isin([0, 1]).all()
```

Every industry outside of sector "92" is covered.
Nothing is covered in sector "92".

```{code-cell} ipython3
:tags: []

assert (df.query('CODE_2 != "92" and DIGITS == 6')['MERGE_SUM'] == 1).all()
assert (df.query('CODE_2 == "92" and DIGITS == 6')['MERGE_SUM'] == 0).all()
```

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

Merged totals are almost equal to CBP totals.
Small difference is likely due to CBP noise (in CBP itself total != sum of 6-digit rows).

```{code-cell} ipython3
:tags: []

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

# 3-digit crosswalk

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
