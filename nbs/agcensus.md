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
title: "NASS - Census of Agriculture"
format:
  html:
    code-fold: true
    standalone: true
    embed-resources: true
execute:
  echo: false
jupyter: python3
date: today
date-format: long
---
```

In this module we use USDA NASS 2017 [Census of Agriculture](https://www.nass.usda.gov/AgCensus/index.php) to estimate county output in NAICS subsectors "111: Crop Production" and "112: Animal Production and Aquaculture".
These industries are not covered by CBP.

```{code-cell} ipython3
:tags: []

import io

import pandas as pd
from rurec.pubdata import naics, bea_io
```

```{raw-cell}

::: {.callout-note appearance=minimal collapse=true}

### Relevant section of the BEA-NAICS crosswalk
```

```{code-cell} ipython3
:tags: []

d = bea_io.get_naics_df()
d = d.query('SUMMARY == "111CA"').rename(columns={'NAICS': 'NAICS_CODE'})
d = d.merge(naics.get_df(2017, 'code').add_prefix('NAICS_'), 'left', 'NAICS_CODE')
d[['SECTOR', 'SUMMARY', 'U_SUMMARY', 'DETAIL', 'DESCRIPTION', 'NAICS_CODE', 'NAICS_TITLE']].fillna('')
```

:::

+++

# Aggregate farm sales

Census of agriculture comes as one giant text file. First need to find rows that correspond to Volume 1, Chapter 2, Table 2: "Market Value of Agricultural Products Sold Including Food Marketing Practices and Value-Added Products".

```{code-cell} ipython3
:tags: []

# dowload URL: https://www.nass.usda.gov/Publications/AgCensus/2017/Online_Resources/Census_Data_Query_Tool/2017_cdqt_data.txt.gz
d = pd.read_csv('../data/nass/2017_cdqt_data.txt', sep='\t', usecols=['CENSUS_CHAPTER', 'CENSUS_TABLE'])
d.query('CENSUS_CHAPTER == 2 and CENSUS_TABLE == 2')
```

```{code-cell} ipython3
# NAICS index items lookup to match NASS commodities to NAICS industries.
d = naics.get_df(2017, 'index')
d[d['INDEX_ITEM'].str.lower().str.contains('christmas')]
```

```{code-cell} ipython3
:tags: []

# relevant rows in table 2: sales in dollars
sales_items = [
# 'COMMODITY TOTALS - SALES, MEASURED IN $',
    'CROP TOTALS - SALES, MEASURED IN $',
        'GRAIN - SALES, MEASURED IN $', 
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
            'FRUIT & TREE NUT TOTALS, (EXCL BERRIES) - SALES, MEASURED IN $',
            'BERRY TOTALS - SALES, MEASURED IN $',
        'HORTICULTURE TOTALS, (EXCL CUT TREES & VEGETABLE SEEDS & TRANSPLANTS) - SALES, MEASURED IN $',
        'CUT CHRISTMAS TREES & SHORT TERM WOODY CROPS - SALES, MEASURED IN $',
            'CUT CHRISTMAS TREES - SALES, MEASURED IN $',
            'SHORT TERM WOODY CROPS - SALES, MEASURED IN $',
        'FIELD CROPS, OTHER, INCL HAY - SALES, MEASURED IN $',
            'MAPLE SYRUP - SALES, MEASURED IN $',
    'ANIMAL TOTALS, INCL PRODUCTS - SALES, MEASURED IN $',
        'POULTRY TOTALS, INCL EGGS - SALES, MEASURED IN $',
        'CATTLE, INCL CALVES - SALES, MEASURED IN $',
        'MILK - SALES, MEASURED IN $', 
        'HOGS - SALES, MEASURED IN $',
        'SHEEP & GOATS TOTALS, INCL WOOL & MOHAIR & MILK - SALES, MEASURED IN $',
        'EQUINE, (HORSES & PONIES) & (MULES & BURROS & DONKEYS) - SALES, MEASURED IN $',
        'AQUACULTURE TOTALS - SALES & DISTRIBUTION, MEASURED IN $'
        'SPECIALTY ANIMAL TOTALS, (EXCL EQUINE) - SALES, MEASURED IN $',
]
```

Load relevant rows, reshape and manually map NASS commodities to BEA detail industries.

```{code-cell} ipython3
:tags: []

#| echo: true
all_columns = pd.read_csv('../data/nass/2017_cdqt_data.txt', sep='\t', nrows=1).columns
df = pd.read_csv('../data/nass/2017_cdqt_data.txt', sep='\t', skiprows=3424331, nrows=226369, names=all_columns, dtype=str,
                 usecols=['SHORT_DESC', 'AGG_LEVEL_DESC', 'STATE_FIPS_CODE', 'COUNTY_CODE', 'DOMAINCAT_DESC', 'VALUE'])

# (D)	Withheld to avoid disclosing data for individual farms.
# (Z)	Less than half of the unit shown.
df['VALUE'] = df['VALUE'].str.replace(',', '').replace({'(D)': None, '(Z)': None}).astype('float64')

df = df.query('DOMAINCAT_DESC.isna()')
del df['DOMAINCAT_DESC']
 
df = df.query('SHORT_DESC.isin(@sales_items)')
df['SHORT_DESC'] = df['SHORT_DESC'].str.split(' - ', expand=True)[0]

df['STCTY'] = df['STATE_FIPS_CODE'] + df['COUNTY_CODE'].fillna('000')
del df['STATE_FIPS_CODE']
del df['COUNTY_CODE']

df = df.set_index(['AGG_LEVEL_DESC', 'STCTY', 'SHORT_DESC'])['VALUE'].unstack()
df = df.fillna(0)

df['1111A0'] = df['SOYBEANS']
df['1111B0'] = df['GRAIN'] - df['SOYBEANS']
df['111200'] = df['VEGETABLE TOTALS, INCL SEEDS & TRANSPLANTS, IN THE OPEN']
df['111300'] = df['FRUIT & TREE NUT TOTALS']
df['111400'] = (df['HORTICULTURE TOTALS, (EXCL CUT TREES & VEGETABLE SEEDS & TRANSPLANTS)']
                + df['CUT CHRISTMAS TREES & SHORT TERM WOODY CROPS'])
df['111900'] = df['TOBACCO'] + df['COTTON, LINT & SEED'] + df['FIELD CROPS, OTHER, INCL HAY']
df['1121A0'] = df['CATTLE, INCL CALVES']
df['112120'] = df['MILK']
df['112300'] = df['POULTRY TOTALS, INCL EGGS']
df['112A00'] = df['ANIMAL TOTALS, INCL PRODUCTS'] - df[['1121A0', '112120', '112300']].sum(1)

df = df[[c for c in df.columns if c[:2] == '11']]
df.columns.name = 'BEA_INDUSTRY_DETAIL'
df = df.stack()
df.name = 'SALES'
df = df.reset_index()
df['SALES'] = df['SALES'].astype('int64')
# '112A00', '1111B0' numbers come out negative sometimes. maybe because totals are suppressed and set to 0.
# quick fix: set negatives to 0
df.loc[df['SALES'] < 0, 'SALES'] = 0
```

The resulting table looks like this.

```{code-cell} ipython3
:tags: []

df.head()
```

```{code-cell} ipython3
:tags: []

df.to_csv('../data/nass/agcensus_sales_by_bea.csv', index=False)
```

# Validate farm sales

To validate sales numbers computed from the NASS ag census, we compare national totals with what is reported by BEA.
The match is not perfect, but not too bad either.

These are some known disparities:

- BEA values from 2012, NASS values from 2017.
- Oilseed farming from NASS is only soybeans, while BEA "1111A0" further includes NAICS "11112: Oilseed (except Soybean) Farming".
- BEA number is from column "T019: Total use of products" in the "Use Table (Supply-Use Framework)", which is probably not the same as sales.

```{code-cell} ipython3
:tags: []

d = bea_io.get_naics_df()
d = d.query('SUMMARY == "111CA"').drop(columns='NAICS').drop_duplicates()
d = d.merge(df.query('AGG_LEVEL_DESC == "NATIONAL"'), 'left', left_on='DETAIL', right_on='BEA_INDUSTRY_DETAIL')
d = d.rename(columns={'SALES': 'NASS_SALES'})
d['NASS_SALES'] = (d['NASS_SALES'] / 1_000_000).round()

# exerpt from BEA "The Use Table (Supply-Use Framework), 2012"
# column "T019: Total use of products"
d1 = pd.read_csv(io.StringIO("""
DETAIL,BEA_TOTAL_USE
1111A0,48815
1111B0,100268
111200,37211
111300,63899
111400,35422
111900,40456
112120,44521
1121A0,90659
112300,44972
112A00,43822
"""))
d = d.merge(d1, 'left', 'DETAIL')
```

```{code-cell} ipython3
:tags: []

ax = d.set_index('DETAIL')[['NASS_SALES', 'BEA_TOTAL_USE']].dropna()\
    .plot(kind='bar', figsize=(12, 4), rot=0,
          title='Output by BEA detail industry within "111CA: Farms"')
```
