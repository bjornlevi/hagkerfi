# Data Requirements — Master Brief for Researcher

**Compiled by:** Project Manager  
**Date:** 2026-04-18  
**Status:** Handed to Researcher for source location

Each requirement lists: what is needed, why, which agent uses it, and the desired format.

---

## Already Confirmed (in pipeline, do not re-locate)

| Dataset | Source | Format | Used by |
|---|---|---|---|
| Population by age (2024) | Hagstofa MAN00101.px API | JSON | Agent 1/2 |
| Income by gender+age (2023) | Hagstofa TEK01001.px API | JSON | Agent 1/3 |
| Employment by age+gender (June 2024) | Hagstofa VIN10001.px API | JSON | Agent 1/3 |
| National budget 2026 | stjornarradid.is XLSX | Excel | Agent 6 |
| Municipal accounts | Samband sveitarfélaga XLSX | Excel | Agent 6 |
| Municipal income tax rates | utsvar_sveitarfelaga.xls | Excel | Agent 6 |
| Property tax table | property_tax_amount.xlsx | Excel | Agent 6 |

---

## REQUIRED — Must locate before M1

### R-01: Housing Inventory (HMS.is)
**Needs:** Apartment/property data for Iceland — unit count, type (1BR/2BR/house), size (m²), municipality, estimated market value, ownership vs rental status  
**Why:** Agent 2 (Households) allocates individuals into dwellings. Cannot form households without knowing what dwellings exist.  
**Format:** Any (API, CSV, XLSX) — as long as it covers all municipalities  
**Used by:** Agent 2, Agent 4 (property values for mortgages), Agent 6 (property tax base)  
**Notes:** HMS (Húsnæðis- og mannvirkjastofnun) is the Housing and Construction Authority. Check hms.is for open data or API.

### R-02: Household Size Distribution
**Needs:** Number of households by size (1-person, 2-person, ... 6+) in Iceland, preferably by municipality  
**Why:** Agent 2 needs to know how many households of each size to form to match reality  
**Format:** Table (age optional, municipality preferred)  
**Used by:** Agent 2  
**Source hint:** Hagstofa — mannfjöldi/fjölskyldur or similar

### R-03: Marital/Cohabitation Status by Age and Gender
**Needs:** Proportion of population that is married, cohabiting, single, divorced, widowed — by age and gender  
**Why:** Agent 2 partner-matching algorithm needs this to determine household formation probabilities  
**Format:** Table  
**Used by:** Agent 2  
**Source hint:** Hagstofa — hjúskaparstaða

### R-04: Fertility Rates by Age of Mother
**Needs:** Births per 1,000 women by age group (or age-specific fertility rate, ASFR)  
**Why:** Agent 2 assigns children to households; Agent 7 (Time Engine) uses this for monthly birth events  
**Format:** Table  
**Used by:** Agent 2, Agent 7  
**Source hint:** Hagstofa — fæðingar, or Frjósemi

### R-05: Mortality Tables (Life Tables)
**Needs:** Probability of death by age and gender  
**Why:** Agent 7 (Time Engine) removes persons from the simulation each month as they die  
**Format:** Table: (age, gender, annual mortality rate)  
**Used by:** Agent 7  
**Source hint:** Hagstofa — lífslíkur, dánartíðni

### R-06: Business Registry by Sector and Size Class
**Needs:** Count of active businesses in Iceland, broken down by: industry sector (ÍSAT/NACE), size class (0-9, 10-49, 50-249, 250+ employees)  
**Why:** Agent 3 generates synthetic businesses calibrated to this distribution  
**Format:** Table  
**Used by:** Agent 3  
**Source hint:** Hagstofa — fyrirtæki, or RSK Fyrirtækjaskrá, or Hagstofa VIN/FYR tables

### R-07: Employment by Sector
**Needs:** Number of employed persons per industry sector (ÍSAT classification)  
**Why:** Agent 3 must ensure the synthetic business sector allocation matches real employment distribution  
**Format:** Table: (sector, employed_count)  
**Used by:** Agent 3  
**Source hint:** Hagstofa VIN or similar — atvinna eftir starfsgreinum

### R-08: Mortgage Interest Rates (Current + Historical)
**Needs:** Current indexed and non-indexed mortgage interest rates in Iceland; historical series (2020–2025) for calibration  
**Why:** Agent 4 sets mortgage monthly payments; Agent 7 updates rates monthly from projection table  
**Format:** Table or time series  
**Used by:** Agent 4, Agent 7  
**Source hint:** Seðlabanki Íslands — húsnæðislán, vextir

### R-09: Household Debt Statistics
**Needs:** Average household debt-to-income ratio; debt breakdown (mortgage vs personal loan vs other) by income bracket if available  
**Why:** QA calibration target for Agent 4 — confirms that mortgage and loan assignment is realistic  
**Format:** Summary statistics acceptable  
**Used by:** Agent 4, QA  
**Source hint:** Seðlabanki — fjármálastöðugleiki, skýrslur, household balance sheets

### R-10: Student Loan Terms (LÍN)
**Needs:** Average student loan balance, monthly repayment amount, interest rate, eligibility criteria  
**Why:** Agent 4 assigns student loans to Student-status persons age 18–25  
**Format:** Summary / published terms  
**Used by:** Agent 4  
**Source hint:** lin.is — lánakjör, úthlutun

### R-11: CPI Basket Weights by Category
**Needs:** Consumer price index basket composition — percentage weight of each spending category (housing, food, transport, utilities, health, education, recreation, etc.)  
**Why:** Agent 5 (Expenses) calibrates category spending shares to this basket  
**Format:** Table: (category, weight %)  
**Used by:** Agent 5  
**Source hint:** Hagstofa — neysluverðlag, CPI weights

### R-12: Electricity and Heating Prices
**Needs:** Average household electricity price per kWh; average annual heating cost (geothermal where applicable)  
**Why:** Agent 5 utility expense category  
**Format:** Price per unit or average annual household cost  
**Used by:** Agent 5  
**Source hint:** Orkustofnun, Samorka, or Hagstofa

### R-13: State Pension Amounts (Tryggingastofnun)
**Needs:** Monthly pension payment amounts by age/category (full pension, partial pension); means-testing thresholds  
**Why:** Agent 6 pays pension transfers to Retired-status persons each month  
**Format:** Published rate table  
**Used by:** Agent 6  
**Source hint:** tr.is — ellilífeyrir, greiðslur

### R-14: Disability Benefit Amounts
**Needs:** Monthly disability benefit (örorkubætur) amounts; eligibility thresholds  
**Why:** Agent 6 pays disability transfers to Disabled-status persons  
**Format:** Published rate table  
**Used by:** Agent 6  
**Source hint:** tr.is — örorkubætur

### R-15: Child Benefit Amounts (Barnabætur)
**Needs:** Monthly child benefit per child; income tapering schedule  
**Why:** Agent 6 pays child benefits to households with children under 18  
**Format:** Published rate table with income tapering formula  
**Used by:** Agent 6  
**Source hint:** skatturinn.is — barnabætur, or tr.is

### R-16: Seðlabanki Economic Projections
**Needs:** Published forecasts for: inflation (CPI), policy interest rate, GDP growth, unemployment rate — for 2025–2027  
**Why:** Agent 7 Time Engine reads economic parameters from a projection table each month  
**Format:** Time series by quarter or year is fine; we'll interpolate to monthly  
**Used by:** Agent 7  
**Source hint:** Seðlabanki — Peningamál, þjóðhagsspá

### R-17: Car Ownership Rate
**Needs:** Number of registered private vehicles in Iceland; ideally by household income or municipality  
**Why:** Agent 5 assigns auto loan + fuel + insurance expenses to car-owning households  
**Format:** Aggregate statistic or table  
**Used by:** Agent 5  
**Source hint:** Samgöngustofa (vehicle registry), or Hagstofa

### R-18: Average Rent by Apartment Size / Municipality
**Needs:** Average monthly rent for 1BR, 2BR, 3BR apartments; ideally by Reykjavik vs other municipalities  
**Why:** Agent 5 assigns rent expense to renting households; also needed to validate HMS property value data  
**Format:** Table  
**Used by:** Agent 5, Agent 4  
**Source hint:** HMS leiguverðlag, Leiguverð.is, or Hagstofa

---

## NICE TO HAVE — Locate if time permits

| ID | Description | Source hint | Agent |
|---|---|---|---|
| N-01 | Unemployment benefit amounts | tr.is | Agent 6 |
| N-02 | VAT rates by product category | skatturinn.is | Agent 6 |
| N-03 | Corporate tax rate + rules | skatturinn.is / RSK | Agent 3, 6 |
| N-04 | Average food expenditure per household | Hagstofa household budget survey | Agent 5 |
| N-05 | Health insurance / out-of-pocket healthcare costs | sjukra.is or Hagstofa | Agent 5 |
| N-06 | Childcare costs by age / municipality | KRAKKAR.is or municipalities | Agent 5 |
| N-07 | Household savings rate | Hagstofa or Seðlabanki | Agent 4 |
| N-08 | Migration data (immigration/emigration by age) | Hagstofa — flutningsmenn | Agent 7 |
