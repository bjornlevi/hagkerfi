# Data Catalog

**Owner:** Researcher  
**Last updated:** 2026-04-18  
**Status:** M0 initial survey complete

All Hagstofa PX-API tables are queried via POST to `https://px.hagstofa.is/pxis/api/v1/is/{path}` with a JSON payload. See existing `pipelines/landing_download.py` for the pattern.

---

## CONFIRMED IN PIPELINE (do not re-download)

| ID | Table/File | Path/URL | Last checked |
|---|---|---|---|
| ✅ | Population by age — MAN00101.px | Ibuar/mannfjoldi/1_yfirlit/Yfirlit_mannfjolda/ | 2026-04-18 |
| ✅ | Income by gender+age — TEK01001.px | Samfelag/launogtekjur/3_tekjur/1_tekjur_skattframtol/ | 2026-04-18 |
| ✅ | Employment by age+gender — VIN10001.px | Samfelag/vinnumarkadur/vinnuaflskraargogn/ | 2026-04-18 |
| ✅ | National budget 2026 — fjarlog_2026.xlsx | stjornarradid.is (static) | 2026-04-18 |
| ✅ | Municipal accounts — arsreikningar_sveitarfelaga.xlsx | Samband sveitarfélaga CDN | 2026-04-18 |
| ✅ | Municipal income tax rates — utsvar_sveitarfelaga.xls | Manual drop-in | 2026-04-18 |
| ✅ | Property tax table — property_tax_amount.xlsx | Manual drop-in | 2026-04-18 |

---

## NEW SOURCES — REQUIRED

### R-01: Housing Inventory
**Status:** Partial — use Census 2021 + HMS rental dashboard  
**Primary:** Hagstofa Census 2021 housing tables  
- `CEN1701.px` — dwellings by municipality (1981/2011/2021)  
- `CEN1704.px` — by building type  
- `CEN1706.px` — by ownership type (owner vs renter)  
- `CEN1709.px` — characteristics by municipality  
- API path: `Ibuar/manntal/2021/7_husnaedi/`  
- Key figures: **78% owner-occupied, 22% rental, ~10% vacant** (2021)  

**Rental prices:** HMS Leiguverðsjá — https://hms.is/gogn-og-maelabord/maelabordleiguskra/leiguverdsja  
- Updated monthly; filterable by municipality, bedrooms, m²  
- **Not directly API-accessible** (Vercel JS app blocks curl)  
- Feb 2026 averages: 1-room ISK 190,843/mo · 2-room ISK 252,006/mo · 3-room ISK 294,344/mo  
- Per m²: Reykjavík ISK 4,041 · Kópavogur ISK 3,810 · national ISK 3,639  

**Annual report:** https://hms.is/skyrslur/vegvisir-leigumarkadar-2025  
**⚠ Limitation:** Census data is 2021. No annual dwelling count API exists. HMS rental data requires browser scraping or manual extraction.  
**Action for Data Engineer:** Query CEN1701–CEN1709 via PX-API. Manually extract HMS rental averages from annual report PDF and hardcode as constants until a scraper can be built.

---

### R-02: Household Size Distribution
**Status:** Found — two complementary tables  
- `MAN07108.px` — nuclear family types by municipality, **updated annually through 2026**  
  Path: `Ibuar/fjolsk/Fjolskyldan/`  
  Covers: singles, married without children, married with children, cohabiting without/with children, lone fathers, lone mothers  
- `CEN1511.px` — household count by number of persons (2011 + 2021 census only)  
- `CEN1514.px` — mean household size by municipality  
**Action:** Use MAN07108 for household type distribution (current). Use CEN1511 for size-in-persons calibration.

---

### R-03: Marital / Cohabitation Status by Age and Gender
**Status:** Found — excellent coverage  
- `MAN07209.px` — population 16+ by gender, single-year age, marital class, **annually through 2026**  
  Path: `Ibuar/fjolsk/Fjolskyldan/`  
  Categories: Ógiftir og einhleypir · Ógiftir í sambúð · Giftir í samvistum við maka · Giftir ekki í samvistum · Ekkill/ekkja · Fráskilinn  
- `CEN1505.px` — de facto cohabitation from 2011/2021 census  
**Action:** Use MAN07209.px directly. Single-year age × gender × 6 status categories — ideal for household formation algorithm.

---

### R-04: Fertility Rates by Age of Mother
**Status:** Found  
- `MAN05101.px` — live births by age of mother (single years), **through 2024**  
  Path: `Ibuar/Faeddirdanir/Faeddir/faedingar/`  
- `MAN05202.px` — total fertility rate (TFR) 1853–2024  
- `MAN05204.px` — births by mother's age and marital status  
**⚠ Note:** Age-specific fertility rates (ASFR) are not pre-calculated in the table. Must divide births by female population at each age (from MAN00101) to derive ASFR.  
**Action:** Download MAN05101 + MAN00101 female population. Compute ASFR in bronze stage. Data Engineer to implement.

---

### R-05: Mortality Tables / Life Tables
**Status:** Found  
- `MAN05401.px` — remaining life expectancy + survivors by age, gender, 5-year period **through 2020–2024**  
  Path: `Ibuar/Faeddirdanir/danir/danir/`  
- `MAN05221.px` — deaths by municipality, gender, age 1981–2024  
- `MAN05210.px` — deaths and death rate 1841–2024  
**⚠ Note:** Probability of death (q_x) must be derived from survivor counts (l_x). Standard life table: q_x = 1 - (l_{x+1} / l_x).  
**Action:** Download MAN05401. Derive q_x in bronze stage. Single-year age × gender × period available.

---

### R-06: Business Registry by Sector and Size Class
**Status:** Found — two tables needed together  
- `FYR01001.px` — registered firms by ÍSAT sector + legal form, **2008–2025** (updated Jan 2026)  
  Path: `Atvinnuvegir/fyrirtaeki/skradfyrirtaeki/3_fyrirtaeki/`  
- `FYR09001.px` — active businesses by sector + **employee size class**, business demography  
  Path: `Atvinnuvegir/fyrirtaeki/skradfyrirtaeki/lydfraedi/` (verify exact path)  
**⚠ Note:** RSK Fyrirtækjaskrá has no confirmed open data API.  
**Action:** Use FYR09001 for size class distribution. Use FYR01001 for sector counts by legal form. Researcher to verify FYR09001 path before Data Engineer downloads.

---

### R-07: Employment by Sector
**Status:** Found  
- `VIN10022.px` — employed persons by ÍSAT sector, month, gender, age, **through April 2026**  
  Path: `Samfelag/vinnumarkadur/vinnuaflskraargogn/`  
  ⚠ Age groupings are aggregate (16–74, 25–64), not single years  
- `VIN10032.px` — similar but by workplace industry rather than worker's primary sector  
**Action:** Use VIN10022. Sector-level employment as calibration target for Agent 3.

---

### R-08: Mortgage Interest Rates
**Status:** Partial — current figures confirmed, automated access blocked  
**Current rates (Nov 2025):**  
- Non-indexed (óverðtryggð): **~8.60%**  
- Indexed (verðtryggð): **~4.00%**  
- Central Bank policy rate: **7.25%** (4th cut of 2025, down from 9.0% peak)  
**Source:** https://sedlabanki.is/gagnatorg/vextir/ (Blazor JS app — no REST API)  
**Alternative:** https://www.gagnabanki.is/report/interests (unofficial mirror — evaluate)  
**Historical:** Available via the data portal web UI; downloadable as Excel through browser.  
**⚠ Limitation:** No programmatic API for Seðlabanki rate data confirmed. Must be hardcoded from manual download or scraped.  
**Action:** Hardcode current rates as constants in Agent 4. Data Engineer to investigate gagnabanki.is for historical series access.

---

### R-09: Household Debt Statistics
**Status:** Found  
- `THJ09000.px` — debts, assets, net worth by family type, age, residence, **1997–2024** (updated Sep 2025)  
  Path: `Samfelag/lifskjor/5_skuldastada_heimili/1_skuldir_eignir/`  
- `THJ09010.px` — debt-to-income ratio distribution, **1997–2024**  
- Aggregate: household debt = **71.4% of GDP** as of Dec 2024  
**Action:** Use THJ09010 as QA calibration target for Agent 4. Download both tables.

---

### R-10: Student Loan Terms (Menntasjóður)
**Status:** Partial — system confirmed, specific rates require browser visit  
**Institution:** Menntasjóður námsmanna (replaced LÍN in July 2020)  
**URL:** https://menntasjodur.is  
- Offers indexed + non-indexed loans  
- Repayment up to 7 years post-study  
- Maximum annual loan amount set in yearly rules  
**⚠ Limitation:** menntasjodur.is did not return content in automated fetch. Current 2025/2026 rate not confirmed from automated access.  
**Action:** Researcher to manually check menntasjodur.is in browser and update this entry with current rates. Until then, use indexed rate = CPI + 1% as proxy.

---

### R-11: CPI Basket Weights by Category
**Status:** Found  
- `VIS01306.px` — weights for all CPI sub-indices, **COICOP2018 classification, updated Jan 2026**  
  Path: `Efnahagur/visitolur/1_vnv/2_undirvisitolur/`  
- `VIS01300.px` — sub-index values and changes (updated Mar 2026)  
**⚠ Note:** Classification switched from ISLCOICOP to COICOP2018 in January 2026. Mapping paper available from Hagstofa.  
**Action:** Download VIS01306. Use category weights to calibrate Agent 5 spending shares.

---

### R-12: Electricity and Heating Prices
**Status:** Partial — usable constants available  
**Electricity:**  
- Average household rate (Sep 2025): **ISK 25.76/kWh** (all-in, incl. distribution + taxes)  
- Source: GlobalPetrolPrices.com citing Eurostat  
- Hagstofa IDN02303.px has Nordic comparison data but only through 2022  
**District heating:**  
- Capital area (geothermal): ~ISK 85,000–120,000/year  
- Rural/electric heating: 2–3× more expensive  
- Orkustofnun tariff reports: https://gogn.orkustofnun.is (PDF, lags by ~2 years)  
- Veitur (Reykjavík): publishes ISK rate sheets at veitur.is  
**Action:** Hardcode ISK 25.76/kWh for electricity. Use ISK 100,000/year geothermal heating for capital area, ISK 250,000/year for other. Flag for future refinement.

---

### R-13: State Pension (Ellilífeyrir)
**Status:** Found — complete  
**Source:** https://island.is/umsokn-um-ellilifeyri/fjarhaedir  
**2025/2026 amounts (pre-tax):**  
- Full pension: **ISK 365,592/month**  
- Half pension: ISK 182,796/month  
- Age supplement (aldursviðbót): ISK 32,917/month  
- Housing supplement (living alone): ISK 92,384/month  
- Income exemption: ISK 43,658/month (from Jan 2026)  
- Pension tapers at income above ~ISK 748,113/month (half) or ISK 856,085/month (full)  
**⚠ Note:** This is the TR first-pillar pension only. Most employees also receive occupational pension (lífeyrissjóður, 2nd pillar) — separate modeling needed.  
**Action:** Use ISK 365,592/month as base Retired-status transfer. Apply income tapering for high-income retirees.

---

### R-14: Disability Benefit (Örorkulífeyrir)
**Status:** Found — complete (new system Sept 2025)  
**Source:** https://island.is/en/new-disability-pensions  
**2025 amounts:**  
- Full disability pension: **ISK 416,950/month**  
- With housing supplement (living alone): ISK 486,076/month  
- Income threshold: ISK 105,200/month; 45% tapering above  
- Eligible: ages 18–67 with 0–25% labor capacity  
**⚠ Note:** System was fully redesigned September 1, 2025. Old örorkubætur data is not comparable.  
**Action:** Use ISK 416,950/month as Disabled-status transfer. Apply tapering for any capital/other income.

---

### R-15: Child Benefits (Barnabætur)
**Status:** Found — complete  
**Source:** https://www.skatturinn.is/einstaklingar/vaxtabaetur-og-barnabaetur/barnabaetur/  
**2026 figures:**  
- Single parent: **ISK 514,500/year per child** (ISK 42,875/month)  
- Couple: **ISK 345,000/year per child** (ISK 28,750/month)  
- Under-7 supplement: ISK 130,000/year per child  
- Income tapering: 4% reduction per child above ISK 5,844,000/year (single) or ISK 11,688,000/year (couple)  
- Paid 4×/year (Feb, May, Jun, Oct)  
**Action:** Implement as household-level transfer in Agent 6. Calculate per child under 18. Apply income taper.

---

### R-16: Seðlabanki Economic Projections
**Status:** Found — PDF format, must be manually extracted  
**Source:** https://cb.is/monetary-policy/central-bank-of-iceland-economic-forecasts/  
**Latest: Monetary Bulletin 2025/4 (Nov 2025):**  
- GDP growth: **1.6% (2025), 2.2% (2026)**  
- Inflation: 4.3% Oct 2025; expected to reach 2.5% target in H2 2026  
- Policy rate: **7.25%** (4th cut of 2025)  
- Unemployment: low; output gap closed  
**Format:** PDF bulletins published 4×/year  
**Action:** Manually extract projection values and populate `simulation_projections` table for Agent 7. Update each quarter when new Monetary Bulletin is published.

---

### R-17: Vehicle Registration / Car Ownership
**Status:** Found  
- `SAM03105.px` — registered vehicles by type, **1999–2025** (updated Mar 2026)  
  Path: `Umhverfi/5_samgongur/3_okutaekiogvegir/1_okutaeki/`  
  Category M1 = private passenger cars  
- `SAM03120.px` — by weight and fuel type  
- `SAM30111.px` — vehicle use by industry sector  
**⚠ Note:** SAM03105 gives total count, not per-household rate. Derive ownership rate: M1 count ÷ household count.  
**Action:** Download SAM03105. Divide M1 total by household count (from R-02) to get cars-per-household rate for Agent 5 expense assignment.

---

### R-18: Average Rent by Size / Municipality
**Status:** Found — hardcode from HMS report; no API  
**Source:** HMS Leiguverðsjá dashboard (https://hms.is/gogn-og-maelabord/maelabordleiguskra/leiguverdsja)  
**Feb 2026 confirmed figures:**  
- 1-room: ISK 190,843/month  
- 2-room: ISK 252,006/month  
- 3-room: ISK 294,344/month  
- Per m²: Reykjavík ISK 4,041 · Kópavogur ISK 3,810 · national average ISK 3,639  
**⚠ Limitation:** HMS site blocks automated access. Must be manually updated from browser or annual report.  
**Action:** Hardcode Feb 2026 figures as constants. Link to HMS annual report for manual updates.

---

## NICE TO HAVE — FOUND

### N-01: Unemployment Benefit
**Source:** https://island.is/en/unemployment-benefits-application/amounts-and-calculations-of-unemployment-benefits  
- Basic (100%): **ISK 383,870/month**  
- Income-related max: **ISK 605,159/month** (first 3 months)  
- Child supplement: ISK 15,355/month per child under 18  
- Maximum duration: 30 months; indexed to CPI  

### N-02: VAT Rates
**Source:** https://www.skatturinn.is/english/companies/value-added-tax/  
- Standard rate: **24%**  
- Reduced rate: **11%** — food, accommodation, passenger transport, books, pools, TV subscriptions  

### N-03: Corporate Tax Rate
**Source:** skatturinn.is  
- Limited companies (ehf/hf): **20%**  
- Partnerships: **37.6%**  
- Capital gains: 22%  

### N-04: Food Expenditure
**Source:** `VIS05303.px` — Path: `Efnahagur/visitolur/4a_neyslarannsokn/`  
⚠ Data only through 2016. Use CPI basket weights (R-11) + ISK 5,140/capita/year food estimate as proxy.  

### N-05: Healthcare Out-of-Pocket
**Source:** https://wp.mcc.is/wp-content/uploads/2025/06/heilbrigdisthjonusta-enska-1.pdf  
- GP visit: ISK 500–3,400  
- Monthly cap: **ISK 25,100** (adults) / ISK 16,700 (pensioners/children)  
- After cap: ISK 4,183/month  
- Dental: not covered for adults  

### N-06: Childcare Costs (Reykjavík)
**Source:** https://reykjavik.is/en/preschool/fees-and-subsidies  
- Standard: meal fee only = **ISK 15,290/month** for ≤36 hrs/week  
- Income discounts apply (40–75% off for lower incomes)  
- Collected 11 months/year  

### N-07: Household Savings Rate
**Source:** Hagstofa national accounts (þjóðhagsreikningar)  
- 2025 household disposable income per capita: ~ISK 6.4M  
- Savings growing faster than previously estimated (2025 preliminary)  
- PX path: `Efnahagur/thjodhagsreikningar/` — verify specific table  

### N-08: Migration Data
**Status:** Found  
- `MAN01401.px` — immigration/emigration by gender, single-year age, nationality, **1986–2025** (updated Mar 2026)  
  Path: `Ibuar/buferlaflutningar/buferlaflmillilanda/`  

---

## OPEN ITEMS FOR RESEARCHER

| ID | Item | Priority |
|---|---|---|
| 🔴 | Manually check menntasjodur.is in browser — get current student loan rate | High |
| 🔴 | Verify FYR09001.px exact API path (size class table) | High |
| 🟡 | Investigate gagnabanki.is for Seðlabanki rate historical series | Medium |
| 🟡 | Extract projection table from Monetary Bulletin 2025/4 PDF | Medium |
| 🟡 | Get HMS rental figures for non-capital municipalities from annual report | Medium |
| 🟢 | Confirm Hagstofa occupational pension (lífeyrissjóðir) contribution data | Low |
| 🟢 | Find national average childcare cost (non-Reykjavík) | Low |
