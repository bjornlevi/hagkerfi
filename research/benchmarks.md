# Calibration Benchmarks

**Owner:** Researcher  
**Last updated:** 2026-04-18  
**Purpose:** Official statistics that the simulation output must match, used by QA for calibration checks after each agent.

Each benchmark lists the target value, the official source, the year, and acceptable error tolerance.

---

## Agent 2 — Household Formation

| Metric | Value | Source | Year | Tolerance |
|---|---|---|---|---|
| Total households | ~115,000 | Hagstofa CEN1514 / MAN07108 | 2021/2026 | ±2% |
| Owner-occupied | 78% | Hagstofa CEN1706 | 2021 | ±3% |
| Rental | 22% | Hagstofa CEN1706 | 2021 | ±3% |
| Avg household size | ~2.5 persons | Hagstofa CEN1514 | 2021 | ±0.1 |
| Single-person households | ~35% | Hagstofa MAN07108 | 2026 | ±3% |
| Households with children | ~30% | Hagstofa MAN07108 | 2026 | ±3% |

---

## Agent 3 — Businesses & Employment

| Metric | Value | Source | Year | Tolerance |
|---|---|---|---|---|
| Total registered firms | ~65,000 | Hagstofa FYR01001 | 2025 | ±5% |
| Employment rate (16–74) | ~80% | Hagstofa VIN10001 | 2024 | ±2% |
| Employment by sector — match distribution | See VIN10022 | Hagstofa VIN10022 | 2024 | ±5% per sector |

---

## Agent 4 — Finance

| Metric | Value | Source | Year | Tolerance |
|---|---|---|---|---|
| Household debt-to-GDP | 71.4% | Seðlabanki / CEIC | Dec 2024 | ±5% |
| Household debt distribution | See THJ09010 | Hagstofa THJ09010 | 2024 | ±10% |
| Indexed mortgage rate | ~4.0% | Seðlabanki gagnatorg | Nov 2025 | ±0.5% |
| Non-indexed mortgage rate | ~8.6% | Seðlabanki gagnatorg | Nov 2025 | ±0.5% |

---

## Agent 5 — Expenses

| Metric | Value | Source | Year | Tolerance |
|---|---|---|---|---|
| Housing share of CPI basket | See VIS01306 | Hagstofa VIS01306 | 2026 | ±2% |
| Food share of CPI basket | See VIS01306 | Hagstofa VIS01306 | 2026 | ±2% |
| Transport share of CPI basket | See VIS01306 | Hagstofa VIS01306 | 2026 | ±2% |
| Avg rent 1-room | ISK 190,843/mo | HMS Leiguverðsjá | Feb 2026 | ±10% |
| Avg rent 2-room | ISK 252,006/mo | HMS Leiguverðsjá | Feb 2026 | ±10% |
| Avg rent 3-room | ISK 294,344/mo | HMS Leiguverðsjá | Feb 2026 | ±10% |
| Electricity per kWh | ISK 25.76 | GlobalPetrolPrices / Eurostat | Sep 2025 | ±5% |

---

## Agent 6 — Government

| Metric | Value | Source | Year | Tolerance |
|---|---|---|---|---|
| Total income tax revenue | See fjarlog_2026 | National budget 2026 | 2026 | ±5% |
| Total municipal tax revenue | See arsreikningar | Municipal accounts | Latest | ±5% |
| State pension (full) | ISK 365,592/mo | island.is / TR | 2026 | exact |
| Disability pension (full) | ISK 416,950/mo | island.is / TR | Sep 2025 | exact |
| Child benefit (couple) | ISK 28,750/mo per child | skatturinn.is | 2026 | exact |
| Child benefit (single parent) | ISK 42,875/mo per child | skatturinn.is | 2026 | exact |
| Unemployment benefit (100%) | ISK 383,870/mo | island.is | 2025 | exact |
| VAT standard rate | 24% | skatturinn.is | 2025 | exact |
| VAT reduced rate | 11% | skatturinn.is | 2025 | exact |
| Corporate tax rate | 20% | skatturinn.is | 2025 | exact |

---

## Agent 7 — Time Engine (Projection Inputs)

| Parameter | Value | Source | Period |
|---|---|---|---|
| GDP growth | 1.6% (2025), 2.2% (2026) | Seðlabanki MB 2025/4 | Annual |
| CPI inflation | 4.3% (Oct 2025) → 2.5% target H2 2026 | Seðlabanki MB 2025/4 | Annual |
| Policy rate | 7.25% | Seðlabanki | Nov 2025 |
| Non-indexed mortgage rate | ~8.6% | Seðlabanki | Nov 2025 |
| Indexed mortgage rate | ~4.0% | Seðlabanki | Nov 2025 |

---

## Key Rates & Constants

| Constant | Value | Source |
|---|---|---|
| Personal tax credit | ISK 779,112/year | calculate_taxes.py (confirmed) |
| Income tax brackets | 31.48% / 37.98% / 46.28% | calculate_taxes.py (confirmed) |
| Capital gains tax | 22% | calculate_taxes.py (confirmed) |
| Capital gains exemption | ISK 300,000/year | calculate_taxes.py (confirmed) |
| Radio fee | ISK 20,900/year | calculate_taxes.py (confirmed) |
| Elderly fund fee | ISK 13,749/year | calculate_taxes.py (confirmed) |
| Municipal tax avg | ~14.94% | utsvar_sveitarfelaga.xls (confirmed) |
| Electricity price | ISK 25.76/kWh | Eurostat / GlobalPetrolPrices Sep 2025 |
| Geothermal heating (capital) | ~ISK 100,000/year | Orkustofnun estimates |
| Electric heating (rural) | ~ISK 250,000/year | Orkustofnun estimates |
