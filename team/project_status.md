# Project Status

**Project:** Hagkerfi — Iceland Economic Simulation  
**Phase:** 0 — Initialization  
**Date:** 2026-04-19  
**PM:** Project Manager

---

## Milestone Plan

| Milestone | Description | Target | Status |
|---|---|---|---|
| M0 | Team initialized, data requirements compiled | 2026-04-18 | ✅ Complete |
| M1 | All data sources located and integrated into pipeline | 2026-04-19 | ✅ Complete |
| M2 | Static world built (households, businesses, finance) | TBD | ⬜ Not started |
| M3 | First monthly tick runs end-to-end | TBD | ⬜ Not started |
| M4 | 12-month simulation stable, calibrated | TBD | ⬜ Not started |
| M5 | Scenario comparison works, reporting complete | TBD | ⬜ Not started |

---

## Current Sprint: M2 — Static World

### Completed (M0)
- [x] Team charter defined
- [x] Role responsibilities assigned
- [x] Folder structure created (`team/`, `research/`, `docs/`, `qa/design_reviews/`)
- [x] Data requirements compiled from all roles → `research/data_requirements.md`
- [x] Researcher survey complete → `research/data_catalog.md`
- [x] Calibration benchmarks established → `research/benchmarks.md`

### Completed (M1)
- [x] Data Engineer: add 12 new Hagstofa PX-API sources to landing pipeline (sector_employment, wages_by_sector_occupation, wage_income_distribution, total_income_distribution, withholding_payments, housing_census_dwellings, household_types, marital_status, births_by_mother_age, life_tables, business_registry, business_size_classes, household_debt, household_debt_ratio, cpi_basket_weights, vehicle_registrations, migration)
- [x] Data Engineer: hardcode HMS rental constants (`housing_rental_rates` table in silver)
- [x] Data Engineer: hardcode Seðlabanki rate constants (`mortgage_rates`, `simulation_projections` tables in silver)
- [x] Data Engineer: hardcode TR/island.is transfer amounts (`transfer_amounts` table in silver)
- [x] Data Engineer: hardcode housing utility costs (`housing_costs` table in silver)
- [x] Data Engineer: hardcode supplementary tax rates (`supplementary_tax_rates` table in silver)
- [x] Researcher: verified FYR09001.px correct path (`Atvinnuvegir/fyrirtaeki/lydfraedi/`, latest year = 2020)
- [x] Researcher: extracted Monetary Bulletin 2025/4 projections (GDP 1.6%/2.2%, CPI to 2.5% by H2 2026)
- [ ] Researcher: check menntasjodur.is in browser for student loan rate (⚠ currently using CPI+1%=5.3% proxy)

### In Progress (M2)
- [ ] Architect: design household formation algorithm (marital_status + household_types → household roster)
- [ ] Data Engineer: implement household formation using marital_status and household_types data in silver
- [ ] Data Engineer: assign dwellings using housing_census_dwellings (owned vs rented)
- [ ] Data Engineer: assign mortgage/rent expenses using mortgage_rates + housing_rental_rates
- [ ] Data Engineer: assign businesses using business_registry + business_size_classes
- [ ] QA: define M2 calibration targets (household size distribution, homeownership rate, debt-to-income distribution)

### Blocked
_Nothing blocked._

### Open Questions
- Occupational pension (lífeyrissjóðir) contribution rates: not in silver yet. Rough rule (4% employee + 8% employer) could be hardcoded until Landssamtök lífeyrissjóða data is available.
- Student loan rate: menntasjodur.is website blocked automation; researcher to manually verify and update `student_loan_terms` table.

---

## Decisions Log

| Date | Decision | Owner | Rationale |
|---|---|---|---|
| 2026-04-18 | Use SQLite for all phases, evaluate DuckDB if tick performance degrades | Architect + DBA | Keeps stack simple; decision point at M3 |
| 2026-04-18 | Household is the unit of financial simulation, not the individual | Architect | 370K individuals × monthly cashflow is tractable at household level |
| 2026-04-18 | Separate projections table from tick mechanics | Architect | Enables manual vs projected scenario comparison cleanly |
