# QA Design Review: Population Validator

**Reviewer:** QA Engineer  
**Date:** 2026-04-18  
**File:** `qa/validate_population.py`  
**Runs:** `make validate` (after `make gold`)

---

## What Is Being Validated

The generated population in `silver.db:population` is compared to the official statistical distributions stored in the same database. The validator does not access the internet — it works entirely from silver.db, which is the single source of truth for official statistics.

---

## Checks

### Check 1 — Age Distribution
**Source:** `population_distribution` (Hagstofa MAN00101, population by single-year age, 2024)  
**Comparison:** Total headcount and per-age share of generated vs official  
**Tolerances:**
- Total count: ±0.5% — population generation should reproduce the exact age × count counts, so tight tolerance is appropriate
- Per-age share (mean absolute deviation): 5% — allows for noise in random sampling

**Why 5% on per-age share:** With ~370K people and generation by random choice, law of large numbers keeps per-age deviation small. If this fails, something is wrong in the sampling loop (wrong age range, wrong population count from source).

---

### Check 2 — Gender Distribution
**Source:** Implicit — no official gender-split total population table in silver (MAN00101 uses "Total" only)  
**Comparison:** Generated male share vs 50% baseline  
**Tolerance:** ±2%

**Limitation:** The true Icelandic male/female split is approximately 49.9/50.1 (nearly equal). Using 50% as baseline is a safe approximation. If gender-split population data is added to silver in a future sprint, update this check to use real official figures.

**Future improvement (R-QA-01):** Add MAN00102 or similar table that has population by gender to silver, and update this check.

---

### Check 3 — Sector (NACE) Distribution Among Employed
**Source:** `sector_employment` (Hagstofa VIN10022, employment by sector × age × gender, 12-month average)  
**Comparison:** Generated sector shares among Employed persons vs official employment shares, per gender  
**Tolerance:** ±8% mean absolute deviation per sector

**Why 8%:** The sector assignment uses age-based boost factors that intentionally deviate from the overall distribution (young workers weighted toward service, older toward management). This creates legitimate divergence from the aggregate VIN10022 shares. 8% allows for this age-stratification effect while catching gross mis-assignments.

**SKIP condition:** If `sector_employment` table is not in silver (new data sources not yet downloaded), this check is skipped with a clear message.

---

### Check 4 — Occupation Class Distribution (Soft Constraints)
**Source:** None — no authoritative ISCO major group distribution for Iceland exists in silver  
**Comparison:** Soft bounds: no single class > 40%, no class < 0.5%  
**Rationale:** Without a definitive official source for occupation class distribution, we check plausibility bounds. A single class dominating > 40% of employed persons would indicate a bug in `_SECTOR_OCCUPATION_WEIGHTS`. A class below 0.5% would indicate it is being assigned too rarely to be meaningful.

**Future improvement (R-QA-02):** If Hagstofa publishes ISCO class employment counts for Iceland, add them to silver and update this check to use real calibration targets.

---

### Check 5 — Income Distribution by Age Group
**Source:** `gender_and_age_income_distribution` (Hagstofa TEK01001, means by age group × gender, 2023)  
**Comparison:** Mean total income, wages, and capital gains in each TEK01001 age group vs official  
**Tolerances:**
- Total income: ±10%
- Wages: ±15% (occupation-based assignment adds legitimate variance vs the TEK01001 all-worker average)
- Capital gains: ±15%

**Why wages tolerance is wider than total income:** TEK01001 Atvinnutekjur is the mean for ALL persons (including non-employed), while VIN02003 wages are for full-time workers only. The generated population assigns wages=0 for non-employed, which should bring the age-group mean down toward the TEK01001 figure. The 15% tolerance accommodates the imperfect conversion between full-time wages and population-average wages.

**Age group mapping:** Generated population has single-year ages; TEK01001 has groups (16, 20, 25, ..., 85+). Generated persons are bucketed into the same groups for comparison.

---

## Exit Code Behaviour

The script exits 0 if all non-skipped checks pass, exits 1 if any check fails. This allows `make validate` to stop the pipeline on failure when run in CI.

---

## Known Limitations at Time of Writing

1. **Gender check uses 50% baseline** — should be updated to use official gender-split population table (see R-QA-01)
2. **Occupation class check uses soft bounds only** — no official ISCO distribution in silver (see R-QA-02)
3. **Sector label matching** — sector labels from VIN10022 and VIN02003 must match exactly for the wage lookup in generate_population.py. A mismatch falls through to occupation-class fallback wages. The sector check in the validator will still pass if sectors are assigned correctly but wages come from fallback. A separate check on wage levels by sector would catch this.
4. **Income comparison uses 2023 TEK01001 vs population generated for 2024 demographic base** — one year lag is acceptable, flag if TEK01001 is updated to 2024.

---

## Sign-off

This validator is approved for use after `make gold`. Add new checks as new data sources are added to silver. Do not remove or widen tolerances without a recorded reason in lessons_learned.md.
