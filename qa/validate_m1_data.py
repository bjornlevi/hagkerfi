"""
QA: M1 Data Integration Validator
===================================
Validates that all M1 tables were correctly downloaded, parsed, and loaded
into silver.duckdb.  Checks table existence, row counts, column presence,
value ranges, and known spot-values for the hardcoded constants tables.

Run:  DB_PATH=data/silver.duckdb python qa/validate_m1_data.py
"""

import sys
import textwrap
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers import get_db_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def _load(conn: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame | None:
    try:
        return conn.execute(f"SELECT * FROM {table}").df()
    except Exception:
        return None


def _check(name: str, passed: bool, note: str = "", detail: str = "") -> dict:
    return {"metric": name, "pass": passed, "note": note, "detail": detail}


def _fail(name: str, note: str = "", detail: str = "") -> dict:
    return _check(name, False, note, detail)


def _pass(name: str, note: str = "") -> dict:
    return _check(name, True, note)


# ---------------------------------------------------------------------------
# Table existence + minimum row counts
# ---------------------------------------------------------------------------

M1_TABLES = {
    # API tables
    "sector_employment":          10,
    "wages_by_sector_occupation": 50,
    "wage_income_distribution":   100,
    "total_income_distribution":  100,
    "withholding_payments":       50,
    "births_by_mother_age":       30,
    "life_tables":                60,
    "household_types":            100,
    "housing_census_dwellings":   50,
    "marital_status":             500,
    "migration":                  500,
    "business_registry":          1000,
    "business_size_classes":      50,
    "household_debt":             50,
    "household_debt_ratio":       20,
    "cpi_basket_weights":         50,
    "vehicle_registrations":      5,
    # Constants tables
    "mortgage_rates":             3,
    "transfer_amounts":           10,
    "housing_rental_rates":       3,
    "student_loan_terms":         2,
    "simulation_projections":     2,
    "housing_costs":              3,
    "supplementary_tax_rates":    3,
}


def check_table_existence(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    existing = set(conn.execute("SHOW TABLES").df()["name"].tolist())
    for table, min_rows in M1_TABLES.items():
        if table not in existing:
            results.append(_fail(f"Table '{table}' exists", note="Table missing from silver.duckdb"))
            continue
        n = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        if n < min_rows:
            results.append(_fail(f"Table '{table}' min rows ≥ {min_rows}",
                                 note=f"Only {n} rows found"))
        else:
            results.append(_pass(f"Table '{table}' exists ({n} rows)"))
    return {"name": "Table existence & row counts", "checks": results}


# ---------------------------------------------------------------------------
# Schema / column presence
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = {
    "sector_employment":          ["sector", "gender", "employment_share"],
    "wages_by_sector_occupation": ["sector", "occupational_class", "gender", "mean_monthly_wage_isk"],
    "wage_income_distribution":   ["gender", "age_group", "percentile", "value_isk"],
    "total_income_distribution":  ["gender", "age_group", "percentile", "value_isk"],
    "withholding_payments":       ["gender", "age_group", "payment_type", "monthly_isk_per_person"],
    "births_by_mother_age":       ["age_of_mother", "births"],
    "life_tables":                ["gender", "age", "survivors", "death_prob"],
    "household_types":            ["family_type", "count"],
    "housing_census_dwellings":   ["municipality", "dwelling_status", "count"],
    "marital_status":             ["age", "gender", "marital_class", "count"],
    "migration":                  ["gender", "age", "migration_type", "count"],
    "business_registry":          ["sector", "legal_form", "firm_count"],
    "business_size_classes":      ["sector", "size_class", "count"],
    "household_debt":             ["year", "value_isk_thousands"],
    "household_debt_ratio":       ["year", "value_isk_thousands"],
    "cpi_basket_weights":         ["category", "weight"],
    "vehicle_registrations":      ["vehicle_type", "count"],
    "mortgage_rates":             ["loan_type", "annual_rate"],
    "transfer_amounts":           ["transfer_type", "monthly_isk"],
    "housing_rental_rates":       ["room_count", "monthly_isk", "municipality"],
    "student_loan_terms":         ["term", "value"],
    "simulation_projections":     ["year", "gdp_growth", "inflation_cpi"],
    "housing_costs":              ["cost_type", "isk"],
    "supplementary_tax_rates":    ["tax", "rate"],
}


def check_schemas(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    existing = set(conn.execute("SHOW TABLES").df()["name"].tolist())
    for table, cols in REQUIRED_COLUMNS.items():
        if table not in existing:
            continue
        actual = set(conn.execute(f"DESCRIBE {table}").df()["column_name"].tolist())
        missing = [c for c in cols if c not in actual]
        if missing:
            results.append(_fail(f"'{table}' required columns present",
                                 note=f"Missing: {missing}"))
        else:
            results.append(_pass(f"'{table}' columns OK"))
    return {"name": "Column schemas", "checks": results}


# ---------------------------------------------------------------------------
# Value range sanity checks
# ---------------------------------------------------------------------------

def check_life_tables(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "life_tables")
    if df is None:
        return {"name": "life_tables sanity", "checks": [_fail("life_tables loaded")]}

    # death_prob must be in [0, 1]
    oob = df[(df["death_prob"] < 0) | (df["death_prob"] > 1)]
    results.append(
        _pass("death_prob in [0, 1]") if oob.empty
        else _fail("death_prob in [0, 1]", note=f"{len(oob)} out-of-range rows")
    )

    # At young ages (1-10), Iceland mortality is very low — expect < 0.005
    young = df[df["age"].between(1, 10)]
    high_young = young[young["death_prob"] > 0.005]
    results.append(
        _pass("Young-age death_prob < 0.5% (ages 1–10)")
        if high_young.empty
        else _fail("Young-age death_prob < 0.5% (ages 1–10)",
                   note=f"{len(high_young)} ages have implausibly high mortality",
                   detail=f"Max: {young['death_prob'].max():.4f} — expected < 0.005 for Iceland ages 1–10; "
                          f"suspect life expectancy column was selected instead of survivors column")
    )

    # At old age (80+), death_prob should be > 0.03
    old = df[df["age"] >= 80]
    if not old.empty:
        low_old = old[old["death_prob"] < 0.03]
        results.append(
            _pass("Old-age death_prob > 3% (age 80+)")
            if low_old.empty
            else _fail("Old-age death_prob > 3% (age 80+)",
                       note=f"{len(low_old)} ages have implausibly low old-age mortality")
        )

    # Both genders present
    genders = set(df["gender"].unique())
    results.append(
        _pass("Both Male and Female in life_tables")
        if {"Male", "Female"}.issubset(genders)
        else _fail("Both Male and Female in life_tables", note=f"Found: {genders}")
    )

    return {"name": "life_tables sanity", "checks": results}


def check_births(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "births_by_mother_age")
    if df is None:
        return {"name": "births_by_mother_age sanity", "checks": [_fail("table loaded")]}

    results.append(_pass("births ≥ 0") if (df["births"] >= 0).all()
                   else _fail("births ≥ 0", note="Negative birth counts found"))

    # Check for duplicate age rows
    dups = df.groupby("age_of_mother").size()
    dup_ages = dups[dups > 1]
    results.append(
        _pass("No duplicate age_of_mother rows")
        if dup_ages.empty
        else _fail("No duplicate age_of_mother rows",
                   note=f"{len(dup_ages)} ages have >1 row: {list(dup_ages.index)}",
                   detail="Likely cause: Eining filter missed 'Öll börn'/'Frumburðir' split "
                          "and/or 'Innan 15 ára' + '15 ára' both parsed as age 15")
    )

    # Peak births should be ages 25–35
    peak = df.loc[df["births"].idxmax(), "age_of_mother"] if not df.empty else None
    results.append(
        _pass(f"Peak births at plausible age (25–35), found age {peak}")
        if peak is not None and 25 <= peak <= 35
        else _fail("Peak births at plausible age (25–35)",
                   note=f"Peak found at age {peak}")
    )

    return {"name": "births_by_mother_age sanity", "checks": results}


def check_sector_employment(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "sector_employment")
    if df is None:
        return {"name": "sector_employment sanity", "checks": [_fail("table loaded")]}

    # Shares should sum to ~1.0 per gender × age_group
    sums = df.groupby(["gender", "age_group"])["employment_share"].sum()
    off = sums[abs(sums - 1.0) > 0.05]
    results.append(
        _pass("Sector shares sum to ~1.0 per gender × age_group")
        if off.empty
        else _fail("Sector shares sum to ~1.0 per gender × age_group",
                   note=f"{len(off)} groups deviate >5% from 1.0",
                   detail=str(off.head(5)))
    )

    # employment_share ≥ 0
    neg = df[df["employment_share"] < 0]
    results.append(_pass("employment_share ≥ 0") if neg.empty
                   else _fail("employment_share ≥ 0", note=f"{len(neg)} negative rows"))

    return {"name": "sector_employment sanity", "checks": results}


def check_wages(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "wages_by_sector_occupation")
    if df is None:
        return {"name": "wages_by_sector_occupation sanity", "checks": [_fail("table loaded")]}

    # mean_monthly_wage_isk should be > 0 and < 10,000,000 (reasonable ISK range)
    pos = df[df["mean_monthly_wage_isk"] > 0]
    high = df[df["mean_monthly_wage_isk"] > 10_000_000]
    results.append(
        _pass(f"mean_monthly_wage_isk positive ({len(pos)}/{len(df)} rows)")
        if len(pos) >= len(df) * 0.8
        else _fail("mean_monthly_wage_isk mostly positive")
    )
    results.append(
        _pass("mean_monthly_wage_isk < 10M ISK")
        if high.empty
        else _fail("mean_monthly_wage_isk < 10M ISK",
                   note=f"{len(high)} implausibly high values")
    )

    # Both genders present
    genders = set(df["gender"].unique())
    results.append(
        _pass("Both genders in wages table")
        if {"Male", "Female"}.issubset(genders)
        else _fail("Both genders in wages table", note=f"Found: {genders}")
    )
    return {"name": "wages_by_sector_occupation sanity", "checks": results}


def check_income_distributions(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    for tbl in ("wage_income_distribution", "total_income_distribution"):
        df = _load(conn, tbl)
        if df is None:
            results.append(_fail(f"{tbl} loaded"))
            continue
        # Both genders
        genders = set(df["gender"].unique())
        results.append(
            _pass(f"{tbl}: both genders")
            if {"Male", "Female"}.issubset(genders)
            else _fail(f"{tbl}: both genders", note=f"Found: {genders}")
        )
        # value_isk ≥ 0
        neg = df[df["value_isk"] < 0]
        results.append(
            _pass(f"{tbl}: value_isk ≥ 0")
            if neg.empty
            else _fail(f"{tbl}: value_isk ≥ 0", note=f"{len(neg)} negative values")
        )
        # Percentile column has expected marker for conditional mean
        has_cond_mean = (df["percentile"] == -1).any()
        results.append(
            _pass(f"{tbl}: conditional mean row (percentile=-1) present")
            if has_cond_mean
            else _fail(f"{tbl}: conditional mean row (percentile=-1) present",
                       note="No percentile=-1 row; may affect wage sampling")
        )
    return {"name": "Income distribution tables", "checks": results}


def check_withholding_payments(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "withholding_payments")
    if df is None:
        return {"name": "withholding_payments sanity", "checks": [_fail("table loaded")]}

    neg = df[df["monthly_isk_per_person"] < 0]
    results.append(_pass("monthly_isk_per_person ≥ 0") if neg.empty
                   else _fail("monthly_isk_per_person ≥ 0", note=f"{len(neg)} negative rows"))

    # Key payment types should be present
    expected_types = ["Launagreiðslur", "Lífeyrisgreiðslur"]
    found_types = set(df["payment_type"].unique())
    missing = [t for t in expected_types if not any(t in ft for ft in found_types)]
    results.append(
        _pass(f"Key payment types present: {expected_types}")
        if not missing
        else _fail(f"Key payment types present", note=f"Missing: {missing}")
    )
    return {"name": "withholding_payments sanity", "checks": results}


def check_mortgage_rates(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "mortgage_rates")
    if df is None:
        return {"name": "mortgage_rates spot-check", "checks": [_fail("table loaded")]}

    loan_map = df.set_index("loan_type")["annual_rate"].to_dict()
    expected = {
        "non_indexed": (0.07, 0.12),   # ~8.6% non-indexed
        "indexed":     (0.02, 0.07),   # ~4.0% indexed
        "policy_rate": (0.05, 0.10),   # ~7.25% policy rate
    }
    for loan, (lo, hi) in expected.items():
        rate = loan_map.get(loan)
        if rate is None:
            results.append(_fail(f"mortgage_rates.{loan} present"))
        elif not (lo <= rate <= hi):
            results.append(_fail(f"mortgage_rates.{loan} in [{_pct(lo)}, {_pct(hi)}]",
                                 note=f"Got {_pct(rate)}"))
        else:
            results.append(_pass(f"mortgage_rates.{loan} = {_pct(rate)}"))
    return {"name": "mortgage_rates spot-check", "checks": results}


def check_transfer_amounts(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "transfer_amounts")
    if df is None:
        return {"name": "transfer_amounts spot-check", "checks": [_fail("table loaded")]}

    t_map = df.set_index("transfer_type")["monthly_isk"].to_dict()
    # State pension full = 365,592 ISK/month (TR Jan 2026)
    pension = t_map.get("state_pension_full")
    results.append(
        _pass(f"state_pension_full = {pension:,.0f} ISK/mo")
        if pension and 350_000 <= pension <= 400_000
        else _fail("state_pension_full in [350k, 400k] ISK/mo",
                   note=f"Got {pension}")
    )
    # Disability full ≥ state pension (disability is more generous)
    disability = t_map.get("disability_pension_full")
    results.append(
        _pass(f"disability_pension_full ({disability:,.0f}) ≥ state_pension_full ({pension:,.0f})")
        if disability and pension and disability >= pension
        else _fail("disability_pension_full ≥ state_pension_full",
                   note=f"disability={disability}, pension={pension}")
    )
    # All transfer amounts positive
    neg = df[df["monthly_isk"] <= 0]
    results.append(_pass("All transfer_amounts > 0") if neg.empty
                   else _fail("All transfer_amounts > 0",
                               note=f"{len(neg)} zero/negative entries: {list(neg['transfer_type'])}"))
    return {"name": "transfer_amounts spot-check", "checks": results}


def check_housing_rental_rates(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "housing_rental_rates")
    if df is None:
        return {"name": "housing_rental_rates spot-check", "checks": [_fail("table loaded")]}

    room_map = df[df["room_count"].notna()].set_index("room_count")["monthly_isk"].to_dict()
    for rooms, expected in [(1, 190_843), (2, 252_006), (3, 294_344)]:
        got = room_map.get(rooms)
        if got is None:
            results.append(_fail(f"{rooms}-room rent present"))
        elif abs(got - expected) > 10_000:
            results.append(_fail(f"{rooms}-room monthly_isk ≈ {expected:,}",
                                 note=f"Got {got:,}"))
        else:
            results.append(_pass(f"{rooms}-room rent = {got:,.0f} ISK/mo"))
    return {"name": "housing_rental_rates spot-check", "checks": results}


def check_simulation_projections(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "simulation_projections")
    if df is None:
        return {"name": "simulation_projections spot-check", "checks": [_fail("table loaded")]}

    years = set(df["year"].unique())
    results.append(
        _pass(f"Years 2025 and 2026 present")
        if {2025, 2026}.issubset(years)
        else _fail("Years 2025 and 2026 present", note=f"Found: {sorted(years)}")
    )

    row_2026 = df[df["year"] == 2026]
    if not row_2026.empty:
        inf = float(row_2026["inflation_cpi"].iloc[0])
        results.append(
            _pass(f"2026 CPI target ~2.5% (got {_pct(inf)})")
            if 0.015 <= inf <= 0.04
            else _fail(f"2026 CPI target in [1.5%, 4%]", note=f"Got {_pct(inf)}")
        )
    return {"name": "simulation_projections spot-check", "checks": results}


def check_supplementary_tax_rates(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "supplementary_tax_rates")
    if df is None:
        return {"name": "supplementary_tax_rates spot-check", "checks": [_fail("table loaded")]}

    oob = df[(df["rate"] < 0) | (df["rate"] > 1)]
    results.append(
        _pass("All rates in [0, 1]") if oob.empty
        else _fail("All rates in [0, 1]", note=f"{len(oob)} out-of-range: {list(oob['tax'])}")
    )

    tax_map = df.set_index("tax")["rate"].to_dict()
    expected = {"vat_standard": 0.24, "vat_reduced": 0.11, "corporate_ltd": 0.20}
    for tax, rate in expected.items():
        got = tax_map.get(tax)
        results.append(
            _pass(f"{tax} = {_pct(rate)}")
            if got is not None and abs(got - rate) < 0.001
            else _fail(f"{tax} = {_pct(rate)}", note=f"Got {got}")
        )
    return {"name": "supplementary_tax_rates spot-check", "checks": results}


def check_cpi_weights(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "cpi_basket_weights")
    if df is None:
        return {"name": "cpi_basket_weights sanity", "checks": [_fail("table loaded")]}

    neg = df[df["weight"] < 0]
    results.append(_pass("All weights ≥ 0") if neg.empty
                   else _fail("All weights ≥ 0", note=f"{len(neg)} negative weights"))

    # Top-level total should be ~10,000 (index base)
    top_row = df[df["category"].str.lower().str.contains("vísitala neyslu|consumer price", na=False)]
    if not top_row.empty:
        total = float(top_row["weight"].iloc[0])
        results.append(
            _pass(f"CPI index base ≈ 10,000 (got {total:.0f})")
            if abs(total - 10_000) < 500
            else _fail(f"CPI index base ≈ 10,000", note=f"Got {total:.0f}")
        )
    return {"name": "cpi_basket_weights sanity", "checks": results}


def check_migration(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "migration")
    if df is None:
        return {"name": "migration sanity", "checks": [_fail("table loaded")]}

    # Should have both immigration and emigration
    types = set(df["migration_type"].unique())
    results.append(
        _pass(f"Migration types present: {types}")
        if len(types) >= 2
        else _fail("At least 2 migration types (in/out)", note=f"Found: {types}")
    )
    # Both genders
    genders = set(df["gender"].unique())
    results.append(
        _pass("Both genders in migration")
        if {"Male", "Female"}.issubset(genders)
        else _fail("Both genders in migration", note=f"Found: {genders}")
    )
    return {"name": "migration sanity", "checks": results}


def check_vehicle_registrations(conn: duckdb.DuckDBPyConnection) -> dict:
    results = []
    df = _load(conn, "vehicle_registrations")
    if df is None:
        return {"name": "vehicle_registrations sanity", "checks": [_fail("table loaded")]}

    # Private cars (M1 category) should be ~240k-260k
    m1 = df[df["vehicle_type"].str.contains("M1|Fólksbifreiðar|sérbifreiðar", na=False)]
    if not m1.empty:
        count = int(m1["count"].sum())
        results.append(
            _pass(f"M1 private cars ≈ 249k (got {count:,})")
            if 200_000 <= count <= 300_000
            else _fail(f"M1 private cars in [200k, 300k]", note=f"Got {count:,}")
        )
    else:
        results.append(_fail("M1 (private car) vehicle_type found",
                             note=f"Types present: {list(df['vehicle_type'].unique())[:5]}"))
    return {"name": "vehicle_registrations sanity", "checks": results}


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def _render(groups: list[dict], run_at: str) -> tuple[str, int, int, int]:
    lines = [
        "=" * 72,
        "  HAGKERFI — M1 Data Integration QA Report",
        f"  {run_at}",
        "=" * 72, "",
    ]
    n_pass = n_fail = n_skip = 0
    for grp in groups:
        lines.append(f"  ── {grp['name']}")
        for chk in grp["checks"]:
            status = chk.get("pass")
            if status is None:
                icon = "⬜ SKIP"; n_skip += 1
            elif status:
                icon = "✅ PASS"; n_pass += 1
            else:
                icon = "❌ FAIL"; n_fail += 1
            lines.append(f"     {icon}  {chk['metric']}")
            if chk.get("note"):
                lines.append(f"            {chk['note']}")
            if chk.get("detail"):
                for dl in textwrap.wrap(chk["detail"], width=64):
                    lines.append(f"            {dl}")
        lines.append("")
    lines += [
        "=" * 72,
        f"  Results:  {n_pass} passed  {n_fail} failed  {n_skip} skipped",
        "=" * 72,
    ]
    return "\n".join(lines), n_pass, n_fail, n_skip


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> bool:
    conn = duckdb.connect(get_db_path())
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    groups = [
        check_table_existence(conn),
        check_schemas(conn),
        check_life_tables(conn),
        check_births(conn),
        check_sector_employment(conn),
        check_wages(conn),
        check_income_distributions(conn),
        check_withholding_payments(conn),
        check_cpi_weights(conn),
        check_migration(conn),
        check_vehicle_registrations(conn),
        check_mortgage_rates(conn),
        check_transfer_amounts(conn),
        check_housing_rental_rates(conn),
        check_simulation_projections(conn),
        check_supplementary_tax_rates(conn),
    ]
    conn.close()

    report, n_pass, n_fail, n_skip = _render(groups, run_at)
    print(report)

    out = Path("qa/m1_validation_report.txt")
    out.write_text(report)
    print(f"\nReport saved to {out}")
    return n_fail == 0


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
