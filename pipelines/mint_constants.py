"""
Write hardcoded economic constants into silver.duckdb as lookup tables.
Run after mint_silver.py.  Sources are manually extracted from official
publications that do not have a machine-readable API (Seðlabanki, TR, HMS, LÍN).
Update values here whenever new publications are released.
"""

import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers import get_db_path

# ---------------------------------------------------------------------------
# Source documentation
# ---------------------------------------------------------------------------
# mortgage_rates      — Seðlabanki.is, Nov 2025 (Blazor app, no API)
# transfer_amounts    — island.is / TR / skatturinn.is, Jan 2026 rates
# housing_rental_rates — HMS Leiguverðsjá dashboard, Feb 2026 (Vercel JS, no API)
# student_loan_terms  — Menntasjóður námsmanna; website blocked automation;
#                       rate = CPI + 1% proxy until confirmed manually
# simulation_projections — Seðlabanki Monetary Bulletin 2025/4, Nov 2025 (PDF)
# ---------------------------------------------------------------------------


def write_mortgage_rates(conn: duckdb.DuckDBPyConnection) -> None:
    df = pd.DataFrame([
        {"loan_type": "non_indexed", "annual_rate": 0.0860,
         "description": "Óverðtryggð lán — non-indexed mortgage, Nov 2025"},
        {"loan_type": "indexed",     "annual_rate": 0.0400,
         "description": "Verðtryggð lán — CPI-indexed mortgage, Nov 2025"},
        {"loan_type": "policy_rate", "annual_rate": 0.0725,
         "description": "Seðlabankavextir — central bank policy rate, Nov 2025"},
    ])
    conn.execute("CREATE OR REPLACE TABLE mortgage_rates AS SELECT * FROM df")
    print(f"Constants: mortgage_rates ({len(df)} rows)")


def write_transfer_amounts(conn: duckdb.DuckDBPyConnection) -> None:
    df = pd.DataFrame([
        # State pension (ellilífeyrir) — TR, Jan 2026
        {"transfer_type": "state_pension_full",          "monthly_isk": 365_592,
         "notes": "Full ellilífeyrir, pre-tax, Jan 2026"},
        {"transfer_type": "state_pension_age_supplement", "monthly_isk": 32_917,
         "notes": "Aldursviðbót supplement, Jan 2026"},
        {"transfer_type": "state_pension_housing",        "monthly_isk": 92_384,
         "notes": "Housing supplement for those living alone, Jan 2026"},
        {"transfer_type": "state_pension_income_exempt",  "monthly_isk": 43_658,
         "notes": "Monthly income exempt from tapering, Jan 2026"},

        # Disability pension (örorkulífeyrir) — TR, Sept 2025 new system
        {"transfer_type": "disability_pension_full",      "monthly_isk": 416_950,
         "notes": "Full örorkulífeyrir, pre-tax, Sept 2025 system"},
        {"transfer_type": "disability_pension_housing",   "monthly_isk": 486_076,
         "notes": "With housing supplement (living alone), Sept 2025"},
        {"transfer_type": "disability_income_threshold",  "monthly_isk": 105_200,
         "notes": "Income threshold before 45% taper kicks in, Sept 2025"},

        # Child benefits (barnabætur) — skatturinn.is, 2026
        {"transfer_type": "child_benefit_single_parent",  "monthly_isk": 42_875,
         "notes": "Per child, single parent, 2026 (514,500/yr ÷ 12)"},
        {"transfer_type": "child_benefit_couple",         "monthly_isk": 28_750,
         "notes": "Per child, couple, 2026 (345,000/yr ÷ 12)"},
        {"transfer_type": "child_benefit_under7_supplement", "monthly_isk": 10_833,
         "notes": "Extra for under-7, 2026 (130,000/yr ÷ 12)"},

        # Unemployment benefit (atvinnuleysisbætur) — island.is, 2025
        {"transfer_type": "unemployment_basic",           "monthly_isk": 383_870,
         "notes": "Basic 100% rate, 2025"},
        {"transfer_type": "unemployment_income_related_max", "monthly_isk": 605_159,
         "notes": "Income-related maximum (first 3 months), 2025"},
        {"transfer_type": "unemployment_child_supplement", "monthly_isk": 15_355,
         "notes": "Per child under 18, 2025"},
    ])
    conn.execute("CREATE OR REPLACE TABLE transfer_amounts AS SELECT * FROM df")
    print(f"Constants: transfer_amounts ({len(df)} rows)")


def write_housing_rental_rates(conn: duckdb.DuckDBPyConnection) -> None:
    df = pd.DataFrame([
        {"room_count": 1, "monthly_isk": 190_843, "per_sqm_isk": None,
         "municipality": "National", "source_date": "2026-02"},
        {"room_count": 2, "monthly_isk": 252_006, "per_sqm_isk": None,
         "municipality": "National", "source_date": "2026-02"},
        {"room_count": 3, "monthly_isk": 294_344, "per_sqm_isk": None,
         "municipality": "National", "source_date": "2026-02"},
        {"room_count": None, "monthly_isk": None, "per_sqm_isk": 4_041,
         "municipality": "Reykjavík", "source_date": "2026-02"},
        {"room_count": None, "monthly_isk": None, "per_sqm_isk": 3_810,
         "municipality": "Kópavogur", "source_date": "2026-02"},
        {"room_count": None, "monthly_isk": None, "per_sqm_isk": 3_639,
         "municipality": "National", "source_date": "2026-02"},
    ])
    conn.execute("CREATE OR REPLACE TABLE housing_rental_rates AS SELECT * FROM df")
    print(f"Constants: housing_rental_rates ({len(df)} rows)")


def write_student_loan_terms(conn: duckdb.DuckDBPyConnection) -> None:
    df = pd.DataFrame([
        {"term": "indexed_rate_proxy",   "value": 0.053,
         "notes": "CPI + 1% proxy; menntasjodur.is blocked automation — confirm manually"},
        {"term": "non_indexed_rate",     "value": None,
         "notes": "Not yet confirmed from menntasjodur.is"},
        {"term": "max_repayment_years",  "value": 7.0,
         "notes": "Maximum repayment period post-study"},
        {"term": "monthly_benefit_2025", "value": None,
         "notes": "Maximum monthly loan amount TBD — check menntasjodur.is in browser"},
    ])
    conn.execute("CREATE OR REPLACE TABLE student_loan_terms AS SELECT * FROM df")
    print(f"Constants: student_loan_terms ({len(df)} rows) — ⚠ indexed rate is proxy only")


def write_simulation_projections(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Seðlabanki Monetary Bulletin 2025/4 projections.
    Annual figures; simulation can interpolate to monthly.
    """
    df = pd.DataFrame([
        {"year": 2025, "quarter": None, "gdp_growth": 0.016,  "inflation_cpi": 0.043,
         "policy_rate": 0.0725, "unemployment_rate": None,
         "notes": "Monetary Bulletin 2025/4, Nov 2025"},
        {"year": 2026, "quarter": None, "gdp_growth": 0.022,  "inflation_cpi": 0.025,
         "policy_rate": None,   "unemployment_rate": None,
         "notes": "MB 2025/4 projection — CPI expected to reach 2.5% target H2 2026"},
    ])
    conn.execute("CREATE OR REPLACE TABLE simulation_projections AS SELECT * FROM df")
    print(f"Constants: simulation_projections ({len(df)} rows)")


def write_housing_costs(conn: duckdb.DuckDBPyConnection) -> None:
    """Hardcoded electricity and heating costs (Orkustofnun / Veitur, 2025)."""
    df = pd.DataFrame([
        {"cost_type": "electricity_per_kwh",        "isk": 25.76,
         "notes": "All-in incl. distribution + taxes, Sep 2025 (GlobalPetrolPrices citing Eurostat)"},
        {"cost_type": "geothermal_heating_annual",  "isk": 100_000,
         "notes": "Capital area average, 2025"},
        {"cost_type": "electric_heating_annual",    "isk": 250_000,
         "notes": "Non-geothermal areas estimate, 2025"},
    ])
    conn.execute("CREATE OR REPLACE TABLE housing_costs AS SELECT * FROM df")
    print(f"Constants: housing_costs ({len(df)} rows)")


def write_tax_rates(conn: duckdb.DuckDBPyConnection) -> None:
    """Supplement to existing tax logic — VAT and corporate rates."""
    df = pd.DataFrame([
        {"tax": "vat_standard",    "rate": 0.24,    "notes": "Standard VAT, skatturinn.is"},
        {"tax": "vat_reduced",     "rate": 0.11,    "notes": "Food, accommodation, transport, books, pools"},
        {"tax": "corporate_ltd",   "rate": 0.20,    "notes": "ehf/hf limited companies"},
        {"tax": "corporate_partnership", "rate": 0.376, "notes": "Partnerships"},
        {"tax": "capital_gains",   "rate": 0.22,    "notes": "Already in calculate_taxes.py"},
    ])
    conn.execute("CREATE OR REPLACE TABLE supplementary_tax_rates AS SELECT * FROM df")
    print(f"Constants: supplementary_tax_rates ({len(df)} rows)")


def main() -> None:
    conn = duckdb.connect(get_db_path())
    write_mortgage_rates(conn)
    write_transfer_amounts(conn)
    write_housing_rental_rates(conn)
    write_student_loan_terms(conn)
    write_simulation_projections(conn)
    write_housing_costs(conn)
    write_tax_rates(conn)
    conn.close()
    print("Constants written to silver.duckdb.")


if __name__ == "__main__":
    main()
