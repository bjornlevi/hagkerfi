"""
Tax Summary Report
==================
Compares simulated tax totals against official budget targets and produces
income/status/tax distribution charts.

Run:  make report   (via population_report.py)
      DB_PATH=data/silver.db python reports/tax_summary.py
"""

import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers import get_db_path, format_number

CHARTS_DIR = Path("reports/charts")
BUDGET_PATH = Path("data/fjarlog_2026.xlsx")
MUNICIPAL_ACCOUNTS_PATH = Path("data/arsreikningar_sveitarfelaga.xlsx")
AGE_RANGE = range(1, 101)


def load_national_budget(path: Path = BUDGET_PATH) -> dict | None:
    if not path.exists():
        print(f"Warning: budget file not found at {path}; skipping budget comparison.")
        return None
    try:
        df = pd.read_excel(path, sheet_name="4-1", header=None)
        def pick(label):
            row = df[df[0] == label]
            if row.empty:
                raise ValueError(f"{label!r} not found in budget table")
            return float(row.iloc[0, 3])
        return {
            "skatttekjur_mkr": pick("Skatttekjur"),
            "heildartekjur_mkr": pick("Heildartekjur ríkissjóðs"),
        }
    except Exception as exc:
        print(f"Warning: could not parse budget file: {exc}")
        return None


def load_municipal_accounts(path: Path = MUNICIPAL_ACCOUNTS_PATH) -> dict | None:
    if not path.exists():
        print(f"Warning: municipal accounts file not found at {path}; skipping.")
        return None
    try:
        df = pd.read_excel(path, sheet_name=0)
        tekjur_row = df[df[df.columns[1]] == "Tekjur"]
        if tekjur_row.empty:
            raise ValueError("Tekjur row not found")
        tekjur_thousand = float(tekjur_row.iloc[0, -1])
        return {"tekjur_mkr": tekjur_thousand / 1000}
    except Exception as exc:
        print(f"Warning: could not parse municipal accounts: {exc}")
        return None


def _save(fig_name: str) -> None:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(CHARTS_DIR / fig_name)
    plt.close()


def plot_income_comparisons(income_ref: pd.DataFrame, pop: pd.DataFrame) -> None:
    pairs = [
        ("Aðrar_tekjur",     "other_income",    "Other Income"),
        ("Fjármagnstekjur",  "capital_gains",   "Capital Gains"),
        ("Atvinnutekjur",    "wages",            "Employment Income"),
        ("Heildartekjur",    "total_income",     "Total Income"),
    ]
    for ref_col, gen_col, label in pairs:
        if ref_col not in income_ref.columns or gen_col not in pop.columns:
            continue
        ref_avg = income_ref.groupby("Aldur")[ref_col].mean().reindex(AGE_RANGE, fill_value=0)
        gen_avg = pop.groupby("age")[gen_col].mean().reindex(AGE_RANGE, fill_value=0)
        plt.figure(figsize=(10, 6))
        plt.plot(AGE_RANGE, ref_avg, label="Official", marker="o", markersize=3)
        plt.plot(AGE_RANGE, gen_avg, label="Generated", marker="o", markersize=3)
        plt.title(f"{label} Distribution by Age")
        plt.xlabel("Age")
        plt.ylabel(label)
        plt.legend()
        plt.grid(True)
        _save(f"{gen_col}_comparison_by_age.png")


def plot_status_distributions(pop: pd.DataFrame) -> None:
    for status in ("Student", "Employed", "Disabled", "Retired"):
        counts = pop[pop["status"] == status].groupby("age").size().reindex(AGE_RANGE, fill_value=0)
        plt.figure(figsize=(10, 6))
        plt.plot(AGE_RANGE, counts, label=status, marker="o", markersize=3)
        plt.title(f"{status} Distribution by Age")
        plt.xlabel("Age")
        plt.ylabel("Number of People")
        plt.legend()
        plt.grid(True)
        _save(f"{status.lower()}_distribution_by_age.png")


def plot_tax_distributions(taxes: pd.DataFrame) -> None:
    pairs = [
        ("income_tax",        "Income Tax"),
        ("capital_gains_tax", "Capital Gains Tax"),
        ("fixed_fees",        "Fixed Fees"),
        ("total_tax",         "Total Tax"),
    ]
    for col, label in pairs:
        if col not in taxes.columns:
            continue
        by_age = taxes.groupby("age")[col].sum().reindex(AGE_RANGE, fill_value=0)
        plt.figure(figsize=(10, 6))
        plt.plot(AGE_RANGE, by_age, label=label, marker="o", markersize=3)
        plt.title(f"{label} Distribution by Age")
        plt.xlabel("Age")
        plt.ylabel("Tax Amount (ISK)")
        plt.legend()
        plt.grid(True)
        _save(f"{col}_distribution_by_age.png")


def main() -> None:
    conn = duckdb.connect(get_db_path())

    pop = conn.execute("SELECT * FROM population").df()
    income_ref = conn.execute("SELECT * FROM gender_and_age_income_distribution").df()

    try:
        taxes = conn.execute(
            "SELECT id, age, income_tax, municipal_tax, capital_gains_tax, fixed_fees, total_tax "
            "FROM population_with_taxes"
        ).df()
    except Exception:
        print("Warning: population_with_taxes table not found. Run 'make simulate' first.")
        taxes = pd.DataFrame()

    conn.close()

    print("Generating income comparison charts...")
    plot_income_comparisons(income_ref, pop)

    print("Generating employment status charts...")
    plot_status_distributions(pop)

    if not taxes.empty:
        print("Generating tax distribution charts...")
        plot_tax_distributions(taxes)

        def to_mkr(x): return x / 1_000_000
        total_income_tax    = taxes["income_tax"].sum()
        total_capital_gains = taxes["capital_gains_tax"].sum()
        total_fixed_fees    = taxes["fixed_fees"].sum()
        total_tax           = taxes["total_tax"].sum()

        print("\n=== Simulated tax take (m.kr) ===")
        print(f"  Income tax:          {format_number(to_mkr(total_income_tax))}")
        print(f"  Capital gains tax:   {format_number(to_mkr(total_capital_gains))}")
        print(f"  Fixed fees:          {format_number(to_mkr(total_fixed_fees))}")
        print(f"  Total collected:     {format_number(to_mkr(total_tax))}")

        budget = load_national_budget()
        muni   = load_municipal_accounts()
        if budget and muni:
            combined_target = budget["skatttekjur_mkr"] + muni["tekjur_mkr"]
            gap = to_mkr(total_tax) - budget["skatttekjur_mkr"]
            pct = (gap / budget["skatttekjur_mkr"]) * 100
            print(f"\n=== vs official targets ===")
            print(f"  State tax target 2026:     {format_number(budget['skatttekjur_mkr'])} m.kr")
            print(f"  Municipal revenues:        {format_number(muni['tekjur_mkr'])} m.kr")
            print(f"  Gap vs state target:       {format_number(gap)} m.kr ({pct:+.1f}%)")

    print(f"\nCharts saved to {CHARTS_DIR}/")


if __name__ == "__main__":
    main()
