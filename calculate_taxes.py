import pandas as pd
import duckdb
from pathlib import Path
from helpers import get_db_path, format_number

PERSONAL_TAX_CREDIT = 779112       # Annual personal tax credit in ISK
TAX_BRACKETS = [
    (446136 * 12, 0.3148),          # Up to 5,353,632 ISK at 31.48%
    (1252501 * 12, 0.3798),         # 5,353,633 - 15,030,012 ISK at 37.98%
    (float('inf'), 0.4628),         # Above 15,030,012 ISK at 46.28%
]
CHILD_TAX_RATE = 0.06
CAPITAL_GAINS_TAX_RATE = 0.22
FREE_CAPITAL_GAINS_LIMIT = 300_000  # ISK/year exempt from capital gains tax
RADIO_FEE = 20_900                  # Annual radio fee (18+)
ELDERLY_FUND_FEE = 13_749           # Annual elderly fund fee (18+)
MUNICIPAL_TAX_PATH = Path("data/utsvar_sveitarfelaga.xls")
FALLBACK_MUNICIPAL_TAX_RATE = 0.1494


def load_municipal_tax_rate(path: Path = MUNICIPAL_TAX_PATH, fallback: float = FALLBACK_MUNICIPAL_TAX_RATE) -> float:
    try:
        df = pd.read_excel(path, sheet_name="Öll", header=None)
        row = df[df[0].astype(str).str.contains("Meðal útvarsprósenta", na=False)]
        return float(row.iloc[0, 2])
    except Exception as exc:
        print(f"Warning: Could not read municipal tax rate from {path}: {exc}. Using fallback {fallback:.4f}.")
        return fallback


MUNICIPAL_TAX_RATE = load_municipal_tax_rate()


def calculate_income_tax(total_income: float, age: int, municipal_rate: float = MUNICIPAL_TAX_RATE) -> tuple[float, float]:
    if age < 16:
        child_tax = max(0, total_income - 180_000) * CHILD_TAX_RATE
        return child_tax, 0.0

    state_tax = 0.0
    remaining = total_income
    for limit, rate in TAX_BRACKETS:
        if remaining <= 0:
            break
        portion = min(remaining, limit)
        state_tax += portion * rate
        remaining -= portion

    municipal_tax = total_income * municipal_rate
    gross_tax = state_tax + municipal_tax
    net_tax = max(0.0, gross_tax - PERSONAL_TAX_CREDIT)
    net_municipal = municipal_tax * (net_tax / gross_tax) if gross_tax > 0 and net_tax > 0 else 0.0
    return net_tax, net_municipal


def calculate_capital_gains_tax(capital_gains: float) -> float:
    return max(0.0, capital_gains - FREE_CAPITAL_GAINS_LIMIT) * CAPITAL_GAINS_TAX_RATE


def calculate_fixed_fees(age: int) -> float:
    return float(RADIO_FEE + ELDERLY_FUND_FEE) if age >= 18 else 0.0


conn = duckdb.connect(get_db_path())
pop = conn.execute("SELECT id, age, total_income, capital_gains FROM population").df()

pop[['income_tax', 'municipal_tax']] = pop.apply(
    lambda r: calculate_income_tax(r['total_income'], r['age']), axis=1, result_type='expand'
)
pop['capital_gains_tax'] = pop['capital_gains'].apply(calculate_capital_gains_tax)
pop['fixed_fees'] = pop['age'].apply(calculate_fixed_fees)
pop['total_tax'] = pop['income_tax'] + pop['capital_gains_tax'] + pop['fixed_fees']

# Persist tax results so downstream reports and the tax_summary script can query them
tax_cols = ['id', 'age', 'income_tax', 'municipal_tax', 'capital_gains_tax', 'fixed_fees', 'total_tax']
pop_taxes = pop[tax_cols]
conn.execute("CREATE OR REPLACE TABLE population_with_taxes AS SELECT * FROM pop_taxes")
conn.close()

total_income_tax      = round(pop['income_tax'].sum())
total_municipal_tax   = round(pop['municipal_tax'].sum())
total_capital_gains   = round(pop['capital_gains_tax'].sum())
total_fixed_fees      = round(pop['fixed_fees'].sum())
total_tax             = round(pop['total_tax'].sum())

print(f"Municipal tax rate (average): {MUNICIPAL_TAX_RATE * 100:.2f}%")
print(f"Total Income Tax:             {format_number(total_income_tax)}")
print(f"  of which municipal:         {format_number(total_municipal_tax)}")
print(f"Total Capital Gains Tax:      {format_number(total_capital_gains)}")
print(f"Total Fixed Fees:             {format_number(total_fixed_fees)}")
print(f"Total Tax Collected:          {format_number(total_tax)}")
print("Tax results written to population_with_taxes table.")
