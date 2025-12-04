import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from pathlib import Path
from helpers import get_db_path, format_number

# Connect to the SQLite database
conn = sqlite3.connect(get_db_path())

# Fetch the original income data
original_income_query = "SELECT * FROM gender_and_age_income_distribution"
original_income_df = pd.read_sql_query(original_income_query, conn)

# Fetch the generated population data
generated_population_query = "SELECT * FROM population"
generated_population_df = pd.read_sql_query(generated_population_query, conn)

# Fetch tax data from the population_with_taxes table
tax_query = """
    SELECT
        age,
        income_tax,
        capital_gains_tax,
        broadcasting_fee,
        elderly_fund_fee,
        total_tax
    FROM population_with_taxes
"""
tax_df = pd.read_sql_query(tax_query, conn)

# Close the connection
conn.close()

# Paths to the official budget data
BUDGET_2026_PATH = Path("data/fjarlog_2026.xlsx")
MUNICIPAL_ACCOUNTS_PATH = Path("data/arsreikningar_sveitarfelaga.xlsx")

# Define age range for comparison
age_range = range(1, 101)


def load_national_budget(path: Path = BUDGET_2026_PATH) -> dict:
    """Load the tax targets from the 2026 budget (table 4-1)."""
    df = pd.read_excel(path, sheet_name="4-1", header=None)

    def pick(label: str) -> float:
        row = df[df[0] == label]
        if row.empty:
            raise ValueError(f"{label} not found in budget table")
        # Column 3 corresponds to the "Frumvarp 2026" column.
        return float(row.iloc[0, 3])

    return {
        "skatttekjur_mkr": pick("Skatttekjur"),
        "heildartekjur_mkr": pick("Heildartekjur ríkissjóðs"),
    }


def load_municipal_accounts(path: Path = MUNICIPAL_ACCOUNTS_PATH) -> dict:
    """Load aggregated municipal revenues (Tekjur) from the pivot sheet.

    The workbook lists amounts in thousand ISK, so we convert to m.kr to align
    with the national budget.
    """
    df = pd.read_excel(path, sheet_name=0)
    tekjur_row = df[df[df.columns[1]] == "Tekjur"]
    if tekjur_row.empty:
        raise ValueError("Tekjur row not found in municipal accounts file")

    tekjur_thousand = float(tekjur_row.iloc[0, -1])
    tekjur_mkr = tekjur_thousand / 1000  # thousand ISK -> m.kr
    return {"tekjur_thousand": tekjur_thousand, "tekjur_mkr": tekjur_mkr}


# Function to plot comparison
def plot_comparison(data_type, original_column, generated_column, title, file_name):
    # Calculate averages by age
    original_avg_by_age = original_income_df.groupby('Aldur')[original_column].mean().reindex(age_range, fill_value=0)
    generated_avg_by_age = generated_population_df.groupby('age')[generated_column].mean().reindex(age_range, fill_value=0)

    # Plot the comparison
    plt.figure(figsize=(10, 6))
    plt.plot(age_range, original_avg_by_age, label='Original', marker='o')
    plt.plot(age_range, generated_avg_by_age, label='Generated', marker='o')
    plt.title(title)
    plt.xlabel('Age')
    plt.ylabel(data_type)
    plt.legend()
    plt.grid(True)
    
    # Save the plot
    plt.savefig('tests/' + file_name)
    plt.close()

# Plot and save comparison for Other Income (Aðrar tekjur)
plot_comparison('Other Income', 'Aðrar_tekjur', 'other_income', 'Other Income Distribution by Age', 'other_income_comparison_by_age.png')

# Plot and save comparison for Capital Gains (Fjármagnstekjur)
plot_comparison('Capital Gains', 'Fjármagnstekjur', 'capital_gains', 'Capital Gains Distribution by Age', 'capital_gains_comparison_by_age.png')

# Plot and save comparison for Employment Income (Atvinnutekjur)
plot_comparison('Employment Income', 'Atvinnutekjur', 'wages', 'Employment Income Distribution by Age', 'employment_income_comparison_by_age.png')

# Plot and save comparison for Total Income (Heildartekjur)
plot_comparison('Total income', 'Heildartekjur', 'total_income', 'Total Income Distribution by Age', 'total_income_comparison_by_age.png')

# Function to plot occupation status
def plot_occupation_status(status, title, file_name):
    # Count the number of people in each status by age
    status_by_age = generated_population_df[generated_population_df['status'] == status].groupby('age').size().reindex(age_range, fill_value=0)

    # Plot the status distribution by age
    plt.figure(figsize=(10, 6))
    plt.plot(age_range, status_by_age, label=status, marker='o')
    plt.title(title)
    plt.xlabel('Age')
    plt.ylabel('Number of People')
    plt.legend()
    plt.grid(True)
    
    # Save the plot
    plt.savefig('tests/' + file_name)
    plt.close()

# Plot and save the distribution of students by age
plot_occupation_status('Student', 'Student Distribution by Age', 'student_distribution_by_age.png')

# Plot and save the distribution of employed individuals by age
plot_occupation_status('Employed', 'Employed Distribution by Age', 'employed_distribution_by_age.png')

# Plot and save the distribution of disabled individuals by age
plot_occupation_status('Disabled', 'Disabled Distribution by Age', 'disabled_distribution_by_age.png')

# Plot and save the distribution of retired individuals by age
plot_occupation_status('Retired', 'Retired Distribution by Age', 'retired_distribution_by_age.png')

# Function to plot tax by age
def plot_tax_by_age(tax_type, tax_column, title, file_name):
    # Sum the tax by age
    tax_by_age = tax_df.groupby('age')[tax_column].sum().reindex(age_range, fill_value=0)

    # Plot the tax distribution by age
    plt.figure(figsize=(10, 6))
    plt.plot(age_range, tax_by_age, label=tax_type, marker='o')
    plt.title(title)
    plt.xlabel('Age')
    plt.ylabel('Tax Amount')
    plt.legend()
    plt.grid(True)
    
    # Save the plot
    plt.savefig('tests/' + file_name)
    plt.close()

# Plot and save the income tax distribution by age
plot_tax_by_age('Income Tax', 'income_tax', 'Income Tax Distribution by Age', 'income_tax_distribution_by_age.png')

# Plot and save the capital gains tax distribution by age
plot_tax_by_age('Capital Gains Tax', 'capital_gains_tax', 'Capital Gains Tax Distribution by Age', 'capital_gains_tax_distribution_by_age.png')

# Calculate and print the total amount for each tax
total_income_tax = tax_df['income_tax'].sum()
total_capital_gains_tax = tax_df['capital_gains_tax'].sum()
total_broadcasting_fee = tax_df['broadcasting_fee'].sum()
total_elderly_fee = tax_df['elderly_fund_fee'].sum()
total_tax_collected = tax_df['total_tax'].sum()


def to_mkr(amount: float) -> float:
    """Convert ISK to m.kr for easier comparison with budgets."""
    return amount / 1_000_000


simulated_totals = {
    "income_tax_mkr": to_mkr(total_income_tax),
    "capital_gains_tax_mkr": to_mkr(total_capital_gains_tax),
    "broadcasting_fee_mkr": to_mkr(total_broadcasting_fee),
    "elderly_fee_mkr": to_mkr(total_elderly_fee),
    "total_tax_mkr": to_mkr(total_tax_collected),
}

budget_targets = load_national_budget()
municipal_accounts = load_municipal_accounts()
combined_public_revenue_target = (
    budget_targets["skatttekjur_mkr"] + municipal_accounts["tekjur_mkr"]
)


def describe_gap(sim_value: float, target_value: float) -> str:
    gap = sim_value - target_value
    pct = (gap / target_value) * 100 if target_value else 0
    return f"{format_number(gap)} m.kr ({pct:+.1f}%)"

print("=== Simulated tax take (m.kr) ===")
print(f"Income tax: {format_number(simulated_totals['income_tax_mkr'])}")
print(f"Capital gains tax: {format_number(simulated_totals['capital_gains_tax_mkr'])}")
print(f"Broadcasting fee: {format_number(simulated_totals['broadcasting_fee_mkr'])}")
print(f"Elderly fund fee: {format_number(simulated_totals['elderly_fee_mkr'])}")
print(f"Total collected (incl. fees): {format_number(simulated_totals['total_tax_mkr'])}")

print("\n=== Official targets (m.kr) ===")
print(f"State budget tax revenue target 2026: {format_number(budget_targets['skatttekjur_mkr'])}")
print(f"Municipal revenues (Tekjur) 2024: {format_number(municipal_accounts['tekjur_mkr'])}")
print(f"Combined state + municipal target: {format_number(combined_public_revenue_target)}")

print("\n=== Gaps (simulated - target) ===")
print(f"Vs state tax target: {describe_gap(simulated_totals['total_tax_mkr'], budget_targets['skatttekjur_mkr'])}")
print(f"Vs combined state + municipal: {describe_gap(simulated_totals['total_tax_mkr'], combined_public_revenue_target)}")

print("\nComparisons, occupation status distributions, and tax distributions have been saved as images.")
