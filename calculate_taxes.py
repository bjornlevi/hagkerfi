import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# Define constants for the tax calculation
PERSONAL_TAX_CREDIT = 779112  # Annual personal tax credit in ISK
TAX_BRACKETS = [
    (446136 * 12, 0.3148),     # Up to 5,353,632 ISK at 31.48%
    (1252501 * 12, 0.3798),    # 5,353,633 - 15,030,012 ISK at 37.98%
    (float('inf'), 0.4628),    # Above 15,030,012 ISK at 46.28%
]
CHILD_TAX_RATE = 0.06  # Tax rate for children under 16
CAPITAL_GAINS_TAX_RATE = 0.22  # 22% tax rate on capital gains
FREE_CAPITAL_GAINS_LIMIT = 300000  # Free capital gains limit in ISK
RADIO_FEE = 20900  # Radio fee in ISK for individuals 18 and older
ELDERLY_FUND_FEE = 13749  # Fee for the Elderly Fund for individuals 18 and older

# Function to calculate income tax based on annual income
def calculate_income_tax(total_income, age):
    if age < 16:
        # Apply children's tax rate
        taxable_income = max(0, total_income - 180000)
        return taxable_income * CHILD_TAX_RATE

    # Calculate tax based on tax brackets
    tax = 0
    for limit, rate in TAX_BRACKETS:
        if total_income <= limit:
            tax += total_income * rate
            break
        else:
            tax += limit * rate
            total_income -= limit

    # Apply personal tax credit after tax calculation
    tax -= PERSONAL_TAX_CREDIT
    tax = max(0, tax)  # Ensure tax does not go below 0
    return tax

# Function to calculate capital gains tax based on annual capital gains
def calculate_capital_gains_tax(capital_gains):
    taxable_capital_gains = max(0, capital_gains - FREE_CAPITAL_GAINS_LIMIT)
    return taxable_capital_gains * CAPITAL_GAINS_TAX_RATE

# Function to calculate other fixed annual fees
def calculate_fixed_fees(age):
    if age >= 18:
        return RADIO_FEE + ELDERLY_FUND_FEE
    return 0

# Connect to the SQLite database
conn = sqlite3.connect('income_data.db')

# Fetch the generated population data
generated_population_query = "SELECT * FROM population"
generated_population_df = pd.read_sql_query(generated_population_query, conn)

# Calculate taxes for each individual in the population
generated_population_df['income_tax'] = generated_population_df.apply(
    lambda row: calculate_income_tax(row['total_income'], row['age']), axis=1
)

generated_population_df['capital_gains_tax'] = generated_population_df.apply(
    lambda row: calculate_capital_gains_tax(row['capital_gains']), axis=1
)

generated_population_df['fixed_fees'] = generated_population_df.apply(
    lambda row: calculate_fixed_fees(row['age']), axis=1
)

# Calculate the total tax for each individual
generated_population_df['total_tax'] = generated_population_df['income_tax'] + \
                                       generated_population_df['capital_gains_tax'] + \
                                       generated_population_df['fixed_fees']

# Close the connection
conn.close()

# Plot and save the tax distributions by age
def plot_tax_by_age(tax_type, tax_column, title, file_name):
    # Sum the tax by age
    tax_by_age = generated_population_df.groupby('age')[tax_column].sum().reindex(age_range, fill_value=0)

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

# Define age range for plotting
age_range = range(1, 109)

# Plot and save the income tax distribution by age
plot_tax_by_age('Income Tax', 'income_tax', 'Income Tax Distribution by Age', 'income_tax_distribution_by_age.png')

# Plot and save the capital gains tax distribution by age
plot_tax_by_age('Capital Gains Tax', 'capital_gains_tax', 'Capital Gains Tax Distribution by Age', 'capital_gains_tax_distribution_by_age.png')

# Calculate and print the total amount for each tax
total_income_tax = round(generated_population_df['income_tax'].sum())
total_capital_gains_tax = round(generated_population_df['capital_gains_tax'].sum())
total_fixed_fees = round(generated_population_df['fixed_fees'].sum())

# Helper function to format numbers in the x.xxx.xxx format
def format_number(number):
    return f"{number:,}".replace(",", ".")

print(f"Total Income Tax: {format_number(total_income_tax)}")
print(f"Total Capital Gains Tax: {format_number(total_capital_gains_tax)}")
print(f"Total Fixed Fees (Radio and Elderly Fund): {format_number(total_fixed_fees)}")

print("Tax distributions and totals have been saved and printed.")
