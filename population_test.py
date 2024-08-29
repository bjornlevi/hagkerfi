import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# Connect to the SQLite database
conn = sqlite3.connect('income_data.db')

# Fetch the original income data
original_income_query = "SELECT * FROM gender_and_age_income_distribution"
original_income_df = pd.read_sql_query(original_income_query, conn)

# Fetch the generated population data
generated_population_query = "SELECT * FROM population"
generated_population_df = pd.read_sql_query(generated_population_query, conn)

# Fetch tax data from the population_with_taxes table
tax_query = "SELECT age, income_tax, capital_gains_tax FROM population_with_taxes"
tax_df = pd.read_sql_query(tax_query, conn)

# Close the connection
conn.close()

# Define age range for comparison
age_range = range(1, 101)

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

print(f"Total Income Tax: {total_income_tax}")
print(f"Total Capital Gains Tax: {total_capital_gains_tax}")

print("Comparisons, occupation status distributions, and tax distributions have been saved as images.")
