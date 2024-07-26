import subprocess

# Run the scripts sequentially
def run_script(script_name):
    try:
        result = subprocess.run(['python', script_name], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(f"{script_name} executed successfully.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_name}.")
        print(e.stderr)

scripts = [
    "wage_distribution.py",
    "population_distribution.py",
    "income_by_gender_and_age.py",
    "cost_of_living.py",
    "generate_population.py"
]

for script in scripts:
    run_script(script)

# Example queries to verify the data in the database
import sqlite3
import pandas as pd

conn = sqlite3.connect('income_data.db')

# Query wage distribution
wage_distribution_df = pd.read_sql_query("SELECT * FROM wage_distribution", conn)
print("Wage Distribution:")
print(wage_distribution_df.head())

# Query population distribution
population_distribution_df = pd.read_sql_query("SELECT * FROM population_distribution", conn)
print("Population Distribution:")
print(population_distribution_df.head())

# Query income by gender and age
income_by_gender_and_age_df = pd.read_sql_query("SELECT * FROM income_by_gender_and_age", conn)
print("Income by Gender and Age:")
print(income_by_gender_and_age_df.head())

# Query cost of living
cost_of_living_df = pd.read_sql_query("SELECT * FROM cost_of_living", conn)
print("Cost of Living:")
print(cost_of_living_df.head())

# Query generated population
generated_population_df = pd.read_sql_query("SELECT * FROM population", conn)
print("Generated Population:")
print(generated_population_df.head())

# Close the connection
conn.close()
