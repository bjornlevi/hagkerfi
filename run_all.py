import subprocess
import sqlite3
import pandas as pd

# List of script files to run
scripts = [
    'wage_distribution.py',
    'population_distribution.py',
    'income_by_gender_and_age.py',
    'cost_of_living.py'
]

# Run each script
for script in scripts:
    try:
        result = subprocess.run(['python', script], check=True, capture_output=True, text=True)
        print(f"Script {script} ran successfully.\n")
        print(f"Output:\n{result.stdout}\n")
    except subprocess.CalledProcessError as e:
        print(f"Error running script {script}.\n")
        print(f"Error message:\n{e.stderr}\n")

# Connect to the SQLite database
conn = sqlite3.connect('income_data.db')

# Example query functions

def query_wage_distribution(wage_range=None):
    query = 'SELECT * FROM wage_distribution WHERE 1=1'
    params = []
    if wage_range is not None:
        query += ' AND wage_range = ?'
        params.append(wage_range)
    df = pd.read_sql_query(query, conn, params=params)
    return df

def query_population_distribution(age=None):
    query = 'SELECT * FROM population_distribution WHERE 1=1'
    params = []
    if age is not None:
        query += ' AND age = ?'
        params.append(age)
    df = pd.read_sql_query(query, conn, params=params)
    df['population'] = df['population'].astype(int)  # Ensure population is int
    return df

def query_income_by_gender_and_age(age=None, gender=None):
    query = 'SELECT * FROM income_by_gender_and_age WHERE 1=1'
    params = []
    if age is not None:
        query += ' AND age = ?'
        params.append(age)
    if gender is not None:
        query += ' AND gender = ?'
        params.append(gender)
    df = pd.read_sql_query(query, conn, params=params)
    return df

def query_cost_of_living_data(category=None):
    query = 'SELECT * FROM cost_of_living WHERE 1=1'
    params = []
    if category is not None:
        query += ' AND category = ?'
        params.append(category)
    df = pd.read_sql_query(query, conn, params=params)
    return df

# Example queries
print("Query result for wage range '1000000-1050000':")
result = query_wage_distribution(wage_range='1000000-1050000')
print(result)

print("Query result for age 17:")
result = query_population_distribution(age=17)
print(result)

print("Query result for age 17 and gender 'Karlar':")
result = query_income_by_gender_and_age(age=17, gender='Karlar')
print(result)

print("Query result for cost of living category 'HousingUtilities':")
result = query_cost_of_living_data(category='HousingUtilities')
result['cost'] = result['cost'].apply(lambda x: f"{x:,.2f}")
print(result)

# Close connection
conn.close()
