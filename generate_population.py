import pandas as pd
import sqlite3
import numpy as np
import random

# Connect to the SQLite database
conn = sqlite3.connect('income_data.db')

# Fetch population distribution data
population_query = "SELECT * FROM population_distribution"
population_df = pd.read_sql_query(population_query, conn)

# Fetch income by gender and age data
income_query = "SELECT * FROM income_by_gender_and_age"
income_df = pd.read_sql_query(income_query, conn)

# Fetch wage distribution data
wage_query = "SELECT * FROM wage_distribution"
wage_distribution_df = pd.read_sql_query(wage_query, conn)

# Close the connection
conn.close()

# Define the occupation map to match the actual column names in the database
occupation_map = {
    'IndustrialWorkers': 'IndustrialWorkers',
    'Professionals': 'Professionals',
    'OfficeStaff': 'OfficeStaff',
    'Managers': 'Managers',
    'Technicians': 'Technicians',
    'Laborers': 'Laborers',
    'ServiceCare': 'ServiceCare',
}

# Gender translation map
gender_translation = {
    'Male': 'Karlar',
    'Female': 'Konur'
}

# Generate the population
population = []

def assign_occupation(age, gender):
    if age <= 5:
        return 'Student'
    if 6 <= age <= 15:
        return 'Student'
    if 16 <= age <= 19:
        if random.random() < 0.8:
            return 'Student'
    if 20 <= age <= 25:
        if random.random() < 0.5:
            return 'Student'
    if age >= 65:
        if random.random() < (age - 64) / 6:
            return 'Retired'
    
    occupation_weights = wage_distribution_df[list(occupation_map.values())].sum()
    occupations = occupation_weights.index.tolist()
    weights = occupation_weights.values
    return random.choices(occupations, weights=weights, k=1)[0]

def assign_income(age, gender, occupation):
    if age < 16:
        return 0, 0, 0, 0  # Skip income data for people under 16
    
    if occupation in ['Student', 'Retired']:
        gender_icelandic = gender_translation[gender]
        income_row = income_df[(income_df['age'] == age) & (income_df['gender'] == gender_icelandic)]
        if not income_row.empty:
            wages = income_row['Wages'].values[0] * np.random.normal(0.5, 0.1)  # Lower mean and tighter distribution for students
            capital_gains = income_row['CapitalGains'].values[0] * np.random.normal(0.5, 0.1)
            other_income = income_row['OtherIncome'].values[0] * np.random.normal(0.5, 0.1)
            total_income = wages + capital_gains + other_income
            return wages, capital_gains, other_income, total_income
        else:
            print(f"No income data found for Age: {age}, Gender: {gender}")
            print(income_df[(income_df['age'] == age)])
            print(income_df[(income_df['gender'] == gender_icelandic)])
            return 0, 0, 0, 0

    occupation_wage_distribution = wage_distribution_df[wage_distribution_df[occupation_map[occupation]] > 0]
    if not occupation_wage_distribution.empty:
        wage_row = occupation_wage_distribution.sample(weights=occupation_wage_distribution[occupation_map[occupation]])
        min_income = wage_row['min_income'].values[0]
        max_income = wage_row['max_income'].values[0]
        wages = np.random.uniform(min_income, max_income)
    else:
        wages = 0

    gender_icelandic = gender_translation[gender]
    income_row = income_df[(income_df['age'] == age) & (income_df['gender'] == gender_icelandic)]
    if not income_row.empty:
        capital_gains = income_row['CapitalGains'].values[0] * np.random.normal(1, 0.5)
        other_income = income_row['OtherIncome'].values[0] * np.random.normal(1, 0.5)
        total_income = wages + capital_gains + other_income
        return wages, capital_gains, other_income, total_income
    else:
        print(f"No income data found for Age: {age}, Gender: {gender}")
        print(income_df[(income_df['age'] == age)])
        print(income_df[(income_df['gender'] == gender_icelandic)])
        return wages, 0, 0, wages

# Populate the database with individuals
for _, row in population_df.iterrows():
    age, count = row['age'], int(row['population'])
    for _ in range(count):
        gender = random.choice(['Male', 'Female'])
        occupation = assign_occupation(age, gender)
        wages, capital_gains, other_income, total_income = assign_income(age, gender, occupation)
        population.append({
            'age': age,
            'gender': gender,
            'occupation': occupation,
            'wages': wages,
            'capital_gains': capital_gains,
            'other_income': other_income,
            'total_income': total_income
        })

# Create SQLite database and table for population
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Drop the table if it exists
c.execute('DROP TABLE IF EXISTS population')

# Create the population table
create_population_table_query = '''
    CREATE TABLE population (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        age INTEGER,
        gender TEXT,
        occupation TEXT,
        wages REAL,
        capital_gains REAL,
        other_income REAL,
        total_income REAL
    )
'''

c.execute(create_population_table_query)

# Insert the population data into the table using parameterized queries
for person in population:
    insert_population_query = '''
        INSERT INTO population (age, gender, occupation, wages, capital_gains, other_income, total_income)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    '''
    c.execute(insert_population_query, (
        person['age'],
        person['gender'],
        person['occupation'],
        person['wages'],
        person['capital_gains'],
        person['other_income'],
        person['total_income']
    ))

# Commit and close connection
conn.commit()
conn.close()

print("Generated population data has been inserted into the database.")
