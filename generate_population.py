import pandas as pd
import sqlite3
import random
import numpy as np

# Connect to the SQLite database
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Fetch the population distribution data from the database
population_distribution_query = 'SELECT * FROM population_distribution'
population_distribution = pd.read_sql_query(population_distribution_query, conn)

# Fetch the income by gender and age group data from the database
income_by_gender_and_age_group_query = 'SELECT * FROM income_by_gender_and_age'
income_by_gender_and_age_group = pd.read_sql_query(income_by_gender_and_age_group_query, conn)

# Fetch the wage distribution data from the database
wage_distribution_query = 'SELECT * FROM wage_distribution'
wage_distribution = pd.read_sql_query(wage_distribution_query, conn)

# Extract occupation names from the database
occupation_names = wage_distribution.columns[1:]

# Convert byte-like objects to strings and then to integers for population_distribution
def convert_to_int(value):
    if isinstance(value, bytes):
        return int(value.decode('utf-8').strip())
    return int(value)

population_distribution['age'] = population_distribution['age'].apply(convert_to_int)
population_distribution['population'] = population_distribution['population'].apply(convert_to_int)

# Create the population table
c.execute('DROP TABLE IF EXISTS population')

create_population_table_query = '''
    CREATE TABLE population (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        age INTEGER,
        gender TEXT,
        total_income REAL,
        wages REAL,
        capital_gains REAL,
        other_income REAL,
        occupation TEXT
    )
'''
c.execute(create_population_table_query)

# Create the population with the correct age distribution and random gender
def create_population(population_distribution):
    population = []
    for _, row in population_distribution.iterrows():
        age = row['age']
        count = row['population']
        for _ in range(count):
            gender = random.choice(['Karlar', 'Konur'])
            population.append({'age': convert_to_int(age), 'gender': gender})
    return population

population = create_population(population_distribution)

# Assign a random income to each person based on their age
def assign_income(person, income_data):
    age = person['age']
    gender = person['gender']
    income_row = income_data[(income_data['age'] == age) & (income_data['gender'] == gender)]
    if not income_row.empty:
        wages = np.random.normal(loc=income_row['Wages'].values[0], scale=0.1 * income_row['Wages'].values[0])
        capital_gains = np.random.normal(loc=income_row['CapitalGains'].values[0], scale=0.1 * income_row['CapitalGains'].values[0])
        other_income = np.random.normal(loc=income_row['OtherIncome'].values[0], scale=0.1 * income_row['OtherIncome'].values[0])
        total_income = wages + capital_gains + other_income
        return total_income, wages, capital_gains, other_income
    return 0, 0, 0, 0

# Assign an occupation based on the total income
def assign_occupation(total_income, age, wage_distribution, occupation_names):
    if age < 18:
        return 'Student'
    if age >= 65:
        retirement_probability = (age - 64) / 6 if age <= 70 else 1
        if random.random() < retirement_probability:
            return 'Retired'
    for _, row in wage_distribution.iterrows():
        min_wage, max_wage = map(int, row['wage_range'].replace('inf', '10000000').split('-'))
        if min_wage <= total_income <= max_wage:
            occupations = {occupation: row[occupation] for occupation in occupation_names}
            occupation = random.choices(list(occupations.keys()), weights=list(occupations.values()), k=1)[0]
            return occupation
    return 'Unemployed'

# Insert the individuals into the database
for person in population:
    total_income, wages, capital_gains, other_income = assign_income(person, income_by_gender_and_age_group)
    occupation = assign_occupation(total_income, person['age'], wage_distribution, occupation_names)
    c.execute('''
        INSERT INTO population (age, gender, total_income, wages, capital_gains, other_income, occupation)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (person['age'], person['gender'], total_income, wages, capital_gains, other_income, occupation))

# Commit and close connection
conn.commit()
conn.close()

print("Population data has been generated and inserted into the database.")
