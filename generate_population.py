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
income_query = "SELECT * FROM gender_and_age_income_distribution"
income_df = pd.read_sql_query(income_query, conn)

# Rename columns from Icelandic to English for consistency
income_df.rename(columns={'Aldur': 'age', 'Kyn': 'gender'}, inplace=True)

# Fetch employment data
employment_query = "SELECT * FROM employment_data"
employment_df = pd.read_sql_query(employment_query, conn)

# Close the connection
conn.close()

# Gender translation map
gender_translation = {
    'Male': 'Karlar',
    'Female': 'Konur'
}

# Occupation distribution with summed categories
mapped_occupation_distribution = {
    'Karlar': {
        'Managers': 17600,
        'Professionals': 19800,
        'Technicians': 17200,
        'OfficeStaff': 2800,
        'ServiceCare': 20000,
        'IndustrialWorkers': 29400,  # Combined value
        'Laborers': 12300  # Combined value
    },
    'Konur': {
        'Managers': 10100,
        'Professionals': 31900,
        'Technicians': 17300,
        'OfficeStaff': 6400,
        'ServiceCare': 25300,
        'IndustrialWorkers': 2700,  # Combined value
        'Laborers': 6300  # Combined value
    }
}

# Normalize the occupation weights to probabilities
def normalize_weights(distribution):
    total = sum(distribution.values())
    return {occupation: count / total for occupation, count in distribution.items()}

occupation_probs = {
    'Karlar': normalize_weights(mapped_occupation_distribution['Karlar']),
    'Konur': normalize_weights(mapped_occupation_distribution['Konur'])
}

def assign_income(age, gender):
    if age < 13:
        return 0, 0, 0, 0  # No income for people under 13
    
    gender_icelandic = gender_translation[gender]
    
    try:
        # Lookup for the specific age and gender
        income_row = income_df[(income_df['age'] == age) & (income_df['gender'] == gender_icelandic)]
        if income_row.empty:
            raise ValueError(f"No income data found for Age: {age}, Gender: {gender_icelandic}")
    except KeyError as e:
        raise KeyError(f"Key error when accessing DataFrame: {str(e)}. Available columns: {income_df.columns.tolist()}")

    # Generate wages, capital gains, and other income using a normalized random distribution
    wages = round(income_row['Atvinnutekjur'].values[0] * np.random.normal(1, 0.1))
    capital_gains = round(income_row['Fjármagnstekjur'].values[0] * np.random.normal(1, 0.1))
    other_income = round(income_row['Aðrar_tekjur'].values[0] * np.random.normal(1, 0.1))

    total_income = wages + capital_gains + other_income
    return wages, capital_gains, other_income, round(total_income)

def assign_status(age, total_income, gender):
    if age < 13:
        return 'Student'
    
    # Retired status starts low at age 60 and increases significantly at 65, most people retire by 67
    if age >= 60:
        retirement_probability = min((age - 59) / 8, 1)  # Probability increases with age
        if random.random() < retirement_probability:
            return 'Retired'
    
    # Employment data provides a probability of being employed by age and gender
    employment_row = employment_df[(employment_df['age'] == age) & (employment_df['gender'] == gender)]
    if employment_row.empty:
        employment_probability = 0.5  # Default to 50% if no data is found
    else:
        employment_probability = employment_row['employed'].values[0] / 100

    # Adjust employment probability based on income
    income_factor = min(total_income / 100000, 1)  # Normalize and cap the income factor
    employment_probability *= income_factor

    if random.random() < employment_probability:
        return 'Employed'
    
    # If not employed, decide between student and disabled based on age
    student_probability = max(0.05, 1 - (age - 13) / (60 - 13))  # Gradually decrease student probability with age
    if random.random() < student_probability:
        return 'Student'
    
    return 'Disabled'

def assign_occupation(gender):
    # Select occupation based on gender and weighted probabilities
    gender_icelandic = gender_translation[gender]
    occupations = list(occupation_probs[gender_icelandic].keys())
    probabilities = list(occupation_probs[gender_icelandic].values())
    
    selected_occupation = random.choices(occupations, probabilities)[0]
    
    return selected_occupation  # Return the selected occupation directly

# Populate the database with individuals
population = []

for _, row in population_df.iterrows():
    age, count = int(row['age']), int(row['population'])  # Ensure age is int
    for _ in range(count):
        gender = random.choice(['Male', 'Female'])
        wages, capital_gains, other_income, total_income = assign_income(age, gender)
        status = assign_status(age, total_income, gender)
        occupation = assign_occupation(gender) if status == 'Employed' else status
        population.append({
            'age': age,
            'gender': gender,
            'occupation': occupation,
            'wages': wages,
            'capital_gains': capital_gains,
            'other_income': other_income,
            'total_income': total_income,
            'status': status
        })

# Identify the top 0.1% of the population based on total income
population_df = pd.DataFrame(population)
top_01_percent_threshold = population_df['total_income'].quantile(0.999)
top_01_percent = population_df[population_df['total_income'] >= top_01_percent_threshold]

def increase_capital_gains_exponentially(row):
    # Increase capital gains exponentially with a larger base factor
    base_factor = 10  # Adjust this base factor as needed for more aggressive growth
    row['capital_gains'] *= np.exp(row['capital_gains'] / 1e6) * base_factor
    row['capital_gains'] = round(row['capital_gains'])
    row['total_income'] = round(row['wages'] + row['capital_gains'] + row['other_income'])
    return row


top_01_percent = top_01_percent.apply(increase_capital_gains_exponentially, axis=1)

# Update the population list with the adjusted top 0.1%
population_df.update(top_01_percent)
population = population_df.to_dict(orient='records')

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
        total_income REAL,
        status TEXT
    )
'''

c.execute(create_population_table_query)

# Insert the population data into the table using parameterized queries
for person in population:
    insert_population_query = '''
        INSERT INTO population (age, gender, occupation, wages, capital_gains, other_income, total_income, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    '''
    c.execute(insert_population_query, (
        person['age'],
        person['gender'],
        person['occupation'],
        person['wages'],
        person['capital_gains'],
        person['other_income'],
        person['total_income'],
        person['status']
    ))

# Commit and close connection
conn.commit()
conn.close()

print("Generated population data with adjusted capital gains for the top 0.1% has been inserted into the database.")
