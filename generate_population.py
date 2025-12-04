import os
import pandas as pd
import sqlite3
import numpy as np
import random
from helpers import get_db_path, format_number

# Connect to the SQLite database
conn = sqlite3.connect(get_db_path())

# Fetch population distribution data
population_query = "SELECT * FROM population_distribution"
population_df = pd.read_sql_query(population_query, conn)

# Fetch income by gender and age data (keep original for diagnostics)
income_query = "SELECT * FROM gender_and_age_income_distribution"
income_df_original = pd.read_sql_query(income_query, conn)
income_df = income_df_original.copy()

# Rename columns from Icelandic to English for consistency
income_df.rename(columns={'Aldur': 'age', 'Kyn': 'gender'}, inplace=True)
# Smooth income curves a bit to reduce noise at higher ages
numeric_cols = ['Heildartekjur', 'Atvinnutekjur', 'Fjármagnstekjur', 'Aðrar_tekjur']
income_df = income_df.sort_values(['gender', 'age'])
for gender in income_df['gender'].unique():
    mask = income_df['gender'] == gender
    income_df.loc[mask, numeric_cols] = (
        income_df.loc[mask, numeric_cols]
        .rolling(window=5, center=True, min_periods=1)
        .mean()
    )

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

    # Generate wages, capital gains, and other income with capped noise to reduce spikes
    noise = lambda s=0.08: np.clip(np.random.normal(1, s), 0.85, 1.15)

    if age >= 90:
        # At very high ages, anchor closely to official means to avoid tail collapse
        wages = round(income_row['Atvinnutekjur'].values[0] * noise(0.03))
        capital_gains = round(income_row['Fjármagnstekjur'].values[0] * noise(0.03))
        other_income = round(income_row['Aðrar_tekjur'].values[0] * noise(0.03))
    else:
        wages = round(income_row['Atvinnutekjur'].values[0] * noise())
        capital_gains = round(income_row['Fjármagnstekjur'].values[0] * noise())
        other_income = round(income_row['Aðrar_tekjur'].values[0] * noise())

    # Gentle taper after 85 to avoid sharp drops; floor at 98% to stay near official plateau
    taper = 1.0
    if age >= 85:
        taper = max(0.98, 1 - (age - 85) * 0.001)
    wages = round(wages * taper)
    capital_gains = round(capital_gains * taper)
    other_income = round(other_income * taper)

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


def adjust_income_for_status(wages, capital_gains, other_income, status, age):
    """Return incomes unchanged; official averages already include non-employment."""
    return wages, capital_gains, other_income

# Populate the database with individuals
population = []

for _, row in population_df.iterrows():
    age, count = int(row['age']), int(row['population'])  # Ensure age is int
    for _ in range(count):
        gender = random.choice(['Male', 'Female'])
        wages, capital_gains, other_income, total_income = assign_income(age, gender)
        status = assign_status(age, total_income, gender)
        wages, capital_gains, other_income = adjust_income_for_status(
            wages, capital_gains, other_income, status, age
        )
        total_income = round(wages + capital_gains + other_income)
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
    # Dampened capital gains boost; apply to older high-income only
    if row['age'] < 65:
        return row
    base_factor = 3
    age_scale = min((row['age'] - 65) / 25, 1)  # ramp from 65 to 90
    row['capital_gains'] *= np.exp(min(row['capital_gains'], 2_000_000) / 1e6) * base_factor * age_scale
    row['capital_gains'] = round(row['capital_gains'])
    row['total_income'] = round(row['wages'] + row['capital_gains'] + row['other_income'])
    return row

top_01_percent = top_01_percent.apply(increase_capital_gains_exponentially, axis=1)

# Update the population list with the adjusted top 0.1%
population_df.update(top_01_percent)
population = population_df.to_dict(orient='records')

# --- Fit diagnostics: compare generated vs official by age/gender ---
def evaluate_fit(pop_df, ref_df):
    ref = ref_df.copy()
    ref = ref.rename(columns={'Aldur': 'age', 'Kyn': 'gender'})
    ref['gender'] = ref['gender'].replace({'Karlar': 'Male', 'Konur': 'Female'})
    ref = ref.rename(columns={
        'Heildartekjur': 'ref_total',
        'Atvinnutekjur': 'ref_wages',
        'Fjármagnstekjur': 'ref_cg',
        'Aðrar_tekjur': 'ref_other',
    })
    ref_means = ref.groupby(['gender', 'age'])[['ref_total', 'ref_wages', 'ref_cg', 'ref_other']].mean().reset_index()

    gen_means = pop_df.groupby(['gender', 'age'])[['total_income', 'wages', 'capital_gains', 'other_income']].mean().reset_index()
    merged = ref_means.merge(gen_means, on=['gender', 'age'], how='outer').fillna(0)
    merged['err_total'] = merged['total_income'] - merged['ref_total']
    merged['err_wages'] = merged['wages'] - merged['ref_wages']
    merged['err_cg'] = merged['capital_gains'] - merged['ref_cg']
    merged['err_other'] = merged['other_income'] - merged['ref_other']
    mae = {k: merged[k].abs().mean() for k in ['err_total', 'err_wages', 'err_cg', 'err_other']}
    print("Fit check (gen - official means by age/gender):")
    print("  MAE total:", format_number(mae['err_total']), "wages:", format_number(mae['err_wages']),
          "cap gains:", format_number(mae['err_cg']), "other:", format_number(mae['err_other']))
    for gender in ['Male', 'Female']:
        worst = merged[merged['gender'] == gender].reindex(
            merged[merged['gender'] == gender]['err_total'].abs().sort_values(ascending=False).index
        ).head(5)
        print(f"  Worst total-income ages ({gender}):",
              [(int(r.age), int(r.err_total)) for _, r in worst.iterrows()])

# After generating, run a fit check
evaluate_fit(population_df, income_df_original)

# Create SQLite database and table for population
conn = sqlite3.connect(get_db_path())
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
