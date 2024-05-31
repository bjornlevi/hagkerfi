import pandas as pd
import sqlite3

# Load the CSV file
income_by_gender_and_age_group_file_path = 'data/income_by_gender_and_age_group.csv'
income_by_gender_and_age_group = pd.read_csv(income_by_gender_and_age_group_file_path, encoding='UTF-8', delimiter=';')

# Replace "ára og eldri" with "- 106 ára" in income_by_gender_and_age_group
income_by_gender_and_age_group['Aldur'] = income_by_gender_and_age_group['Aldur'].str.replace('ára og eldri', '- 106 ára')

# Unpack the age groups into specific ages
def unpack_age_groups(row):
    age_group = row['Aldur']
    gender = row['Kyn']
    income = row['2022']
    income_type = row['Tekjur og skattar']
    unpacked_rows = []
    
    if 'ára' in age_group:
        ages = age_group.replace(' ára', '').split(' - ')
        if len(ages) == 2:
            start_age, end_age = map(int, ages)
        else:
            start_age, end_age = int(ages[0]), int(ages[0])

    for age in range(start_age, end_age + 1):
        unpacked_rows.append({'Age': age, 'Gender': gender, 'Income': income, 'IncomeType': income_type})
    
    return unpacked_rows

# Unpack the age groups into specific ages
cleaned_data = []
for _, row in income_by_gender_and_age_group.iterrows():
    cleaned_data.extend(unpack_age_groups(row))

# Convert the cleaned data into a DataFrame
cleaned_income_by_gender_and_age_group = pd.DataFrame(cleaned_data).drop_duplicates()

# Pivot the DataFrame to have a column for each income type
cleaned_income_by_gender_and_age_group = cleaned_income_by_gender_and_age_group.pivot_table(
    index=['Age', 'Gender'], 
    columns='IncomeType', 
    values='Income', 
    aggfunc='first'
).reset_index()

# Rename columns to match the required database schema
cleaned_income_by_gender_and_age_group = cleaned_income_by_gender_and_age_group.rename(columns={
    'Heildartekjur': 'TotalIncome', 
    'Atvinnutekjur': 'Wages', 
    'Fjármagnstekjur': 'CapitalGains', 
    'Aðrar tekjur': 'OtherIncome'
})

# Create SQLite database and table
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Drop the table if it exists
c.execute('DROP TABLE IF EXISTS income_by_gender_and_age')

# Create the income_by_gender_and_age table
create_income_table_query = '''
    CREATE TABLE income_by_gender_and_age (
        age INTEGER,
        gender TEXT,
        TotalIncome REAL,
        Wages REAL,
        CapitalGains REAL,
        OtherIncome REAL
    )
'''

c.execute(create_income_table_query)

# Insert the cleaned income by gender and age data into the table using parameterized queries
for _, row in cleaned_income_by_gender_and_age_group.iterrows():
    age = row['Age']
    gender = row['Gender']
    total_income = row.get('TotalIncome', None)
    wages = row.get('Wages', None)
    capital_gains = row.get('CapitalGains', None)
    other_income = row.get('OtherIncome', None)
    insert_income_query = '''
        INSERT INTO income_by_gender_and_age (age, gender, TotalIncome, Wages, CapitalGains, OtherIncome)
        VALUES (?, ?, ?, ?, ?, ?)
    '''
    c.execute(insert_income_query, (age, gender, total_income, wages, capital_gains, other_income))

# Commit and close connection
conn.commit()
conn.close()

print("Income by gender and age data has been inserted into the database.")
