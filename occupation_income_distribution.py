import pandas as pd
import sqlite3

# Load the CSV file (https://www.hagstofa.is/talnaefni/samfelag/laun-og-tekjur/laun/)
occupation_income_distribution_file_path = 'data/wage_distribution_by_occupation.csv'
occupation_income_distribution = pd.read_csv(occupation_income_distribution_file_path, encoding='UTF-8')

# Adjust the 'Laun' column in occupation_income_distribution to remove dots and multiply by 1000
def adjust_wage_ranges(wage_range):
    parts = wage_range.replace('.', '').split('-')
    if len(parts) == 2:
        return f"{int(parts[0]) * 1000}-{int(parts[1]) * 1000}"
    else:  # For ranges that only have a lower bound (e.g., "1500-")
        return f"{int(parts[0]) * 1000}-inf"

occupation_income_distribution['Laun'] = occupation_income_distribution['Laun'].apply(adjust_wage_ranges)

# Convert the percentage columns to float, replace commas with dots, and divide by 100
for column in occupation_income_distribution.columns[1:]:
    occupation_income_distribution[column] = occupation_income_distribution[column].str.replace(',', '.').astype(float) / 100

# Rename columns to be database-safe
occupation_income_distribution.rename(columns={
    'Iðnaðarfólk': 'IndustrialWorkers', 
    'Sérfræðingar': 'Professionals', 
    'Skrifstofufólk': 'OfficeStaff', 
    'Stjórnendur': 'Managers', 
    'Tæknar': 'Technicians', 
    'Verkafólk': 'Laborers', 
    'Þjónusta og umönnun': 'ServiceCare'
}, inplace=True)

# Split wage_range into min_income and max_income
def split_wage_range(row):
    parts = row['Laun'].split('-')
    row['min_income'] = int(parts[0])
    row['max_income'] = int(parts[1]) if parts[1] != 'inf' else float('inf')
    return row

occupation_income_distribution = occupation_income_distribution.apply(split_wage_range, axis=1)

# Create SQLite database and table
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Drop the table if it exists
c.execute('DROP TABLE IF EXISTS occupation_income_distribution')

# Create the occupation_income_distribution table with columns for each occupation and the total percentage
create_wage_table_query = '''
    CREATE TABLE occupation_income_distribution (
        wage_range TEXT,
        min_income INTEGER,
        max_income INTEGER,
        IndustrialWorkers REAL,
        Professionals REAL,
        OfficeStaff REAL,
        Managers REAL,
        Technicians REAL,
        Laborers REAL,
        ServiceCare REAL,
        Alls REAL
    )
'''

c.execute(create_wage_table_query)

# Insert the cleaned wage distribution data into the table using parameterized queries
for _, row in occupation_income_distribution.iterrows():
    wage_range = row['Laun']
    values = [wage_range, row['min_income'], row['max_income']] + [row[col] for col in ['IndustrialWorkers', 'Professionals', 'OfficeStaff', 'Managers', 'Technicians', 'Laborers', 'ServiceCare', 'Alls']]
    placeholders = ', '.join(['?'] * len(values))
    insert_wage_query = f'''
        INSERT INTO occupation_income_distribution (wage_range, min_income, max_income, IndustrialWorkers, Professionals, OfficeStaff, Managers, Technicians, Laborers, ServiceCare, Alls)
        VALUES ({placeholders})
    '''
    c.execute(insert_wage_query, values)

# Commit and close connection
conn.commit()
conn.close()

print("Occupation income distribution data has been inserted into the database.")
