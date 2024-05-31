import pandas as pd
import sqlite3

# Load the CSV file
wage_distribution_file_path = 'data/wage_distribution.csv'
wage_distribution = pd.read_csv(wage_distribution_file_path, encoding='UTF-8')

# Convert byte-like objects to strings and then to integers for population_distribution
def convert_to_int(value):
    if isinstance(value, bytes):
        return int(value.decode('utf-8').strip())
    return int(value)

# Adjust the 'Laun' column in wage_distribution to remove dots and multiply by 1000
def adjust_wage_ranges(wage_range):
    parts = wage_range.replace('.', '').split('-')
    if len(parts) == 2:
        return f"{int(parts[0]) * 1000}-{int(parts[1]) * 1000}"
    else:  # For ranges that only have a lower bound (e.g., "1500-")
        return f"{int(parts[0]) * 1000}-inf"

wage_distribution['Laun'] = wage_distribution['Laun'].apply(adjust_wage_ranges)

# Convert the percentage columns to float, replace commas with dots, and divide by 100
for column in wage_distribution.columns[1:]:
    wage_distribution[column] = wage_distribution[column].str.replace(',', '.').astype(float) / 100

# Rename columns to be database and code safe
wage_distribution = wage_distribution.rename(columns={
    'Iðnaðarfólk': 'Industrials',
    'Sérfræðingar': 'Professionals',
    'Skrifstofufólk': 'OfficeStaff',
    'Stjórnendur': 'Managers',
    'Tæknar': 'Technicians',
    'Verkafólk': 'Laborers',
    'Þjónusta og umönnun': 'ServiceCare'
})

# Create SQLite database and table
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Drop the table if it exists
c.execute('DROP TABLE IF EXISTS wage_distribution')

# Create the wage_distribution table with columns for each occupation and the total percentage
columns = ', '.join([f"{col.replace(' ', '_').replace('ö', 'o').replace('Þ', 'Th')}" for col in wage_distribution.columns[1:]])
create_wage_table_query = f'''
    CREATE TABLE wage_distribution (
        wage_range TEXT,
        {columns} REAL
    )
'''

c.execute(create_wage_table_query)

# Insert the cleaned wage distribution data into the table using parameterized queries
for _, row in wage_distribution.iterrows():
    wage_range = row['Laun']
    values = [wage_range] + [row[col] for col in wage_distribution.columns[1:]]
    placeholders = ', '.join(['?'] * len(values))
    insert_wage_query = f'''
        INSERT INTO wage_distribution (wage_range, {', '.join(wage_distribution.columns[1:].str.replace(' ', '_').str.replace('ö', 'o').str.replace('Þ', 'Th'))})
        VALUES ({placeholders})
    '''
    c.execute(insert_wage_query, values)

# Commit and close connection
conn.commit()
conn.close()

print("Wage distribution data has been cleaned and inserted into the database.")

# Function to query data from the database
def query_wage_distribution(wage_range=None):
    conn = sqlite3.connect('income_data.db')
    query = 'SELECT * FROM wage_distribution WHERE 1=1'
    params = []
    if wage_range is not None:
        query += ' AND wage_range = ?'
        params.append(wage_range)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# Example query
print("Query result for wage range '1000000-1050000':")
result = query_wage_distribution(wage_range='1000000-1050000')
print(result)

# Close connection
conn.close()
