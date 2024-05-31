import pandas as pd
import sqlite3

# Load the CSV file
population_distribution_file_path = 'data/population_distribution.csv'
population_distribution = pd.read_csv(population_distribution_file_path, encoding='UTF-8', header=None, names=['Age', 'Population'])

# Convert byte-like objects to strings and then to integers for population_distribution
def convert_to_int(value):
    if isinstance(value, bytes):
        return int(value.decode('utf-8').strip())
    return int(value)

# Convert the Age and Population columns to integers
population_distribution['Age'] = population_distribution['Age'].apply(convert_to_int)
population_distribution['Population'] = population_distribution['Population'].apply(convert_to_int)

# Create SQLite database and table
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Drop the table if it exists
c.execute('DROP TABLE IF EXISTS population_distribution')

# Create the population_distribution table
create_population_table_query = '''
    CREATE TABLE population_distribution (
        age INTEGER,
        population INTEGER
    )
'''

c.execute(create_population_table_query)

# Insert the population distribution data into the table using parameterized queries
for _, row in population_distribution.iterrows():
    age = row['Age']
    population = row['Population']
    insert_population_query = '''
        INSERT INTO population_distribution (age, population)
        VALUES (?, ?)
    '''
    c.execute(insert_population_query, (convert_to_int(age), convert_to_int(population)))

# Commit and close connection
conn.commit()
conn.close()

print("Population distribution data has been inserted into the database.")
