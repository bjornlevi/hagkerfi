import pandas as pd
import sqlite3
import requests

# Define the API endpoint and the query
url = 'https://px.hagstofa.is:443/pxis/api/v1/is/Ibuar/mannfjoldi/1_yfirlit/Yfirlit_mannfjolda/MAN00101.px'
query = {
    "query": [
        {
            "code": "Kyn",
            "selection": {
                "filter": "item",
                "values": [
                    "Total"
                ]
            }
        },
        {
            "code": "Aldur",
            "selection": {
                "filter": "item",
                "values": [str(i) for i in range(110)]  # Ages from 0 to 109
            }
        },
        {
            "code": "Ár",
            "selection": {
                "filter": "item",
                "values": [
                    "2024"
                ]
            }
        }
    ],
    "response": {
        "format": "json"
    }
}

# Make the POST request to the API
response = requests.post(url, json=query)
data = response.json()

# Extract and prepare the data
records = []
for record in data['data']:
    age = int(record['key'][1])  # Extract and convert the age to an integer
    population = int(record['values'][0])  # Extract and convert the population to an integer
    records.append([age, population])

# Convert to DataFrame
population_distribution = pd.DataFrame(records, columns=['Age', 'Population'])

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
    # Explicitly convert values to integers to avoid byte-like entries
    age = int(row['Age'])
    population = int(row['Population'])
    insert_population_query = '''
        INSERT INTO population_distribution (age, population)
        VALUES (?, ?)
    '''
    c.execute(insert_population_query, (age, population))

# Commit and close connection
conn.commit()
conn.close()

print("Population distribution data has been inserted into the database.")
