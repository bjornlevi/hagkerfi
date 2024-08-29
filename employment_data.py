import requests
import json
import pandas as pd
import sqlite3
import numpy as np

# Define the API endpoint and the query
api_url = "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/vinnumarkadur/vinnuaflskraargogn/VIN10001.px"
query = {
  "query": [
    {
      "code": "Mánuður",
      "selection": {
        "filter": "item",
        "values": [
          "2024M06"
        ]
      }
    },
    {
      "code": "Kyn",
      "selection": {
        "filter": "item",
        "values": [
          "1",
          "2"
        ]
      }
    },
    {
      "code": "Aldursflokkar",
      "selection": {
        "filter": "item",
        "values": [
          "Yngri en 15 ára",
          "15 til 19 ára",
          "20 til 24 ára",
          "25 til 29 ára",
          "30 til 34 ára",
          "35 til 39 ára",
          "40 til 44 ára",
          "45 til 49 ára",
          "50 til 54 ára",
          "55 til 59  ára",
          "60 til 64 ára",
          "65 til 69 ára",
          "70 ára og eldri"
        ]
      }
    },
    {
      "code": "Uppruni",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "Lögheimili",
      "selection": {
        "filter": "item",
        "values": [
          "0"
        ]
      }
    }
  ],
  "response": {
    "format": "json"
  }
}

# Fetch the data from the API
response = requests.post(api_url, json=query)
data = response.json()

# Extract data
records = []
for item in data['data']:
    month, gender, age_group, _, _ = item['key']
    employed = int(item['values'][0])
    gender = 'male' if gender == '1' else 'female'
    records.append({'month': month, 'gender': gender, 'age_group': age_group, 'employed': employed})

# Convert to DataFrame
df = pd.DataFrame(records)

print(df)

# Function to expand employment data
def expand_employment_data(df, gender):
    expanded_employment = []

    for _, row in df.iterrows():
        age_group = row['age_group']
        employment = float(row['employed'])

        if age_group == "Yngri en 15 ára":
            age_distribution = {13: 0.5, 14: 0.5}  # Use ages 13 and 14
        elif age_group == "70 ára og eldri":
            age_distribution = {70: 0.4, 71: 0.3, 72: 0.2, 73: 0.1, 74: 0.05, 75: 0.025, 76: 0.01, 77: 0.005, 78: 0.0025, 79: 0.001, 80: 0}
        else:
            start_age, end_age = [int(s) for s in age_group.split() if s.isdigit()]
            age_distribution = {age: 1/(end_age - start_age + 1) for age in range(start_age, end_age + 1)}

        for age, weight in age_distribution.items():
            employed_count = int(employment * weight)
            expanded_employment.append({"age": age, "gender": gender, "employed": employed_count})

    return pd.DataFrame(expanded_employment)

# Expand the employment data
expanded_male_df = expand_employment_data(df[df['gender'] == 'male'], 'male')
expanded_female_df = expand_employment_data(df[df['gender'] == 'female'], 'female')

# Combine male and female data
expanded_df = pd.concat([expanded_male_df, expanded_female_df])

# Save the expanded data to SQLite
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Create the employment table
c.execute('DROP TABLE IF EXISTS employment_data')
create_employment_table_query = '''
    CREATE TABLE employment_data (
        age INTEGER,
        gender TEXT,
        employed INTEGER
    )
'''
c.execute(create_employment_table_query)

# Insert the expanded data
insert_employment_query = '''
    INSERT INTO employment_data (age, gender, employed)
    VALUES (?, ?, ?)
'''
for _, row in expanded_df.iterrows():
    c.execute(insert_employment_query, (row['age'], row['gender'], row['employed']))

# Commit and close the connection
conn.commit()
conn.close()

print("Employment data has been expanded and inserted into the database.")