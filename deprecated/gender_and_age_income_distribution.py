import requests
import pandas as pd
import sqlite3

# API endpoint and query
url = "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/launogtekjur/3_tekjur/1_tekjur_skattframtol/TEK01001.px"
query = {
    "query": [
        {
            "code": "Eining",
            "selection": {
                "filter": "item",
                "values": ["0"]
            }
        },
        {
            "code": "Kyn",
            "selection": {
                "filter": "item",
                "values": ["1", "2"]
            }
        },
        {
            "code": "Aldur",
            "selection": {
                "filter": "item",
                "values": [
                    "16", "20", "25", "30", "35", "40", "45", "50", 
                    "55", "60", "65", "70", "75", "80", "85+"
                ]
            }
        },
        {
            "code": "Ár",
            "selection": {
                "filter": "item",
                "values": ["2023"]
            }
        }
    ],
    "response": {"format": "json"}
}

# Send the request to the API
response = requests.post(url, json=query)
data = response.json()

# Prepare to collect and structure the data
rows = []
income_categories = {
    "0": "Heildartekjur",
    "1": "Atvinnutekjur",
    "2": "Fjármagnstekjur",
    "3": "Aðrar tekjur",
    "4": "Skattar á greiðslugrunni",
    "5": "Ráðstöfunartekjur"
}

# Process each entry in the data
for entry in data['data']:
    category_code = entry['key'][0]  # The 'Tekjur og skattar' category code
    gender = entry['key'][2]  # Gender: 1 for male, 2 for female
    age_group = entry['key'][3]  # Age group
    value = float(entry['values'][0]) * 1000  # Convert from thousands to ISK
    
    # Create a dictionary entry for the row
    row = {
        "Aldur": age_group,
        "Kyn": gender,
        income_categories[category_code]: value
    }
    rows.append(row)

# Convert the list of rows into a DataFrame
df = pd.DataFrame(rows)

# Pivot the DataFrame so that income categories become columns
df_pivot = df.pivot_table(index=["Aldur", "Kyn"], 
                          values=list(income_categories.values()), 
                          aggfunc="first").reset_index()

# Replace gender codes with readable labels
df_pivot['Kyn'] = df_pivot['Kyn'].replace({"1": "Karlar", "2": "Konur"})

# Handle "85+" age group
df_pivot['Aldur'] = df_pivot['Aldur'].replace({"85+": 85})
df_pivot['Aldur'] = df_pivot['Aldur'].astype(int)

# Sort by age and gender to ensure order
df_pivot.sort_values(by=['Kyn', 'Aldur'], inplace=True)

# Separate numeric and categorical columns
numeric_columns = list(income_categories.values())

# Ensure all numeric columns are properly typed
df_pivot[numeric_columns] = df_pivot[numeric_columns].apply(pd.to_numeric)

# Interpolate the missing ages without altering categorical columns
complete_ages = []
for gender in df_pivot['Kyn'].unique():
    sub_df = df_pivot[df_pivot['Kyn'] == gender].copy()  # Explicitly copy the DataFrame
    
    # Reindex to include all ages between 1 and 109
    sub_df = sub_df.set_index('Aldur').reindex(range(1, 110))
    
    # Interpolate numeric columns
    sub_df[numeric_columns] = sub_df[numeric_columns].interpolate(method='linear')
    
    # Set income for ages 1 to 15 to 0
    sub_df.loc[1:15, numeric_columns] = 0
    
    # Set the income for ages 85 to 109 to the value at age 85
    sub_df.loc[85:109, numeric_columns] = sub_df.loc[85, numeric_columns].values
    
    # Fill the 'Kyn' column without inplace=True to avoid the warning
    sub_df['Kyn'] = sub_df['Kyn'].fillna(gender)
    
    # Reset the index
    sub_df = sub_df.reset_index()
    
    # Append to the list
    complete_ages.append(sub_df)

# Concatenate the complete DataFrames for both genders
df_complete = pd.concat(complete_ages).reset_index(drop=True)

# Ensure correct column order for the database insertion
df_complete = df_complete[[
    "Aldur", "Kyn", "Heildartekjur", "Atvinnutekjur", 
    "Fjármagnstekjur", "Aðrar tekjur", "Skattar á greiðslugrunni", "Ráðstöfunartekjur"
]]

# Connect to SQLite database and save the data
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Drop the table if it exists
c.execute('DROP TABLE IF EXISTS gender_and_age_income_distribution')

# Create the gender_and_age_income_distribution table with all expected columns
create_table_query = '''
    CREATE TABLE gender_and_age_income_distribution (
        Aldur INTEGER,
        Kyn TEXT,
        Heildartekjur REAL,
        Atvinnutekjur REAL,
        Fjármagnstekjur REAL,
        Aðrar_tekjur REAL,
        Skattar REAL,
        Ráðstöfunartekjur REAL
    )
'''
c.execute(create_table_query)

# Insert the data with correct column mapping
for _, row in df_complete.iterrows():
    insert_query = '''
        INSERT INTO gender_and_age_income_distribution (Aldur, Kyn, Heildartekjur, Atvinnutekjur, Fjármagnstekjur, Aðrar_tekjur, Skattar, Ráðstöfunartekjur)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    '''
    c.execute(insert_query, tuple(row))

# Commit and close the connection
conn.commit()
conn.close()

print("Income distribution data has been inserted into the gender_and_age_income_distribution table.")
