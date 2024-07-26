import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('income_data.db')

# Fetch income data by gender and age
query = "SELECT * FROM income_by_gender_and_age WHERE age = 69"
income_df = pd.read_sql_query(query, conn)

# Display the income data for individuals aged 65 and older
print("Income Data for Individuals Aged 65 and Older:")
print(income_df)

# Close the connection
conn.close()
