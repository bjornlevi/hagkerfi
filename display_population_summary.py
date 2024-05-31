import sqlite3
import pandas as pd
import random

# Connect to the SQLite database
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Function to fetch random samples from the population table
def fetch_random_samples(n=10):
    query = 'SELECT * FROM population ORDER BY RANDOM() LIMIT ?'
    df = pd.read_sql_query(query, conn, params=(n,))
    return df

# Function to fetch summary of one occupation
def fetch_occupation_summary(occupation):
    query = f'''
        SELECT 
            CASE 
                WHEN p.total_income BETWEEN wd.min_income AND wd.max_income THEN wd.wage_range 
            END AS wage_range,
            COUNT(*) as count, 
            COUNT(*) * 100.0 / (SELECT COUNT(*) FROM population WHERE occupation = '{occupation}') as percentage
        FROM population p
        JOIN wage_distribution wd ON p.total_income BETWEEN wd.min_income AND wd.max_income
        WHERE p.occupation = '{occupation}'
        GROUP BY wage_range
        ORDER BY wage_range
    '''
    df = pd.read_sql_query(query, conn)
    return df

# Fetch the wage distribution data from the database
wage_distribution_query = 'SELECT * FROM wage_distribution'
wage_distribution = pd.read_sql_query(wage_distribution_query, conn)

# Pick a random occupation
occupation_to_summarize = random.choice(wage_distribution.columns[1:])

# Fetch random samples from the population
random_samples = fetch_random_samples(10)
print("Random Samples from Population:")
print(random_samples)

# Fetch summary for the randomly picked occupation
occupation_summary = fetch_occupation_summary(occupation_to_summarize)
print(f"\nSummary for Occupation: {occupation_to_summarize}")
print(occupation_summary)

# Close connection
conn.close()
