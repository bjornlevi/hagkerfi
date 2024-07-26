import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Function to fetch a summary for each occupation
def fetch_occupation_summary():
    # Fetch distinct occupations from the population table
    distinct_occupations_query = 'SELECT DISTINCT occupation FROM population WHERE occupation != "Alls"'
    distinct_occupations = pd.read_sql_query(distinct_occupations_query, conn)
    occupations = distinct_occupations['occupation'].tolist()

    # Fetch wage ranges from the wage_distribution table
    wage_ranges_query = '''
        SELECT 
            wage_range,
            CAST(SUBSTR(wage_range, 1, INSTR(wage_range, '-') - 1) AS INTEGER) as min_income,
            CASE 
                WHEN INSTR(wage_range, '-') = LENGTH(wage_range) THEN 10000000
                ELSE CAST(SUBSTR(wage_range, INSTR(wage_range, '-') + 1) AS INTEGER)
            END as max_income
        FROM wage_distribution
    '''
    wage_ranges = pd.read_sql_query(wage_ranges_query, conn)

    # Iterate through each occupation and wage range, displaying the summary
    for occupation in occupations:
        print(f"\nSummary for Occupation: {occupation}")
        summary = []
        for _, row in wage_ranges.iterrows():
            wage_range = row['wage_range']
            min_income = row['min_income']
            max_income = row['max_income']

            query = f'''
                SELECT 
                    '{wage_range}' as wage_range,
                    COUNT(*) as count, 
                    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM population WHERE occupation = '{occupation}')) as percentage
                FROM population
                WHERE occupation = '{occupation}'
                AND wages BETWEEN {min_income} AND {max_income}
            '''
            result = pd.read_sql_query(query, conn)
            count = result['count'].iloc[0]
            percentage = result['percentage'].iloc[0]

            summary.append((wage_range, count, percentage))

        summary_df = pd.DataFrame(summary, columns=['wage_range', 'count', 'percentage'])
        print(summary_df)

# Execute the function to fetch and display the summary
fetch_occupation_summary()

# Close connection
conn.close()
