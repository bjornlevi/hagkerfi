import pandas as pd
import sqlite3

# Load the CSV files
consumption_file_path = 'data/cost_of_living.csv'
consumer_price_index_file_path = 'data/consumer_price_index.csv'

consumption_data = pd.read_csv(consumption_file_path, encoding='UTF-8')
consumer_price_index = pd.read_csv(consumer_price_index_file_path, encoding='UTF-8', delimiter=';')

# Remove the last three rows from the consumption data
consumption_data = consumption_data[:-3]

# Clean column names in consumption_data
consumption_data.columns = consumption_data.columns.str.strip()
consumption_data = consumption_data.rename(columns={
    'Flokkur': 'Category', 
    '2013-2016 Krónur Alls': 'Cost'
})

# Rename consumption categories to match consumer price index columns
category_rename_mapping = {
    '01 Matur og drykkjarvörur': 'FoodAndNonAlcoholicBeverages',
    '02 Áfengi og tóbak': 'AlcoholAndTobacco',
    '031 Föt': 'Clothing',
    '04 Húsnæði, hiti og rafmagn': 'HousingUtilities',
    '05 Húsgögn, heimilisbúnaður o.fl.': 'FurnishingsHouseholdEquipment',
    '06 Heilsa': 'Health',
    '07 Ferðir og flutningar': 'Transport',
    '08 Póstur og sími': 'Communication',
    '09 Tómstundir og menning': 'RecreationAndCulture',
    '10 Menntun': 'Education',
    '11 Hótel og veitingastaðir': 'HotelsRestaurants',
    '12 Aðrar vörur og þjónusta': 'MiscellaneousGoodsAndServices'
}

consumption_data['Category'] = consumption_data['Category'].map(category_rename_mapping)

# Adjust the consumption costs based on consumer price index
consumer_price_index = consumer_price_index.rename(columns={
    'Mánuður': 'Month',
    '01 Matur og drykkjarvörur': 'FoodAndNonAlcoholicBeverages',
    '02 Áfengi og tóbak': 'AlcoholAndTobacco',
    '031 Föt': 'Clothing',
    '04 Húsnæði, hiti og rafmagn': 'HousingUtilities',
    '05 Húsgögn, heimilisbúnaður o.fl.': 'FurnishingsHouseholdEquipment',
    '06 Heilsa': 'Health',
    '07 Ferðir og flutningar': 'Transport',
    '08 Póstur og sími': 'Communication',
    '09 Tómstundir og menning': 'RecreationAndCulture',
    '10 Menntun': 'Education',
    '11 Hótel og veitingastaðir': 'HotelsRestaurants',
    '12 Aðrar vörur og þjónusta': 'MiscellaneousGoodsAndServices'
})

cpi_2016 = consumer_price_index[consumer_price_index['Month'] == '2016M01'].set_index('Month').transpose()
cpi_2024 = consumer_price_index[consumer_price_index['Month'] == '2024M05'].set_index('Month').transpose()

cpi_2016.columns = ['CPI_2016']
cpi_2024.columns = ['CPI_2024']

# Merge the CPI data for 2016 and 2024 with consumption data
consumption_data = consumption_data.merge(cpi_2016, left_on='Category', right_index=True)
consumption_data = consumption_data.merge(cpi_2024, left_on='Category', right_index=True)

# Adjust the costs
consumption_data['AdjustedCost'] = consumption_data['Cost'] * (consumption_data['CPI_2024'] / consumption_data['CPI_2016'])

# Drop unnecessary columns after adjustment
consumption_data = consumption_data.drop(columns=['Cost', 'CPI_2016', 'CPI_2024'])

# Rename AdjustedCost to Cost for consistency
consumption_data = consumption_data.rename(columns={'AdjustedCost': 'Cost'})

# Create SQLite database and tables
conn = sqlite3.connect('income_data.db')
c = conn.cursor()

# Drop the table if it exists
c.execute('DROP TABLE IF EXISTS cost_of_living')

# Create the cost_of_living table
create_cost_of_living_table_query = '''
    CREATE TABLE cost_of_living (
        category TEXT,
        cost REAL
    )
'''

c.execute(create_cost_of_living_table_query)

# Insert the cleaned cost of living data into the table using parameterized queries
for _, row in consumption_data.iterrows():
    category = row['Category']
    cost = row['Cost']
    insert_cost_of_living_query = '''
        INSERT INTO cost_of_living (category, cost)
        VALUES (?, ?)
    '''
    c.execute(insert_cost_of_living_query, (category, cost))

# Commit and close connection
conn.commit()
conn.close()

print("Cost of living data has been inserted into the database.")

# Function to query cost of living data from the database
def query_cost_of_living_data(category=None):
    conn = sqlite3.connect('income_data.db')
    query = 'SELECT * FROM cost_of_living WHERE 1=1'
    params = []
    if category is not None:
        query += ' AND category = ?'
        params.append(category)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# Example query
print("Query result for cost of living category 'HousingUtilities':")
result = query_cost_of_living_data(category='HousingUtilities')
print(result)

# Close connection
conn.close()
