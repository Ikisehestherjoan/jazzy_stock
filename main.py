# Step 1: Import necessary libraries
import pandas as pd
import datetime
import psycopg2
from sqlalchemy import create_engine
from configparser import ConfigParser  # Corrected import statement

config = ConfigParser()
config.read('.env')

# Database credentials
db_user = config['DB_CONN']['db_user']
db_password = config['DB_CONN']['pass']  # Corrected variable name
db_host = config['DB_CONN']['db_host']
db_database = config['DB_CONN']['db_database']

# =================================================================================
# =================CREATING DATABASE CONNECTION====================================

# Step 2: Create a connection to the PostgreSQL database
conn = psycopg2.connect(
    user=db_user,
    password=db_password,
    host=db_host,
    database=db_database
    
)

# Step 3: Create a cursor to execute SQL queries
cur = conn.cursor()

# Uncomment the following section if you want to create the table
# Step 4: Create the tables in the database (Modify this based on your table structure)
create_table_query = '''
CREATE TABLE IF NOT EXISTS stock (
    Ticker VARCHAR(50),
    Name VARCHAR(50),
    Volume FLOAT,
    Price FLOAT,
    Change FLOAT,
    Date DATE
);
'''
cur.execute(create_table_query)

# Commit the changes to the database
conn.commit()

# ==========================================================================================================

# ===================================================================================================================

def extract_first_page(url_1):
    table_1 = pd.read_html(url_1)
    data_1 = table_1[3]
    Ticker = data_1['Ticker']
    Name = data_1['Name']
    Volume = data_1['Volume']
    Price = data_1['Price']
    Change = data_1['Change']
    Date = datetime.datetime.now()

    # Print the data (optional)
    print(Ticker, Name, Volume, Price, Change, Date)

    # Return the data as a DataFrame
    return pd.DataFrame({
        'Ticker': Ticker,
        'Name': Name,
        'Volume': Volume,
        'Price': Price,
        'Change': Change,
        'Date': Date
    })

result_df_1 = extract_first_page('https://afx.kwayisi.org/ngx/')

# Save the DataFrame to a CSV file (optional)
# result_df_1.to_csv('first_data.csv', index=False)

def extract_second_page(url_2):
    table_2 = pd.read_html(url_2)
    data_2 = table_2[3]
    Ticker = data_2['Ticker']
    Name = data_2['Name']
    Volume = data_2['Volume']
    Price = data_2['Price']
    Change = data_2['Change']
    Date = datetime.datetime.now()

    # Print the data (optional)
    print(Ticker, Name, Volume, Price, Change, Date)

    # Return the data as a DataFrame
    return pd.DataFrame({
        'Ticker': Ticker,
        'Name': Name,
        'Volume': Volume,
        'Price': Price,
        'Change': Change,
        'Date': Date
    })

result_df = extract_second_page('https://afx.kwayisi.org/ngx/?page=2')

# # Save the DataFrame to a CSV file (optional)
# result_df.to_csv('second_data.csv', index=False)

# # ==================================================================================================
# # ===========TRANSFORMATION STAGE ===========================================
# # CONCATENATING THE TWO EXTRACTED DATA AS ONE DATA
concatenated_data = pd.concat([result_df_1, result_df], ignore_index=True)

# # ===================================================================================================
# # For companies that do not have a value for volume traded for a particular
# # day, then the value for volume should be the mean volume of all
# # companies that traded that day.

# # DATA CLEANING
# # FILLING THE VOLUME COLUMN WITH THE MEAN
concatenated_data['Volume'].fillna(concatenated_data['Volume'].mean(), inplace=True)

# # concatenated_data.to_csv('merged_data.csv', index=False)

# ==================================DATA INGESTION TO POSTGRESQL====
# # Step 6: Insert the data into the PostgreSQL table
# # Make sure the column names in the DataFrame match the table columns
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}/{db_database}')
concatenated_data.to_sql('stock', engine, if_exists='replace', index=False)

# # Close the cursor and connection
cur.close()
conn.close()
