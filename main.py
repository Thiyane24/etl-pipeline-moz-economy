from pipeline.extract import read_csv, add_audit_columns
from pipeline.transform import transform
from pipeline.load import create_connection, load_to_db
from gold_data import bq1_gdp_growth, bq2_data_completeness
from datetime import datetime
import pandas as pd 

#load the csv file

df = read_csv()

#create the load_id

load_id = datetime.now().strftime("%Y%m%d%H%M%S")

#add audit columns
df = add_audit_columns(df, load_id)

#print the first 10 rows to confirm
print(df.head(10))

print("\n")

print(f"DataFrame shape: {df.shape}")

print("\n")

print(f"Columns: {df.columns.tolist()}")
print("\n")

#saving it as a parquet file
filepath0 = "Data/bronze_raw/" + load_id + ".parquet"

df.to_parquet(filepath0)


#calling the transform function to clean the data and save it in the silver folder
df_cleaned = pd.read_parquet(f'Data/bronze_raw/{load_id}.parquet')

df_cleaned = transform(df_cleaned)

filepath1 = 'Data/silver/' + load_id + '.parquet'

df_cleaned.to_parquet(filepath1)


#Loading the silver file to DW in sqlite

conn = create_connection('database/economy.db')

load_to_db(df_cleaned, conn, 'economic_data')


#Check tables
check_query= "PRAGMA table_info(economic_data);"
columns_df = pd.read_sql(check_query,conn)
print(columns_df)
print('\n')

# Check if GDP indicator exists
check_gdp = pd.read_sql("""
SELECT DISTINCT [Indicator Code], [Indicator Name]
FROM economic_data
WHERE [Indicator Code] LIKE '%GDP%'
LIMIT 10;
""", conn)
print(check_gdp)
print('\n')

#Gold layer questions 1 and 2

gdp_growth = bq1_gdp_growth(conn)
print(gdp_growth)
print('\n')

completeness_df = bq2_data_completeness(conn)
print(completeness_df)

conn.close()

#Saving files as csv and parquet

completeness_df.to_csv('Data/gold/bq1_gdp_growth.csv', index=False)
completeness_df.to_parquet('Data/gold/bq1_gdp_growth.parquet')

completeness_df.to_csv('Data/gold/bq2_data_completeness.csv', index=False)
completeness_df.to_parquet('Data/gold/bq2_data_completeness.parquet')

