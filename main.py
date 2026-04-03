from pipeline.extract import read_csv, add_audit_columns
from datetime import datetime

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
filepath = "Data/bronze_raw/" + load_id + ".parquet"

df.to_parquet(filepath)