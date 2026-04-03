import pandas as pd
import hashlib
from datetime import datetime


def read_csv():
    df = pd.read_csv('Data/messy_economy_moz (1).csv', dtype = str)
    return df
    
    
def add_audit_columns(df,load_id):
    
    #hashing function using hashlib and sha256
    def hash_row(row):
        combined = str(row['Country ISO3']) + str(row['Year']) + str(row['Indicator Code']) + str(row['Value']) + str(row['Indicator Name']) + str(row['ref_date'])
        hash_value = hashlib.sha256(combined.encode()).hexdigest()
        return hash_value
    
    #apply the hash to all function rows
    df['_row_hash']= df.apply(hash_row,axis=1)
    
    #add _load_timestamp
    df['_load_timestamp'] = datetime.now()
    
    #add _load_id
    df['_load_id'] = load_id
    
    return df