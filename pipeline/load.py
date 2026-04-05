import pandas as pd
import sqlite3

def create_connection(db_path):
    conn = sqlite3.connect(db_path) 
    return conn

def load_to_db(df,conn,table_name):
    df.to_sql(table_name, conn, if_exists = 'replace')
    return df