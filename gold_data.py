import pandas as pd
import sqlite3

def create_connection(db_path):
    conn = sqlite3.connect(db_path) 
    return conn

def bq1_gdp_growth(conn):
    query = """SELECT [Year], [Value] as gdp_growth_pct
FROM economic_data
WHERE [Indicator Code] = 'NY.GDP.MKTP.KD.ZG'
ORDER BY [Year];"""
    df = pd.read_sql(query,conn)
    return df

def bq2_data_completeness(conn):
    query = """
    SELECT 
        [Indicator Code],
        [Indicator Name],
        COUNT(DISTINCT [Year]) as years_with_data,
        (SELECT COUNT(DISTINCT [Year]) FROM economic_data) as total_possible_years,
        (COUNT(DISTINCT [Year]) * 100.0 / (SELECT COUNT(DISTINCT [Year]) FROM economic_data)) as completeness_pct
    FROM economic_data
    GROUP BY [Indicator Code], [Indicator Name]
    ORDER BY completeness_pct DESC
    LIMIT 10;
    """
    df = pd.read_sql(query,conn)
    return df


