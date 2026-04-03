import pandas as pd 

#clean ISO3
def clean_iso3(df):
    df['Country ISO3'] = df['Country ISO3'].str.replace('0','O')
    return df

#clean country name
def clean_country_name(df):
    df['Country Name'] = df['Country Name'].str.capitalize().str.strip()
    return df

#clean indicator name
def clean_indicator_name(df):
    df['Indicator Name'] = df['Indicator Name'].str.replace('"','').str.strip()
    return df

#year clean and extracted in digits
def clean_year(df):
    def extract_year(value):
        try:
            year = int(float(value))
            return year
        except:
            parts = str(value).replace('/', ' ').replace('-', ' ').split()
            for part in parts:
                if len(part) == 4 and part.isdigit() and 1900 <= int(part) <= 2100:
                    return int(part)
            return None  
    
    df['Year'] = df['Year'].apply(extract_year)
    return df


#dropping missing values
def drop_missing_values(df):
    df = df.dropna(subset= ['Value', 'data_entry_by'])
    df = df[['Country Name', 'Country ISO3', 'Year', 'Indicator Name',
       'Indicator Code', 'Value', 'data_entry_by', 'upload_ts']]
    return df

def transform(df):
    #first drop then clean the data
   df = drop_missing_values(df)
   df = clean_iso3(df)
   df = clean_country_name(df)
   df = clean_indicator_name(df)
   df = clean_year(df)
   return df
    
    
    
    
