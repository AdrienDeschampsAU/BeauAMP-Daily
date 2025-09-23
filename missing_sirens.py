import os
import pandas as pd
    
path = '' # your directory
os.chdir(path)


# Import processed data

df = pd.read_parquet('processed_data.parquet')


# Select awarded firms with a mentioned name and address but no SIREN

awarded_missing = df[(df['AWARDED_SIREN'] == "nan") & (df['AWARDED_ADDRESS'] != "nan") & (df['AWARDED_STATED_NAME'] != "nan")][['AWARDED_STATED_NAME', 'AWARDED_ADDRESS']].drop_duplicates()
awarded_missing = awarded_missing.astype(str)
awarded_missing.to_parquet('awarded_missing_siren.parquet', index=False)


# Select contracting authorities with a mentioned name and address but no SIREN

contracting_missing = df[(df['CONTRACTING_SIREN'] == "nan") & (df['CONTRACTING_ADDRESS'] != "nan") & (df['CONTRACTING_STATED_NAME'] != "nan")][['CONTRACTING_STATED_NAME', 'CONTRACTING_ADDRESS']].drop_duplicates()
contracting_missing = contracting_missing.astype(str)
contracting_missing.to_parquet('contracting_missing_siren.parquet', index=False)
