import pandas as pd
import os

path = '' # your directory
os.chdir(path)


# Import the database as of yesterday

past = pd.read_parquet('past_data.parquet')


# Import today's information

df = pd.read_parquet('data_world_geolocated.parquet')


# Import today's award notices and find the ones that are amending past award notices

award_notices = pd.read_parquet('award_day.parquet')

rectificatifs = award_notices[
    award_notices['etat'] == 'RECTIFICATIF'
]


# Update past data in case of an amendment

for _, row in rectificatifs.iterrows():
    ids_to_remove = str(row['annonce_lie']).replace(' ', ',').replace(';', ',').split(',')
    ids_to_remove = [id_.strip() for id_ in ids_to_remove if id_.strip()]
    past = past[~past['ID_BOAMP_AWARD'].isin(ids_to_remove)]


# Merge past and today's data

merged = pd.concat([past, df], ignore_index=True)
merged = merged.drop_duplicates()


# Export the final dataframe

merged = merged.astype(str)
merged = merged.replace("None", "nan")
merged.to_parquet('final_data.parquet')
