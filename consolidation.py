import pandas as pd
import numpy as np
import os
import dask.dataframe as dd



def map_activity(row):
    if row['ACTIVITY_CODE_VERSION'] == 'NAP':
        return dic_version_1973.get(row['MAIN_ACTIVITY_LEVEL_5'])
    elif row['ACTIVITY_CODE_VERSION'] == 'NAF1993':
        return dic_version_1993.get(row['MAIN_ACTIVITY_LEVEL_5'])
    elif row['ACTIVITY_CODE_VERSION'] == 'NAFRev1':
        return dic_version_2003.get(row['MAIN_ACTIVITY_LEVEL_5'])
    elif row['ACTIVITY_CODE_VERSION'] == 'NAFRev2':
        return dic_version_2008.get(row['MAIN_ACTIVITY_LEVEL_5'])
    else:
        return np.nan

def map_levels(row, level, levels_list):
    try:
        if row['ACTIVITY_CODE_VERSION'] == 'NAP':
            return levels_list[0].loc[row['MAIN_ACTIVITY_LEVEL_5'], level]
        elif row['ACTIVITY_CODE_VERSION'] == 'NAF1993':
            return levels_list[1].loc[row['MAIN_ACTIVITY_LEVEL_5'], level]
        elif row['ACTIVITY_CODE_VERSION'] == 'NAFRev1':
            return levels_list[2].loc[row['MAIN_ACTIVITY_LEVEL_5'], level]
        elif row['ACTIVITY_CODE_VERSION'] == 'NAFRev2':
            return levels_list[3].loc[row['MAIN_ACTIVITY_LEVEL_5'], level]
        else:
            return np.nan
    except:
        return np.nan

def assign_name(row):
    if pd.notna(row['CONTRACTING_SIREN']) and row['CONTRACTING_SIREN'] in institutions.index:
        row['CONTRACTING_SIREN_NAME'] = institutions.loc[row['CONTRACTING_SIREN'], 'NAME']
    if pd.notna(row['AWARDED_SIREN']) and row['AWARDED_SIREN'] in institutions.index:
        row['AWARDED_SIREN_NAME'] = institutions.loc[row['AWARDED_SIREN'], 'NAME']
    return row


path = '' # your directory
os.chdir(path)


# Import of the data including estimated SIRENs

df = pd.read_parquet('data_with_siren.parquet')


# List of SIRENs found in the data

siren_list = list(
    set(
        x for x in pd.concat([df['AWARDED_SIREN'], df['CONTRACTING_SIREN']]).tolist()
        if str(x).lower() != "nan" and str(x) != "None"
    )
)


# Read national datasets on economic agents

legal_status = pd.read_csv('legal_status.csv', sep=';', encoding='latin1', dtype='str')
dictionnaire_legal = legal_status.set_index('categorie_juridique_insee')['libelle'].to_dict()

staff_status = pd.read_csv('staff_status.csv', sep=';')
dictionnaire_staff = staff_status.set_index('code')['size'].to_dict()

codes_1973 = pd.read_csv('level_names_1973.csv', sep=';')
codes_1993 = pd.read_csv('level_names_1993.csv', sep=';')
codes_2003 = pd.read_csv('level_names_2003.csv', sep=';')
codes_2008 = pd.read_csv('level_names_2008.csv', sep=';')

dic_version_1973 = codes_1973.set_index('code')['name'].to_dict()
dic_version_1993 = codes_1993.set_index('code')['name'].to_dict()
dic_version_2003 = codes_2003.set_index('code')['name'].to_dict()
dic_version_2008 = codes_2008.set_index('code')['name'].to_dict()

levels_1973 = pd.read_csv('Levels_1973.csv', sep=';')
levels_1993 = pd.read_csv('Levels_1993.csv', sep=';')
levels_2003 = pd.read_csv('Levels_2003.csv', sep=';')
levels_2008 = pd.read_csv('Levels_2008.csv', sep=';')

levels_list = [levels_1973, levels_1993, levels_2003, levels_2008]
for i in range(len(levels_list)):
    levels_list[i] = levels_list[i].astype(str)
    levels_list[i] = levels_list[i].set_index('NIV5')

columns_to_read = [
    'denominationUniteLegale',
    'siren',
    'dateCreationUniteLegale',
    'trancheEffectifsUniteLegale',
    'categorieEntreprise',
    'societeMissionUniteLegale',
    'etatAdministratifUniteLegale',
    'categorieJuridiqueUniteLegale',
    'activitePrincipaleUniteLegale',
    'economieSocialeSolidaireUniteLegale',
    'nomenclatureActivitePrincipaleUniteLegale'
]

ddf = dd.read_parquet("StockUniteLegale_utf8.parquet", columns=columns_to_read)
institutions = ddf[ddf["siren"].isin(siren_list)].compute()


# Add variables to the dataframe with SIRENs

column_names_institutions = {
                         'categorieJuridiqueUniteLegale': 'LEGAL_STATUS',
                         'activitePrincipaleUniteLegale': 'MAIN_ACTIVITY_LEVEL_5',
                         'economieSocialeSolidaireUniteLegale': 'SSE',
                         'societeMissionUniteLegale': 'MISSION',
                         'dateCreationUniteLegale': 'CREATION_DATE',
                         'trancheEffectifsUniteLegale': 'STAFF',
                         'categorieEntreprise': 'SIZE',
                         'nomenclatureActivitePrincipaleUniteLegale': 'ACTIVITY_CODE_VERSION',
                         'denominationUniteLegale': 'NAME'
                         }
institutions = institutions.rename(columns=column_names_institutions)

institutions['SSE'] = institutions['SSE'].replace({"O": True, "N": False})

institutions['MISSION'] = institutions['MISSION'].replace({"O": True, "N": False})

institutions['LEGAL_STATUS_NAME'] = institutions['LEGAL_STATUS'].astype(str).map(dictionnaire_legal)

institutions['STAFF'] = pd.to_numeric(institutions['STAFF'], errors='coerce').astype('Int64')

institutions['STAFF'] = institutions['STAFF'].map(dictionnaire_staff)

institutions['MAIN_ACTIVITY_NAME'] = institutions.apply(map_activity, axis=1)

institutions['MAIN_ACTIVITY_LEVEL_1'] = institutions.apply(map_levels, args=('NIV1', levels_list), axis=1)
institutions['MAIN_ACTIVITY_LEVEL_2'] = institutions.apply(map_levels, args=('NIV2', levels_list), axis=1)
institutions['MAIN_ACTIVITY_LEVEL_3'] = institutions.apply(map_levels, args=('NIV3', levels_list), axis=1)
institutions['MAIN_ACTIVITY_LEVEL_4'] = institutions.apply(map_levels, args=('NIV4', levels_list), axis=1)

translation_versions = {
    'NAP': '1973',
    'NAF1993': '1993',
    'NAFRev1': '2003',
    'NAFRev2': '2008'}
institutions['ACTIVITY_CODE_VERSION'] = institutions['ACTIVITY_CODE_VERSION'].replace(translation_versions)

institutions = institutions.set_index('siren')

df = df.apply(assign_name, axis=1)

df['CONTRACTING_LEGAL_STATUS'] = df['CONTRACTING_SIREN'].map(institutions['LEGAL_STATUS'])

df['CONTRACTING_LEGAL_STATUS_NAME'] = df['CONTRACTING_SIREN'].map(institutions['LEGAL_STATUS_NAME'])

df['CONTRACTING_STAFF'] = df['CONTRACTING_SIREN'].map(institutions['STAFF'])

df['CONTRACTING_MAIN_ACTIVITY'] = df['CONTRACTING_SIREN'].map(institutions['MAIN_ACTIVITY_NAME'])

df['CONTRACTING_SSE'] = df['CONTRACTING_SIREN'].map(institutions['SSE'])

df['CONTRACTING_CREATION_DATE'] = df['CONTRACTING_SIREN'].map(institutions['CREATION_DATE'])

df['CONTRACTING_MAIN_ACTIVITY_CODE'] = df['CONTRACTING_SIREN'].map(institutions['MAIN_ACTIVITY_LEVEL_5'])

df['CONTRACTING_ACTIVITY_VERSION'] = df['CONTRACTING_SIREN'].map(institutions['ACTIVITY_CODE_VERSION'])

df['AWARDED_LEGAL_STATUS'] = df['AWARDED_SIREN'].map(institutions['LEGAL_STATUS'])

df['AWARDED_LEGAL_STATUS_NAME'] = df['AWARDED_SIREN'].map(institutions['LEGAL_STATUS_NAME'])

df['AWARDED_STAFF'] = df['AWARDED_SIREN'].map(institutions['STAFF'])

df['AWARDED_SSE'] = df['AWARDED_SIREN'].map(institutions['SSE'])

df['AWARDED_MISSION'] = df['AWARDED_SIREN'].map(institutions['MISSION'])

df['AWARDED_ACTIVITY_VERSION'] = df['AWARDED_SIREN'].map(institutions['ACTIVITY_CODE_VERSION'])

df['AWARDED_ACTIVITY_LEVEL_1'] = df['AWARDED_SIREN'].map(institutions['MAIN_ACTIVITY_LEVEL_1'])

df['AWARDED_ACTIVITY_LEVEL_2'] = df['AWARDED_SIREN'].map(institutions['MAIN_ACTIVITY_LEVEL_2'])

df['AWARDED_ACTIVITY_LEVEL_3'] = df['AWARDED_SIREN'].map(institutions['MAIN_ACTIVITY_LEVEL_3'])

df['AWARDED_ACTIVITY_LEVEL_4'] = df['AWARDED_SIREN'].map(institutions['MAIN_ACTIVITY_LEVEL_4'])

df['AWARDED_ACTIVITY_LEVEL_5'] = df['AWARDED_SIREN'].map(institutions['MAIN_ACTIVITY_LEVEL_5'])

df['AWARDED_MAIN_ACTIVITY'] = df['AWARDED_SIREN'].map(institutions['MAIN_ACTIVITY_NAME'])

df['AWARDED_CREATION_DATE'] = df['AWARDED_SIREN'].map(institutions['CREATION_DATE'])

df['CONTRACTING_STAFF'] = df['CONTRACTING_STAFF'].apply(lambda x: x.lstrip() if isinstance(x, str) else x)

df['AWARDED_STAFF'] = df['AWARDED_STAFF'].apply(lambda x: x.lstrip() if isinstance(x, str) else x)


# Release memory and export the consolidated dataframe

del institutions

df = df.astype(str)
df = df.replace("None", "nan")
df.to_parquet("consolidated_data.parquet")

