import pandas as pd
import os
import requests
import re
from datetime import datetime, timedelta

path = '/home/adminuser/boamp/files'
os.chdir(path)

df = pd.read_parquet('data_world_geolocated.parquet')

trad_dict = {
    "ACCELERATED": "procedure_acceleree",
    "ADDITIONAL_CPV": "cpv_supp",
    "ADVERTISING": "publicite",
    "AGP": "omc",
    "ALLOTMENT": "allotissement",
    "AMENDED": "modifie",
    "AWARDED": "decision",
    "AWARDED_ADDRESS": "adresse_fournisseur",
    "AWARDED_SIREN": "siren_fournisseur",
    "AWARDED_SIREN_NAME": "nom_siren_fournisseur",
    "AWARDED_STATED_NAME": "nom_declare_fournisseur",
    "AWARD_NOTICE_DATE": "date_avis_attribution",
    "CONTRACTING_ADDRESS": "adresse_acheteur",
    "CONTRACTING_SIREN": "siren_acheteur",
    "CONTRACTING_SIREN_NAME": "nom_siren_acheteur",
    "CONTRACTING_STATED_NAME": "nom_declare_acheteur",
    "CONTRACT_NOTICE_DATE": "date_avis_marche",
    "CONTRACT_START": "debut_contrat",
    "CONTRACT_TYPE": "type_contrat",
    "CRITERION_NAME": "intitule_critere",
    "CRITERION_TYPE": "type_critere",
    "CRITERION_WEIGHT": "poids_critere",
    "DURATION": "duree",
    "ENVIRONMENTAL_CLAUSE": "clause_environnementale",
    "ESTIMATED_TOTAL_VALUE": "valeur_totale_estimee",
    "EU_FUNDED": "financement_ue",
    "EXECUTION_SITE": "lieu_execution",
    "FRAMEWORK_AGREEMENT_TYPE": "type_accord_cadre",
    "FUNDS_NAME": "nom_fonds_ue",
    "ID_BOAMP_AWARD": "id_boamp_attribution",
    "ID_BOAMP_CONTRACT": "id_boamp_contrat",
    "LOT_AWARDED_PRICE": "prix_attribution_lot",
    "LOT_ESTIMATED_VALUE": "valeur_estimee_lot",
    "LOT_ID": "id_lot",
    "MAIN_CPV": "cpv",
    "MAX_TOTAL_VALUE_FRAMEWORK_AGREEMENT": "valeur_max_totale_accord_cadre",
    "NUMBER_LOTS": "nombre_lots",
    "NUMBER_OFFERS": "nombre_offres",
    "NUMBER_OFFERS_SME": "nombre_offres_pme",
    "OBJECT": "objet",
    "PROCEDURE_TYPE": "procedure",
    "PROCUREMENT_PROJECT_ID": "id_projet",
    "RENEWAL": "renouvellement",
    "RESERVED": "marche_reserve",
    "SME_FRIENDLY": "favorable_pme",
    "SOCIAL_CLAUSE": "clause_sociale",
    "STRATEGIC_ENVIRONMENTAL": "strategique_environnemental",
    "STRATEGIC_SOCIAL": "strategique_social",
    "TOTAL_VALUE": "valeur_totale",
    "CONTRACTING_LEGAL_STATUS": "code_statut_juridique_acheteur",
    "CONTRACTING_LEGAL_STATUS_NAME": "nom_statut_juridique_acheteur",
    "CONTRACTING_STAFF": "effectif_acheteur",
    "CONTRACTING_MAIN_ACTIVITY": "activite_principale_acheteur",
    "CONTRACTING_SSE": "acheteur_ess",
    "CONTRACTING_CREATION_DATE": "date_creation_acheteur",
    "CONTRACTING_MAIN_ACTIVITY_CODE": "code_activite_principale_acheteur",
    "CONTRACTING_ACTIVITY_VERSION": "version_activite_acheteur",
    "AWARDED_LEGAL_STATUS": "code_statut_juridique_fournisseur",
    "AWARDED_LEGAL_STATUS_NAME": "nom_statut_juridique_fournisseur",
    "AWARDED_STAFF": "effectif_fournisseur",
    "AWARDED_SSE": "fournisseur_ess",
    "AWARDED_MISSION": "fournisseur_mission",
    "AWARDED_ACTIVITY_VERSION": "version_activite_fournisseur",
    "AWARDED_ACTIVITY_LEVEL_1": "niveau_activite1_fournisseur",
    "AWARDED_ACTIVITY_LEVEL_2": "niveau_activite2_fournisseur",
    "AWARDED_ACTIVITY_LEVEL_3": "niveau_activite3_fournisseur",
    "AWARDED_ACTIVITY_LEVEL_4": "niveau_activite4_fournisseur",
    "AWARDED_ACTIVITY_LEVEL_5": "niveau_activite5_fournisseur",
    "AWARDED_MAIN_ACTIVITY": "activite_principale_fournisseur",
    "AWARDED_CREATION_DATE": "date_creation_fournisseur",
    "AWARDED_GPS": "gps_fournisseur",
    "CONTRACTING_GPS": "gps_acheteur",
    "AWARDED_CITY_OFFICIAL": "nom_commune_fournisseur",
    "AWARDED_DEPARTMENT": "departement_fournisseur",
    "AWARDED_REGION": "region_fournisseur",
    "AWARDED_CITY_CODE": "code_commune_fournisseur",
    "AWARDED_EPCI": "epci_fournisseur",
    "AWARDED_LIBEPCI": "nom_epci_fournisseur",
    "AWARDED_NATURE_EPCI": "nature_epci_fournisseur",
    "CONTRACTING_CITY_OFFICIAL": "nom_commune_acheteur",
    "CONTRACTING_DEPARTMENT": "departement_acheteur",
    "CONTRACTING_REGION": "region_acheteur",
    "CONTRACTING_CITY_CODE": "code_commune_acheteur",
    "CONTRACTING_EPCI": "epci_acheteur",
    "CONTRACTING_LIBEPCI": "nom_epci_acheteur",
    "CONTRACTING_NATURE_EPCI": "nature_epci_acheteur",
    "AWARDED_SIREN_MENTIONED": "siren_fournisseur_connu",
    "CONTRACTING_SIREN_MENTIONED": "siren_acheteur_connu",
}

df = df.rename(columns=trad_dict)

df['decision'] = df['decision'].replace({
    "selected": "attribue",
    "no award": "non attribue",
    "still open": "en cours"
})

df['type_contrat'] = df['type_contrat'].replace({
    "works": "travaux",
    "services": "services",
    "supplies": "fournitures"
})

def trad_type_critere(val):
    val = re.sub(r'ENVIRONMENTAL', 'ENVIRONNEMENTAL', val)
    val = re.sub(r'DELAY', 'DELAI', val)
    val = re.sub(r'PRICE', 'PRIX', val)
    val = re.sub(r'QUALITY', 'QUALITE', val)
    val = re.sub(r'TECHNICAL', 'TECHNIQUE', val)
    return val.lower()

df['type_critere'] = df['type_critere'].astype(str).apply(trad_type_critere)

df['type_accord_cadre'] = df['type_accord_cadre'].replace({
    "no competition": "sans remise en concurrence",
    "mixed": "hybride",
    "competition": "avec remise en concurrence"
})

df['procedure'].replace({
    'open':'ouverte',
    'negotiated':'negociee',
    'no competition':'sans concurrence',
    'competitive dialogue':'dialogue competitif',
    'restricted':'restreinte',
    'restricted design contest':'concours restreint',
    'open design contest':'concours ouvert',
    'adapted':'adaptee',
    'innovation partnership':"partenariat d'innovation",
    'other': 'autre'
    }, inplace=True)

df['effectif_acheteur'] = df['effectif_acheteur'].str.strip()

df['effectif_acheteur'] = (
    df['effectif_acheteur']
    .str.replace('and more', ' et plus ', regex=False)
    .str.replace('to', ' à ', regex=False)
    .str.replace('or', ' ou ', regex=False)
)

df['effectif_fournisseur'] = df['effectif_fournisseur'].str.strip()

df['effectif_fournisseur'] = (
    df['effectif_fournisseur']
    .str.replace('and more', ' et plus ', regex=False)
    .str.replace('to', ' à ', regex=False)
    .str.replace('or', ' ou ', regex=False)
)

df = df.drop(['activite_principale_acheteur', 'activite_principale_fournisseur'], axis=1)

df = df.astype(str)


# Export quotidien

yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

df_csv = df.replace(["None", "nan"], '')

file_date = (datetime.now() - timedelta(days=1)).strftime('%d_%m_%Y')
parquet_name = f"BeauAMP_{file_date}.parquet"
csv_name = f"BeauAMP_{file_date}.csv"

df.to_parquet(parquet_name)
df_csv.to_csv(csv_name, sep=';', index=False)



API_KEY = "eyJhbGciOiJIUzUxMiJ9.eyJ1c2VyIjoiNjYyYjE1YzYyYTQ2MTc3ZDAxN2I2ZTgwIiwidGltZSI6MTc1ODA0ODQzOS45ODQ0NDcyfQ.92qzqytnWUiM2Ue2mnQT8XWpkZoJby-GEZt-8iv5BJEqNTsMpPrVsHbLj5sQapgxJzbJBgWjY5iUjuB4bIRbfQ"
DATASET_ID = "66372f4012b586dde5a071a0"

headers = {"X-API-KEY": API_KEY}

upload_url = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ID}/upload/"

# Export Parquet
with open(parquet_name, "rb") as f:
    files = {"file": (parquet_name, f, "application/octet-stream")}
    response = requests.post(upload_url, headers=headers, files=files)
try:
    response.raise_for_status()
    print(f"✅ Fichier Parquet {parquet_name} uploadé avec succès !")
    print(response.json())
except requests.exceptions.HTTPError as err:
    print(f"❌ Erreur lors de l'upload du Parquet : {err}")
    print(response.text)

# Export CSV
with open(csv_name, "rb") as f:
    files = {"file": (csv_name, f, "text/csv")}
    response = requests.post(upload_url, headers=headers, files=files)
try:
    response.raise_for_status()
    print(f"✅ Fichier CSV {csv_name} uploadé avec succès !")
    print(response.json())
except requests.exceptions.HTTPError as err:
    print(f"❌ Erreur lors de l'upload du CSV : {err}")
    print(response.text)
