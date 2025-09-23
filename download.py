import requests
from datetime import datetime, timedelta
import json
import os
import pandas as pd
import ast


columns_contract = [
    "idweb",
    "objet",
    "famille",
    "code_departement",
    "dateparution",
    "datelimitereponse",
    "nomacheteur",
    "perimetre",
    "type_procedure",
    "marche_public_simplifie",
    "dc",
    "type_marche",
    "etat",
    "gestion",
    "donnees",
    "annonce_lie"
    ]

columns_award = [
    "idweb",
    "objet",
    "famille",
    "code_departement",
    "nomacheteur",
    "type_procedure",
    "criteres",
    "dateparution",
    "gestion",
    "etat",
    "perimetre",
    "donnees",
    "dc",
    "type_marche",
    "annonce_lie"
    ]

liste_departements = [
    "75",
    "59",
    "13",
    "92",
    "69",
    "93",
    "33",
    "78",
    "94",
    "62",
    "91",
    "76",
    "83",
    "77",
    "31",
    "95",
    "6",
    "34",
    "44",
    "38",
    "67",
    "35",
    "57",
    "60",
    "54",
    "45",
    "29",
    "74",
    "30",
    "42",
    "80",
    "27",
    "17",
    "14",
    "84",
    "56",
    "64",
    "49",
    "73",
    "51",
    "1",
    "68",
    "2",
    "63",
    "974",
    "37",
    "21",
    "50",
    "85",
    "40",
    "87",
    "25",
    "28",
    "72",
    "86",
    "26",
    "66",
    "71",
    "11",
    "22",
    "88",
    "10",
    "972",
    "16",
    "20A",
    "47",
    "973",
    "18",
    "24",
    "41",
    "79",
    "55",
    "4",
    "52",
    "89",
    "3",
    "61",
    "7",
    "65",
    "3",
    "8",
    "20B",
    "58",
    "82",
    "23",
    "5",
    "12",
    "81",
    "39",
    "19",
    "90",
    "53",
    "36",
    "43",
    "32",
    "70",
    "9",
    "46",
    "15",
    "976",
    ""
    ]


# Function for downloading notices with the BOAMP API

def get_boamp_announcements_by_dept(date_pre, date_post, limit, code_dept, offset=0):
    base_url = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
    where_clause = (
        f"dateparution > '{date_pre}' AND dateparution < '{date_post}' "
        f"AND code_departement = '{code_dept}' "
        f"AND famille = 'JOUE' "
        f"AND source_schema = '3.2.5' "
        f"AND (type_avis = '1' OR type_avis = '6')"
    )    
    params = {
        "where": where_clause,
        "limit": limit,
        "offset": offset
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        results = data.get("results", [])
        total_count = data.get("total_count", 0)
        return results, total_count
    else:
        return [], 0


# Initialize directory

path = ''  # your directory
os.chdir(path)


# Initialize dates

today = datetime.now().strftime("%Y-%m-%d")
two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
all_annonces = []
seen_idweb = set()


# Download notices for every departement

for dept in liste_departements:
    dept_annonces_count = 0
    offset = 0
    iteration = 1
    while True:
        annonces, total_count = get_boamp_announcements_by_dept(
            date_pre=two_days_ago,
            date_post=today,
            limit=100, 
            code_dept=dept, 
            offset=offset
        )
        new_annonces_this_iteration = 0
        for annonce in annonces:
            idweb = annonce.get("idweb")
            if idweb and idweb not in seen_idweb:
                all_annonces.append(annonce)
                seen_idweb.add(idweb)
                new_annonces_this_iteration += 1
                dept_annonces_count += 1
        if len(annonces) == 0:
            break
        elif len(annonces) < 100:
            break
        elif offset + 100 >= total_count:
            break
        offset += 100
        iteration += 1
        if iteration > 50:
            break


# Separate contract notices from award notices

notices_day = pd.DataFrame(all_annonces)
award_day = notices_day[notices_day['nature_libelle'] == 'Résultat de marché'][columns_award]
contract_day = notices_day[notices_day['nature_libelle'] == 'Avis de marché'][columns_contract]
award_day = award_day.astype(str)
contract_day = contract_day.astype(str)


# Save today's award notices

award_day.to_parquet('award_day.parquet')


# Add today's contract notices to the rest of contract notices

past_contracts = pd.read_parquet('contract_notices.parquet')
three_years_ago = datetime.now() - timedelta(days=3*365)
past_contracts['dateparution'] = pd.to_datetime(past_contracts['dateparution'], errors='coerce')
past_contracts = past_contracts[past_contracts['dateparution'] > three_years_ago]


# Delete amended past contract notices

rectificatifs_contract = contract_day[contract_day['etat'] == 'RECTIFICATIF']
rectificatifs_contract = rectificatifs_contract.astype(str)

ids_to_remove = set()
for idx, row in rectificatifs_contract.iterrows():
    annonce_lie = row['annonce_lie']
    if annonce_lie != 'None':
        if isinstance(annonce_lie, str):
            try:
                annonce_lie_list = ast.literal_eval(annonce_lie)
                if not isinstance(annonce_lie_list, list):
                    annonce_lie_list = [annonce_lie_list]
            except Exception:
                annonce_lie_list = [annonce_lie]
        else:
            annonce_lie_list = annonce_lie
        ids_to_remove.update([str(x) for x in annonce_lie_list if pd.notna(x) and x not in [None, 'None', 'nan']])

past_contracts = past_contracts[~past_contracts['idweb'].astype(str).isin(ids_to_remove)].reset_index(drop=True)


# Save the updated stock of contract notices

past_contracts = pd.concat([past_contracts, contract_day], ignore_index=True)
past_contracts = past_contracts.drop_duplicates(subset=['idweb'])
past_contracts = past_contracts.astype(str)
past_contracts.to_parquet('contract_notices.parquet')
