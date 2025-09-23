import time
import pandas as pd
import os
import json
from googleapiclient.discovery import build
import re
import ast
import numpy as np



def clean_rang1(name):
    if not isinstance(name, str):
        return name
    return re.sub(r'rang[\s\-]?1', '', name, flags=re.IGNORECASE).strip()

def extract_city(address):
    try:
        address_fixed = address.replace('null', 'None')
        d = ast.literal_eval(address_fixed)
        return d.get("CityName", "")
    except Exception:
        return ""

def extract_zip(address):
    try:
        address_fixed = address.replace('null', 'None')
        d = ast.literal_eval(address_fixed)
        return d.get("PostalZone", "")
    except Exception:
        return ""

def is_groupement(name):
    name = str(name).lower()
    return 'groupement' in name and (any(sep in name for sep in ['/', '-', ',', '&', '\\']) or ' et ' in name)

def add_quote(val):
    import numpy as np
    if isinstance(val, (list, np.ndarray)):
        return [f"'{str(v)}" if isinstance(v, str) and not str(v).startswith("'") else v for v in val]
    if pd.isna(val):
        return val
    if isinstance(val, str) and not val.startswith("'"):
        return f"'{val}"
    return val

def get_first_index(val):
    if pd.isna(val):
        return val
    if isinstance(val, str) and val.startswith("[") and val.endswith("]"):
        try:
            lst = ast.literal_eval(val)
            if isinstance(lst, list) and len(lst) > 0:
                return lst[0]
        except Exception:
            pass
    return val

def normalize_address(addr):
    if isinstance(addr, str):
        try:
            d = ast.literal_eval(addr.replace("null", "None").replace('"', "'"))
            for k, v in d.items():
                if isinstance(v, str):
                    d[k] = v.lower()
            return d
        except Exception:
            return addr.lower() if isinstance(addr, str) else addr
    return addr

def get_siret_from_google(row, api_key, cse_id, api_request_count):
    if api_request_count[0] >= API_DAILY_LIMIT:
        return "API_LIMIT_REACHED"
    name = row.get('CONTRACTING_STATED_NAME', row.get('AWARDED_STATED_NAME'))
    address = row.get('CONTRACTING_ADDRESS', row.get('AWARDED_ADDRESS', row.get('ADDRESS', '')))
    raw_name = clean_rang1(str(name))
    name = raw_name.lower()
    if 'groupement' in name and 'public' not in name:
        if any(sep in name for sep in ['/', '-', ',', '&', '\\']) or ' et ' in name:
            cleaned = re.sub(r'groupement( (solidaire|conjoint))?', '', raw_name, flags=re.IGNORECASE)
            cleaned = re.sub(r'\([^\)]*\)', '', cleaned)
            cleaned = re.sub(r'["“”«»]', '', cleaned)
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            sous_noms = re.split(r'\s*/\s*|\s*-\s*|\s*,\s*|\s+et\s+|\s*&\s*|\s*\\\\\s*', cleaned)
            sous_noms = [s.strip() for s in sous_noms if s.strip()][:10]
            results = []
            for nom in sous_noms:
                row_copy = row.copy()
                if 'CONTRACTING_STATED_NAME' in row_copy:
                    row_copy['CONTRACTING_STATED_NAME'] = nom
                else:
                    row_copy['AWARDED_STATED_NAME'] = nom
                res = get_siret_from_google(row_copy, api_key, cse_id, api_request_count)
                results.append(res)
            return results
    if '/' in name and 'groupement' not in name:
        cleaned = re.sub(r'\([^\)]*\)', '', raw_name)
        cleaned = re.sub(r'["“”«»]', '', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        sous_noms = [s.strip() for s in cleaned.split('/') if s.strip()][:10]
        results = []
        for nom in sous_noms:
            row_copy = row.copy()
            if 'CONTRACTING_STATED_NAME' in row_copy:
                row_copy['CONTRACTING_STATED_NAME'] = nom
            else:
                row_copy['AWARDED_STATED_NAME'] = nom
            res = get_siret_from_google(row_copy, api_key, cse_id, api_request_count)
            results.append(res)
        return results
    service = build("customsearch", "v1", developerKey=api_key)
    def safe_search(query):
        if api_request_count[0] >= API_DAILY_LIMIT:
            return None
        try:
            api_request_count[0] += 1
            result = service.cse().list(q=query, cx=cse_id, num=1).execute()
            time.sleep(1)
            return result
        except Exception:
            time.sleep(1)
            return None
    street = None
    city = None
    zip_code = None
    try:
        address_json = address
        if isinstance(address_json, str):
            try:
                d = json.loads(address_json)
            except Exception:
                address_fixed = address_json.replace('null', 'None')
                d = ast.literal_eval(address_fixed)
            street = d.get('StreetName', None)
            if isinstance(street, dict):
                street = street.get('#text', None)
            zip_code = row.get('ZIP_CODE', None)
            if zip_code in [None, '', 'None'] or pd.isna(zip_code):
                zip_code = None
            city = row.get('CITY', None)
            if city in [None, '', 'None'] or pd.isna(city):
                city = None
            street = street if street not in [None, '', 'None'] else None
            address_parts = []
            if street is not None:
                address_parts.append(str(street))
            if zip_code is not None:
                address_parts.append(str(zip_code))
            if city is not None:
                address_parts.append(str(city))
            address_str = " ".join(address_parts).strip()
        else:
            address_str = ''
    except Exception:
        address_str = ''
    if (street is None or street == "") and (city is None or city == "") and (zip_code is None or zip_code == ""):
        return None
    def extract_siren(siret_or_siren):
        if siret_or_siren and isinstance(siret_or_siren, str):
            siret_or_siren = re.sub(r'\D', '', siret_or_siren)
            if len(siret_or_siren) == 14:
                return siret_or_siren[:9]
            if len(siret_or_siren) == 9:
                return siret_or_siren
        return None
    if city is not None:
        query_etab_city = f"site:https://annuaire-entreprises.data.gouv.fr/etablissement {raw_name} {city}"
        res = safe_search(query_etab_city)
        if res:
            for item in res.get("items", []):
                link = item.get("link", "")
                match_siret = re.search(r"(\d{14})(?:/)?$", link)
                if match_siret:
                    return extract_siren(match_siret.group(1))
        query_siren_city = f"site:https://annuaire-entreprises.data.gouv.fr {raw_name} {city}"
        res = safe_search(query_siren_city)
        if res:
            for item in res.get("items", []):
                link = item.get("link", "")
                match_siret = re.search(r"(\d{14})(?:/)?$", link)
                if match_siret:
                    return extract_siren(match_siret.group(1))
                match_siren = re.search(r"(\d{9})(?:/)?$", link)
                if match_siren:
                    return extract_siren(match_siren.group(1))
    if zip_code is not None:
        query_siren_zip = f"site:https://annuaire-entreprises.data.gouv.fr {raw_name} {zip_code}"
        res = safe_search(query_siren_zip)
        if res:
            for item in res.get("items", []):
                link = item.get("link", "")
                match_siret = re.search(r"(\d{14})(?:/)?$", link)
                if match_siret:
                    return extract_siren(match_siret.group(1))
                match_siren = re.search(r"(\d{9})(?:/)?$", link)
                if match_siren:
                    return extract_siren(match_siren.group(1))
        dep = str(zip_code)[:2] if pd.notna(zip_code) else ''
        query_siren_dep = f"site:https://annuaire-entreprises.data.gouv.fr {raw_name} {dep}"
        res = safe_search(query_siren_dep)
        if res:
            for item in res.get("items", []):
                link = item.get("link", "")
                match_siret = re.search(r"(\d{14})(?:/)?$", link)
                if match_siret:
                    return extract_siren(match_siret.group(1))
                match_siren = re.search(r"(\d{9})(?:/)?$", link)
                if match_siren:
                    return extract_siren(match_siren.group(1))
    return None


path = '' # your directory
os.chdir(path)
api_key = '' # your API key
cse_id = '' # your Custom Search Engine

API_DAILY_LIMIT = 9990
api_request_count = [0]


# Read today's missing SIRENs

df_contracting = pd.read_parquet('contracting_missing_siren.parquet')
df_awarded = pd.read_parquet('awarded_missing_siren.parquet')


# Read past identified SIRENs

stock_estimated_contracting = pd.read_parquet('stock_contracting_sirens.parquet')
stock_estimated_awarded = pd.read_parquet('stock_awarded_sirens.parquet')


# Import today's missing SIRENs from past known SIRENs

stock_contracting_dict = {
    (str(row['CONTRACTING_STATED_NAME']).lower(), str(normalize_address(row['CONTRACTING_ADDRESS']))): row['CONTRACTING_SIREN']
    for _, row in stock_estimated_contracting.iterrows()
    if row['CONTRACTING_SIREN'] != "nan"
}

def get_contracting_siren_from_stock(row):
    key = (str(row['CONTRACTING_STATED_NAME']).lower(), str(normalize_address(row['CONTRACTING_ADDRESS'])))
    return stock_contracting_dict.get(key, "nan")

df_contracting['CONTRACTING_SIREN'] = df_contracting.apply(get_contracting_siren_from_stock, axis=1)

stock_awarded_dict = {
    (str(row['AWARDED_STATED_NAME']).lower(), str(normalize_address(row['AWARDED_ADDRESS']))): row['AWARDED_SIREN']
    for _, row in stock_estimated_awarded.iterrows()
    if row['AWARDED_SIREN'] != "nan"
}

def get_awarded_siren_from_stock(row):
    key = (str(row['AWARDED_STATED_NAME']).lower(), str(normalize_address(row['AWARDED_ADDRESS'])))
    return stock_awarded_dict.get(key, "nan")

df_awarded['AWARDED_SIREN'] = df_awarded.apply(get_awarded_siren_from_stock, axis=1)


# Estimation of contracting authority SIRENs with Google API

df_contracting['CITY'] = df_contracting['CONTRACTING_ADDRESS'].apply(extract_city)
df_contracting['ZIP_CODE'] = df_contracting['CONTRACTING_ADDRESS'].apply(extract_zip)

results_contracting = []
for idx, row in enumerate(df_contracting.itertuples(index=False), 1):
    if api_request_count[0] >= API_DAILY_LIMIT:
        break
    if row.CONTRACTING_SIREN == "nan":
        res = get_siret_from_google(row._asdict(), api_key, cse_id, api_request_count)
        if res == "API_LIMIT_REACHED":
            break
        results_contracting.append(res)
        time.sleep(1)
    else:
        results_contracting.append(row.CONTRACTING_SIREN)

df_contracting = df_contracting.iloc[:len(results_contracting)].copy()
df_contracting['CONTRACTING_SIREN'] = results_contracting
df_contracting = df_contracting.astype(str)
df_contracting.to_parquet('estimated_sirens_contracting.parquet', index=False)

df_contracting = df_contracting.iloc[:len(results_contracting)].copy()
df_contracting['CONTRACTING_SIREN'] = results_contracting
df_contracting = df_contracting.astype(str)
df_contracting.to_parquet('estimated_sirens_contracting.parquet', index=False)


# Update and random renewal of known contracting authority SIRENs

new_contracting = df_contracting[['CONTRACTING_STATED_NAME', 'CONTRACTING_ADDRESS', 'CONTRACTING_SIREN']]
stock_estimated_contracting = pd.concat([stock_estimated_contracting, new_contracting], ignore_index=True)
stock_estimated_contracting = stock_estimated_contracting.drop_duplicates()
np.random.seed(42)
mask = np.random.rand(len(stock_estimated_contracting)) > 0.01
stock_estimated_contracting = stock_estimated_contracting[mask].reset_index(drop=True)
stock_estimated_contracting = stock_estimated_contracting.astype(str)


# Export the updated stock of contracting authority known SIRENs

stock_estimated_contracting.to_parquet('stock_contracting_sirens.parquet', index=False)


# Estimation of awarded firm SIRENs with Google API

df_awarded['CITY'] = df_awarded['AWARDED_ADDRESS'].apply(extract_city)
df_awarded['ZIP_CODE'] = df_awarded['AWARDED_ADDRESS'].apply(extract_zip)

results_awarded = []
for idx, row in enumerate(df_awarded.itertuples(index=False), 1):
    if api_request_count[0] >= API_DAILY_LIMIT:
        break
    if row.AWARDED_SIREN == "nan":
        res = get_siret_from_google(row._asdict(), api_key, cse_id, api_request_count)
        if res == "API_LIMIT_REACHED":
            break
        results_awarded.append(res)
        time.sleep(1)
    else:
        results_awarded.append(row.AWARDED_SIREN)

df_awarded = df_awarded.iloc[:len(results_awarded)].copy()
df_awarded['AWARDED_SIREN'] = results_awarded
df_awarded = df_awarded.astype(str)
df_awarded.to_parquet('estimated_sirens_awarded.parquet', index=False)


# Update and random renewal of known firm SIRENs

new_awarded = df_awarded[['AWARDED_STATED_NAME', 'AWARDED_ADDRESS', 'AWARDED_SIREN']]
stock_estimated_awarded = pd.concat([stock_estimated_awarded, new_awarded], ignore_index=True)
stock_estimated_awarded = stock_estimated_awarded.drop_duplicates()
np.random.seed(42)
mask = np.random.rand(len(stock_estimated_awarded)) > 0.01
stock_estimated_awarded = stock_estimated_awarded[mask].reset_index(drop=True)
stock_estimated_awarded = stock_estimated_awarded.astype(str)


# Export the updated stock of known firm SIRENs

stock_estimated_awarded.to_parquet('stock_awarded_sirens.parquet', index=False)
