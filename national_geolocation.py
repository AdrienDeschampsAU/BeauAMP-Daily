import pandas as pd
import numpy as np
import os
import requests
import json
import time
import ast



path = '' # your directory
os.chdir(path)

df = pd.read_parquet('consolidated_data.parquet')


# Identify firms located in France

def is_country_fra(address_json):
    if not isinstance(address_json, str) or address_json.lower() == "nan":
        return False
    try:
        address_json = address_json.replace('null', 'None')
        address = ast.literal_eval(address_json)
        return address.get("Country", "") == "FRA"
    except Exception:
        return False

mask_geo =  df['AWARDED_ADDRESS'].apply(is_country_fra)

unique_awarded = df.loc[mask_geo, 'AWARDED_ADDRESS'].dropna().unique()


# Geolocate firms in France with the BAN API

def safe_parse_address(address_json):
    try:
        address_json = address_json.replace('null', 'None')
        address = ast.literal_eval(address_json)
        for k, v in address.items():
            if isinstance(v, str):
                address[k] = v.replace(',', '')
            elif v is None:
                address[k] = ""
        return address
    except Exception as e:
        print("Erreur de parsing pour :", address_json)
        return None

def get_gps_and_city_ban(address_json):
    if not isinstance(address_json, str) or not address_json or address_json.lower() == 'nan':
        return (None, None, None, None)
    address = safe_parse_address(address_json)
    if address is None:
        return (None, None, None, None)
    if not address.get("StreetName", ""):
        city_query = address.get("CityName", "")
        url = "https://api-adresse.data.gouv.fr/search/"
        params = {"q": city_query, "limit": 1}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            results = response.json().get("features", [])
            if results:
                props = results[0]["properties"]
                city_official = props.get("city", None)
                context = props.get("context", "")
                context_parts = context.split(', ')
                department = context_parts[1] if len(context_parts) > 1 else None
                region = context_parts[2] if len(context_parts) > 2 else None
                return (None, city_official, department, region)
            else:
                return (None, None, None, None)
        except Exception:
            return (None, None, None, None)
    parts = [
        address.get("StreetName", ""),
        address.get("AdditionalStreetName", ""),
        address.get("CityName", ""),
        address.get("PostalZone", ""),
        address.get("Country", "")
    ]
    full_address = ', '.join([str(p) for p in parts if p])
    url = "https://api-adresse.data.gouv.fr/search/"
    params = {"q": full_address, "limit": 1}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json().get("features", [])
        if results:
            coords = results[0]["geometry"]["coordinates"]
            props = results[0]["properties"]
            city_official = props.get("city", None)
            context = props.get("context", "")
            context_parts = context.split(', ')
            department = context_parts[1] if len(context_parts) > 1 else None
            region = context_parts[2] if len(context_parts) > 2 else None
            return ((coords[1], coords[0]), city_official, department, region)
        else:
            return (None, None, None, None)
    except Exception:
        return (None, None, None, None)

awarded_gps_city_dict = {}
for i, addr in enumerate(unique_awarded):
    awarded_gps_city_dict[addr] = get_gps_and_city_ban(addr)
    if (i + 1) % 47 == 0:
        time.sleep(1)


# Create new variables about firm locations

df.loc[mask_geo, 'AWARDED_GPS'] = df.loc[mask_geo, 'AWARDED_ADDRESS'].map(lambda x: awarded_gps_city_dict.get(x, (None, None, None, None))[0])
df.loc[mask_geo, 'AWARDED_CITY_OFFICIAL'] = df.loc[mask_geo, 'AWARDED_ADDRESS'].map(lambda x: awarded_gps_city_dict.get(x, (None, None, None, None))[1])
df.loc[mask_geo, 'AWARDED_DEPARTMENT'] = df.loc[mask_geo, 'AWARDED_ADDRESS'].map(lambda x: awarded_gps_city_dict.get(x, (None, None, None, None))[2])
df.loc[mask_geo, 'AWARDED_REGION'] = df.loc[mask_geo, 'AWARDED_ADDRESS'].map(lambda x: awarded_gps_city_dict.get(x, (None, None, None, None))[3])


# Identify contracting authorities located in France

mask_geo =  df['CONTRACTING_ADDRESS'].apply(is_country_fra)


# Geolocate contracting authorities in France with the BAN API

unique_contracting = df.loc[mask_geo, 'CONTRACTING_ADDRESS'].dropna().unique()
contracting_gps_city_dict = {}
for i, addr in enumerate(unique_contracting):
    contracting_gps_city_dict[addr] = get_gps_and_city_ban(addr)
    if (i + 1) % 47 == 0:
        time.sleep(1)


# Create new variables about contracting authority location

df.loc[mask_geo, 'CONTRACTING_GPS'] = df.loc[mask_geo, 'CONTRACTING_ADDRESS'].map(lambda x: contracting_gps_city_dict.get(x, (None, None, None, None))[0])
df.loc[mask_geo, 'CONTRACTING_CITY_OFFICIAL'] = df.loc[mask_geo, 'CONTRACTING_ADDRESS'].map(lambda x: contracting_gps_city_dict.get(x, (None, None, None, None))[1])
df.loc[mask_geo, 'CONTRACTING_DEPARTMENT'] = df.loc[mask_geo, 'CONTRACTING_ADDRESS'].map(lambda x: contracting_gps_city_dict.get(x, (None, None, None, None))[2])
df.loc[mask_geo, 'CONTRACTING_REGION'] = df.loc[mask_geo, 'CONTRACTING_ADDRESS'].map(lambda x: contracting_gps_city_dict.get(x, (None, None, None, None))[3])


# Add information about municipalities and municipal federations (EPCI)

communes_by_year = {}
epci_by_year = {}
for year in range(2024, 2026):
    year_str = str(year)
    communes_by_year[year_str] = pd.read_csv(f'liste_communes_{year_str}.csv', dtype=str, sep=';', usecols=['LIBGEO', 'EPCI']).set_index('LIBGEO')['EPCI'].to_dict()
    epci_df = pd.read_csv(f'liste_EPCI_{year_str}.csv', dtype=str, sep=';')
    epci_by_year[year_str] = {
        'LIBEPCI': epci_df.set_index('EPCI')['LIBEPCI'].to_dict(),
        'NATURE_EPCI': epci_df.set_index('EPCI')['NATURE_EPCI'].to_dict()
    }

def get_year_from_id_boamp(id_boamp):
    if isinstance(id_boamp, str) and len(id_boamp) >= 2:
        return '20' + id_boamp[:2]
    return None

def get_epci_info(city_official, id_boamp_award, communes_by_year, epci_by_year):
    year = get_year_from_id_boamp(id_boamp_award)
    if not year or year not in communes_by_year or year not in epci_by_year:
        return pd.Series([None, None, None])
    epci = communes_by_year[year].get(city_official)
    libepci = epci_by_year[year]['LIBEPCI'].get(epci)
    nature_epci = epci_by_year[year]['NATURE_EPCI'].get(epci)
    return pd.Series([epci, libepci, nature_epci])

df[['AWARDED_EPCI', 'AWARDED_LIBEPCI', 'AWARDED_NATURE_EPCI']] = df.apply(
    lambda row: get_epci_info(row['AWARDED_CITY_OFFICIAL'], row['ID_BOAMP_AWARD'], communes_by_year, epci_by_year),
    axis=1
)

df[['CONTRACTING_EPCI', 'CONTRACTING_LIBEPCI', 'CONTRACTING_NATURE_EPCI']] = df.apply(
    lambda row: get_epci_info(row['CONTRACTING_CITY_OFFICIAL'], row['ID_BOAMP_AWARD'], communes_by_year, epci_by_year),
    axis=1
)

codgeo_by_year = {}
for year in range(2024, 2026):
    year_str = str(year)
    communes_df = pd.read_csv(f'liste_communes_{year_str}.csv', dtype=str, sep=';', usecols=['LIBGEO', 'CODGEO'])
    codgeo_by_year[year_str] = communes_df.set_index('LIBGEO')['CODGEO'].to_dict()

def get_codgeo(city_official, id_boamp_award, codgeo_by_year):
    year = get_year_from_id_boamp(id_boamp_award)
    if not year or year not in codgeo_by_year:
        return None
    return codgeo_by_year[year].get(city_official)

df['AWARDED_CITY_CODE'] = df.apply(
    lambda row: get_codgeo(row['AWARDED_CITY_OFFICIAL'], row['ID_BOAMP_AWARD'], codgeo_by_year),
    axis=1
)

df['CONTRACTING_CITY_CODE'] = df.apply(
    lambda row: get_codgeo(row['CONTRACTING_CITY_OFFICIAL'], row['ID_BOAMP_AWARD'], codgeo_by_year),
    axis=1
)


# Export the consolidated dataframe

df = df.astype(str)
df = df.replace("None", "nan")
df.to_parquet('data_national_geolocated.parquet')
