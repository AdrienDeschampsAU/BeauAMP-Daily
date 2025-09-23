import pandas as pd
import os
import requests
import json
import time
import ast



path = '' # your directory
os.chdir(path)

df = pd.read_parquet('data_national_geolocated.parquet')

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
    except Exception:
        return None

def is_country_not_fra(address_json):
    if not isinstance(address_json, str) or address_json.lower() == "nan":
        return False
    try:
        address_json = address_json.replace('null', 'None')
        address = ast.literal_eval(address_json)
        return address.get("Country", "") != "FRA"
    except Exception:
        return False

def has_city_or_street(address_json):
    address = safe_parse_address(address_json)
    if address is None:
        return False
    street = address.get("StreetName", "")
    city = address.get("CityName", "")
    return bool(street) or bool(city)

def geocode_foreign_address(address_json):
    address = safe_parse_address(address_json)
    if address is None:
        return (None, None)
    query_parts = [
        address.get("StreetName", ""),
        address.get("AdditionalStreetName", ""),
        address.get("CityName", ""),
        address.get("PostalZone", ""),
        address.get("Country", "")
    ]
    query = ', '.join([str(p) for p in query_parts if p])
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": query, "format": "json", "limit": 1}
    try:
        response = requests.get(url, params=params, headers={"User-Agent": "geo-script"})
        response.raise_for_status()
        results = response.json()
        if results:
            lat = results[0].get("lat", None)
            lon = results[0].get("lon", None)
            return (lat, lon)
        else:
            return (None, None)
    except Exception:
        return (None, None)


# Find awarded firms and contracting authorities located outside France

mask_awarded_foreign = df['AWARDED_ADDRESS'].apply(is_country_not_fra) & df['AWARDED_ADDRESS'].apply(has_city_or_street)
mask_contracting_foreign = df['CONTRACTING_ADDRESS'].apply(is_country_not_fra) & df['CONTRACTING_ADDRESS'].apply(has_city_or_street)


# Geolocation of awarded firms located outside France with the Nominatim API

unique_awarded_foreign = df.loc[mask_awarded_foreign, 'AWARDED_ADDRESS'].dropna().unique()
awarded_foreign_gps_dict = {}
for i, addr in enumerate(unique_awarded_foreign):
    awarded_foreign_gps_dict[addr] = geocode_foreign_address(addr)
    time.sleep(1.2)

df.loc[mask_awarded_foreign, 'AWARDED_GPS'] = df.loc[mask_awarded_foreign, 'AWARDED_ADDRESS'].map(lambda x: awarded_foreign_gps_dict.get(x, (None, None))[0])
df.loc[mask_awarded_foreign, 'AWARDED_CITY_OFFICIAL'] = df.loc[mask_awarded_foreign, 'AWARDED_ADDRESS'].map(lambda x: awarded_foreign_gps_dict.get(x, (None, None))[1])


# Geolocation of contracting authorities located outside France with the Nominatim API

unique_contracting_foreign = df.loc[mask_contracting_foreign, 'CONTRACTING_ADDRESS'].dropna().unique()
contracting_foreign_gps_dict = {}
for i, addr in enumerate(unique_contracting_foreign):
    contracting_foreign_gps_dict[addr] = geocode_foreign_address(addr)
    time.sleep(1.2)

df.loc[mask_contracting_foreign, 'CONTRACTING_GPS'] = df.loc[mask_contracting_foreign, 'CONTRACTING_ADDRESS'].map(lambda x: contracting_foreign_gps_dict.get(x, (None, None))[0])
df.loc[mask_contracting_foreign, 'CONTRACTING_CITY_OFFICIAL'] = df.loc[mask_contracting_foreign, 'CONTRACTING_ADDRESS'].map(lambda x: contracting_foreign_gps_dict.get(x, (None, None))[1])


# Export the geolocated dataframe

df = df.astype(str)
df = df.replace("None", "nan")
df.to_parquet('data_world_geolocated.parquet')
