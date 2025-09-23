import os
import pandas as pd
import ast



path = '' # your directory
os.chdir(path)

def normalize_address(addr):
    try:
        if isinstance(addr, str):
            d = ast.literal_eval(addr)
        elif isinstance(addr, dict):
            d = addr
        else:
            return str(addr)
        normalized = {
            k: (v.lower().strip() if isinstance(v, str) and v is not None else v)
            for k, v in sorted(d.items())
        }
        return str(normalized)
    except Exception:
        return str(addr).lower().strip() if isinstance(addr, str) else str(addr)


# Import the processed dataframe

df = pd.read_parquet('processed_data.parquet')


# Merger for contracting authority SIRENs

siren_contracting = pd.read_parquet('estimated_sirens_contracting.parquet')

df['CONTRACTING_STATED_NAME_lower'] = df['CONTRACTING_STATED_NAME'].str.lower().str.strip()
df['CONTRACTING_ADDRESS_norm'] = df['CONTRACTING_ADDRESS'].apply(normalize_address)

siren_contracting['CONTRACTING_STATED_NAME_lower'] = siren_contracting['CONTRACTING_STATED_NAME'].str.lower().str.strip()
siren_contracting['CONTRACTING_ADDRESS_norm'] = siren_contracting['CONTRACTING_ADDRESS'].apply(normalize_address)

mapping_contracting = siren_contracting.set_index(
    ['CONTRACTING_STATED_NAME_lower', 'CONTRACTING_ADDRESS_norm']
)['CONTRACTING_SIREN'].to_dict()

mask_none = df['CONTRACTING_SIREN'] == "nan"
df.loc[mask_none, 'CONTRACTING_SIREN'] = df.loc[mask_none].apply(
    lambda row: mapping_contracting.get((row['CONTRACTING_STATED_NAME_lower'], row['CONTRACTING_ADDRESS_norm']),
                                       row['CONTRACTING_SIREN']),
    axis=1
)


# Merger for awarded firm SIRENs

siren_awarded = pd.read_parquet('estimated_sirens_awarded.parquet')

df['AWARDED_STATED_NAME_lower'] = df['AWARDED_STATED_NAME'].str.lower().str.strip()
df['AWARDED_ADDRESS_norm'] = df['AWARDED_ADDRESS'].apply(normalize_address)

siren_awarded['AWARDED_STATED_NAME_lower'] = siren_awarded['AWARDED_STATED_NAME'].str.lower().str.strip()
siren_awarded['AWARDED_ADDRESS_norm'] = siren_awarded['AWARDED_ADDRESS'].apply(normalize_address)

mask_none_awarded = df['AWARDED_SIREN'] == "nan"

mapping_awarded = siren_awarded.set_index(
    ['AWARDED_STATED_NAME_lower', 'AWARDED_ADDRESS_norm']
)['AWARDED_SIREN'].to_dict()

df.loc[mask_none_awarded, 'AWARDED_SIREN'] = df.loc[mask_none_awarded].apply(
    lambda row: mapping_awarded.get((row['AWARDED_STATED_NAME_lower'], row['AWARDED_ADDRESS_norm']),
                                    row['AWARDED_SIREN']),
    axis=1
)


# Export the consolidated dataframe

df.drop(columns=[ 'CONTRACTING_STATED_NAME_lower', 'CONTRACTING_ADDRESS_norm', 'AWARDED_STATED_NAME_lower', 'AWARDED_ADDRESS_norm'], inplace=True)

df = df.astype(str)
df = df.replace("None", "nan")
df.to_parquet('data_with_siren.parquet', index=False)
