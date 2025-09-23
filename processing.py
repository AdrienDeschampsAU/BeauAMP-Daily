import pandas as pd
import os
import io
import json
import numpy as np
import re
import ast



# Initialize directory

path = '' # your directory
os.chdir(path)


# Read today's award notices and past contract notices

award_notices = pd.read_parquet("award_day.parquet")
contract_notices = pd.read_parquet("contract_notices.parquet")


# Initialize a dataframe

dataframe = pd.DataFrame()


# Bastic variables directly derived from award notices

dataframe['AMENDED'] = award_notices['etat'] == 'RECTIFICATIF'

dataframe['CONTRACTING_STATED_NAME'] = award_notices['nomacheteur']

dataframe['ID_BOAMP_AWARD'] = award_notices['idweb']

dataframe['ENVIRONMENTAL_CLAUSE'] = award_notices['criteres'].str.contains('environnementaux', case=False, na=False)

dataframe['SOCIAL_CLAUSE'] = award_notices['criteres'].str.contains('sociaux', case=False, na=False)

dataframe['PROCEDURE_TYPE'] = award_notices['type_procedure']

type_dict = {
    "OUVERT": "open",
    "NEGOCIE": "negotiated",
    "RESTREINT": "restricted",
    "AUTRE": "other",
    "DIALOGUE_COMPETITIF": "competitive dialogue",
    "PARTENARIAT_INNOVATION": "innovation partnership"
}

dataframe['PROCEDURE_TYPE'] = dataframe['PROCEDURE_TYPE'].map(type_dict)

type_dict = {
    "['SERVICES']": "services",
    "['TRAVAUX']": "works",
    "['FOURNITURES']": "supplies"
}

dataframe['CONTRACT_TYPE'] = award_notices['type_marche'].map(type_dict)

dataframe['OBJECT'] = award_notices['objet']

dataframe['AWARD_NOTICE_DATE'] = award_notices['gestion'].apply(
    lambda x: pd.read_json(io.StringIO(x))['INDEXATION']['DATE_PUBLICATION'] if pd.notna(x) else None
)


# Connect contract and award notices using direct reference

def create_award_to_contract_mapping_fast(award_notices, contract_notices):
    contract_idwebs = set(str(idweb) for idweb in contract_notices['idweb'].dropna())
    
    def extract_contract_id(annonce_lie):
        if pd.isna(annonce_lie):
            return None
        try:
            ids_in_annonce = ast.literal_eval(str(annonce_lie))
            if not isinstance(ids_in_annonce, list):
                ids_in_annonce = [ids_in_annonce]
        except Exception:
            ids_in_annonce = [str(annonce_lie)]
        for candidate in ids_in_annonce:
            candidate_str = str(candidate)
            if candidate_str in contract_idwebs:
                return candidate_str
        return None
    return award_notices['annonce_lie'].apply(extract_contract_id)

dataframe['ID_BOAMP_CONTRACT'] = create_award_to_contract_mapping_fast(award_notices, contract_notices)


# Connect contrat and award notices using similar purchase description and authority name

for idx, row in dataframe[dataframe['ID_BOAMP_CONTRACT'].isna()].iterrows():
    nom = row['CONTRACTING_STATED_NAME']
    objet = row['OBJECT']
    matches = contract_notices[
        (contract_notices['nomacheteur'] == nom) &
        (contract_notices['objet'] == objet)
    ]
    if len(matches) == 1:
        dataframe.at[idx, 'ID_BOAMP_CONTRACT'] = matches.iloc[0]['idweb']

def get_contract_notice_date(idweb):
    if pd.isna(idweb):
        return None
    row = contract_notices[contract_notices['idweb'] == idweb]
    if row.empty or pd.isna(row.iloc[0]['gestion']):
        return None
    try:
        return pd.read_json(io.StringIO(row.iloc[0]['gestion']))['INDEXATION']['DATE_PUBLICATION']
    except Exception:
        return None

dataframe['CONTRACT_NOTICE_DATE'] = dataframe['ID_BOAMP_CONTRACT'].apply(get_contract_notice_date)


# Establish more connections between contract and award notices with the project id

def extract_procurement_project_id_from_contract(contract_json):
    if pd.isna(contract_json):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(contract_json))
        if 'EFORMS' in data and 'ContractNotice' in data['EFORMS']:
            procurement_id = (
                data.get('EFORMS', {})
                    .get('ContractNotice', {})
                    .get('cac:ProcurementProject', {})
                    .get('cbc:ID', {})
            )
            if isinstance(procurement_id, dict):
                return procurement_id.get('#text')
            elif isinstance(procurement_id, str):
                return procurement_id
        ref_marche = data.get('OBJET', {}).get('REF_MARCHE')
        if ref_marche:
            return ref_marche
        return np.nan
    except Exception:
        return np.nan

def fill_missing_contract_ids_by_procurement_id(dataframe, contract_notices):
    contract_notices['PROCUREMENT_PROJECT_ID_EXTRACTED'] = contract_notices['donnees'].apply(
        extract_procurement_project_id_from_contract
    )
    used_idwebs = set(dataframe['ID_BOAMP_CONTRACT'].dropna())
    available_contracts = contract_notices[
        (~contract_notices['idweb'].isin(used_idwebs)) & 
        (contract_notices['PROCUREMENT_PROJECT_ID_EXTRACTED'].notna())
    ].copy()
    procurement_to_idwebs = {}
    for _, row in available_contracts.iterrows():
        proc_id = row['PROCUREMENT_PROJECT_ID_EXTRACTED']
        idweb = row['idweb']
        if proc_id not in procurement_to_idwebs:
            procurement_to_idwebs[proc_id] = []
        procurement_to_idwebs[proc_id].append(idweb)
    missing_mask = dataframe['ID_BOAMP_CONTRACT'].isna()
    missing_indices = dataframe[missing_mask].index.tolist()
    matches_found = 0
    missing_by_award = {}
    for idx in missing_indices:
        award_idx = dataframe.loc[idx, 'AWARD_IDX']
        if isinstance(award_idx, pd.Series):
            award_idx = award_idx.iloc[0] if not award_idx.empty else None
        
        if award_idx not in missing_by_award:
            missing_by_award[award_idx] = []
        missing_by_award[award_idx].append(idx)    
    for award_idx, indices in missing_by_award.items():
        proc_ids = []
        for idx in indices:
            proc_id = dataframe.loc[idx, 'PROCUREMENT_PROJECT_ID']
            if isinstance(proc_id, pd.Series):
                proc_id = proc_id.iloc[0] if not proc_id.empty else None
            if pd.notna(proc_id):
                proc_ids.append(proc_id)
        if not proc_ids:
            continue
        candidates = None
        found_match = False        
        for i, proc_id in enumerate(proc_ids):
            if proc_id in procurement_to_idwebs:
                current_candidates = set(procurement_to_idwebs[proc_id])
                if candidates is None:
                    candidates = current_candidates
                else:
                    candidates = candidates.intersection(current_candidates)
                if len(candidates) == 1:
                    chosen_idweb = list(candidates)[0]
                    for idx in indices:
                        dataframe.loc[idx, 'ID_BOAMP_CONTRACT'] = chosen_idweb
                        matches_found += 1
                    for pid in procurement_to_idwebs:
                        if chosen_idweb in procurement_to_idwebs[pid]:
                            procurement_to_idwebs[pid].remove(chosen_idweb)
                    found_match = True
                    break
                elif len(candidates) == 0:
                    break
        if not found_match and candidates and len(candidates) > 1:
            chosen_idweb = list(candidates)[0]
            for idx in indices:
                dataframe.loc[idx, 'ID_BOAMP_CONTRACT'] = chosen_idweb
                matches_found += 1
            for pid in procurement_to_idwebs:
                if chosen_idweb in procurement_to_idwebs[pid]:
                    procurement_to_idwebs[pid].remove(chosen_idweb)
    contract_notices.drop('PROCUREMENT_PROJECT_ID_EXTRACTED', axis=1, inplace=True)    
    return dataframe

def extract_procurement_project_id(donnee_json):
    if pd.isna(donnee_json):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        procurement_id = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProject', {})
                .get('cbc:ID')
        )
        if isinstance(procurement_id, dict):
            return procurement_id.get('#text')
        elif isinstance(procurement_id, str):
            return procurement_id
        
        return np.nan
    except Exception:
        return np.nan

dataframe['PROCUREMENT_PROJECT_ID'] = [
    extract_procurement_project_id(award_notices.loc[row['AWARD_IDX'], 'donnees'])
    for _, row in dataframe.iterrows()
]

dataframe = fill_missing_contract_ids_by_procurement_id(dataframe, contract_notices)


# Advertising period duration

def get_advertising_days(idweb):
    if pd.isna(idweb):
        return None
    row = contract_notices[contract_notices['idweb'] == idweb]
    if row.empty or pd.isna(row.iloc[0]['gestion']):
        return None
    try:
        data = pd.read_json(io.StringIO(row.iloc[0]['gestion']))
        date_pub = data['INDEXATION'].get('DATE_PUBLICATION', None)
        date_limite = data['INDEXATION'].get('DATE_LIMITE_REPONSE', None)
        if pd.isna(date_pub) or pd.isna(date_limite):
            return None
        date_pub = pd.to_datetime(date_pub).tz_localize(None)
        date_limite = pd.to_datetime(date_limite).tz_localize(None)
        return int(round((date_limite - date_pub).days))
    except Exception:
        return None

dataframe['ADVERTISING'] = dataframe['ID_BOAMP_CONTRACT'].apply(get_advertising_days)


# Number of lots and allotment

def extract_lots(gestion_json):
    if pd.isna(gestion_json):
        return []
    try:
        data = pd.read_json(io.StringIO(gestion_json))
        lots = []
        lot_result = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:NoticeResult', {})
                .get('efac:LotResult', None)
        )
        if lot_result is None:
            return []
        if isinstance(lot_result, list):
            for lot in lot_result:
                lots.append(lot)
        else:
            lots.append(lot_result)
        return lots
    except Exception as e:
        return []

dataframe['NUMBER_LOTS'] = [
    len(extract_lots(donnee)) if pd.notna(donnee) else np.nan
    for donnee in award_notices['donnees']
]

dataframe['ALLOTMENT'] = dataframe['NUMBER_LOTS'].apply(lambda x: True if x > 1 else (False if x == 1 else np.nan))


# Contract total value
                
def extract_total_value(donnee_json):
    if pd.isna(donnee_json):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        total = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:NoticeResult', {})
                .get('cbc:TotalAmount', {})
                .get('#text', None)
        )
        if total is not None:
            return float(total)
        return np.nan
    except Exception:
        return np.nan
                                  
dataframe["TOTAL_VALUE"] = [
    extract_total_value(donnee) if pd.notna(donnee) else np.nan
    for donnee in award_notices['donnees']
]


# Accelerated procedure

def extract_accelerated(donnee_json):
    if pd.isna(donnee_json):
        return False
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        proc_code = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:TenderingProcess', {})
                .get('cac:ProcessJustification', {})
                .get('cbc:ProcessReasonCode', {})
        )
        if isinstance(proc_code, dict):
            if proc_code.get('@listName') == 'accelerated-procedure':
                val = proc_code.get('#text', None)
                if val == "true":
                    return True
                if val == "false":
                    return False
        elif isinstance(proc_code, str):
            return proc_code == "true"
        return False
    except Exception:
        return False

dataframe['ACCELERATED'] = [
    extract_accelerated(donnee) if pd.notna(donnee) else np.nan
    for donnee in award_notices['donnees']
]


# Divide the dataframe into contract lots

def get_lot_ids(donnee):
    if pd.notna(donnee):
        lots = extract_lots(donnee)
        ids = []
        for lot in lots:
            lot_id = None
            if isinstance(lot, dict):
                tender_lot = lot.get('efac:TenderLot', {})
                if isinstance(tender_lot, dict):
                    id_field = tender_lot.get('cbc:ID')
                    if isinstance(id_field, dict):
                        lot_id = id_field.get('#text', None)
                    elif isinstance(id_field, str):
                        lot_id = id_field
            if lot_id is not None:
                ids.append(lot_id)
        return ids
    return [None]

rows = []
for idx, row in dataframe.iterrows():
    donnee = award_notices.loc[idx, 'donnees']
    lot_ids = get_lot_ids(donnee)
    for lot_id in lot_ids:
        new_row = row.copy()
        new_row['LOT_ID'] = lot_id
        new_row['AWARD_IDX'] = idx
        rows.append(new_row)

dataframe = pd.DataFrame(rows)


# European funds

def get_eu_funding_for_lot(donnee, lot_id):
    if pd.notna(donnee) and lot_id:
        try:
            data = pd.read_json(io.StringIO(donnee))
            lots = (
                data.get('EFORMS', {})
                    .get('ContractAwardNotice', {})
                    .get('cac:ProcurementProjectLot', [])
            )
            if not isinstance(lots, list):
                lots = [lots]
            for lot in lots:
                id_field = lot.get('cbc:ID')
                current_id = None
                if isinstance(id_field, dict):
                    current_id = id_field.get('#text', None)
                elif isinstance(id_field, str):
                    current_id = id_field
                if current_id == lot_id:
                    funding = lot.get('cac:TenderingTerms', {}).get('cbc:FundingProgramCode', {})
                    if isinstance(funding, dict):
                        return funding.get('#text', None) == "eu-funds"
                    elif isinstance(funding, str):
                        return funding == "eu-funds"
            return False
        except Exception:
            return False
    return False

def extract_funding_program_name(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                funding_program = lot.get('cac:TenderingTerms', {}).get('cbc:FundingProgram', {})
                if isinstance(funding_program, dict):
                    return funding_program.get('#text')
                elif isinstance(funding_program, str):
                    return funding_program
        return np.nan
    except Exception:
        return np.nan

dataframe['EU_FUNDED'] = [
    get_eu_funding_for_lot(
        award_notices.loc[idx, 'donnees'],
        row['LOT_ID']
    )
    for idx, row in dataframe.iterrows()
]

dataframe['FUNDS_NAME'] = [
    extract_funding_program_name(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    ) if row['EU_FUNDED'] else np.nan
    for _, row in dataframe.iterrows()
]


# CPV codes

def get_main_cpv_for_lot(donnee, lot_id):
    if pd.notna(donnee) and lot_id:
        try:
            data = pd.read_json(io.StringIO(donnee))
            lots = (
                data.get('EFORMS', {})
                    .get('ContractAwardNotice', {})
                    .get('cac:ProcurementProjectLot', [])
            )
            if not isinstance(lots, list):
                lots = [lots]
            for lot in lots:
                id_field = lot.get('cbc:ID')
                current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
                if current_id == lot_id:
                    main_cpv = (
                        lot.get('cac:ProcurementProject', {})
                           .get('cac:MainCommodityClassification', {})
                           .get('cbc:ItemClassificationCode', {})
                    )
                    if isinstance(main_cpv, dict):
                        return main_cpv.get('#text')
                    elif isinstance(main_cpv, str):
                        return main_cpv
            return None
        except Exception:
            return None
    return None

def get_additional_cpvs_for_lot(donnee, lot_id):
    if pd.notna(donnee) and lot_id:
        try:
            data = pd.read_json(io.StringIO(donnee))
            lots = (
                data.get('EFORMS', {})
                    .get('ContractAwardNotice', {})
                    .get('cac:ProcurementProjectLot', [])
            )
            if not isinstance(lots, list):
                lots = [lots]
            for lot in lots:
                id_field = lot.get('cbc:ID')
                current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
                if current_id == lot_id:
                    add_cpvs = (
                        lot.get('cac:ProcurementProject', {})
                           .get('cac:AdditionalCommodityClassification', [])
                    )
                    if isinstance(add_cpvs, dict):
                        add_cpvs = [add_cpvs]
                    codes = []
                    for add_cpv in add_cpvs:
                        code = add_cpv.get('cbc:ItemClassificationCode', {})
                        if isinstance(code, dict):
                            code_val = code.get('#text')
                            if code_val:
                                codes.append(code_val)
                        elif isinstance(code, str):
                            codes.append(code)
                    return codes
            return []
        except Exception:
            return []
    return []

dataframe['MAIN_CPV'] = [
    get_main_cpv_for_lot(
        award_notices.loc[idx, 'donnees'],
        row['LOT_ID']
    )
    for idx, row in dataframe.iterrows()
]

dataframe['ADDITIONAL_CPV'] = [
    get_additional_cpvs_for_lot(
        award_notices.loc[idx, 'donnees'],
        row['LOT_ID']
    ) if get_additional_cpvs_for_lot(
        award_notices.loc[idx, 'donnees'],
        row['LOT_ID']
    ) else np.nan
    for idx, row in dataframe.iterrows()
]


# Award price        

def get_lot_awarded_price(donnee, lot_id):
    if pd.notna(donnee) and lot_id:
        try:
            data = pd.read_json(io.StringIO(donnee))
            lot_tenders = (
                data.get('EFORMS', {})
                    .get('ContractAwardNotice', {})
                    .get('ext:UBLExtensions', {})
                    .get('ext:UBLExtension', {})
                    .get('ext:ExtensionContent', {})
                    .get('efext:EformsExtension', {})
                    .get('efac:NoticeResult', {})
                    .get('efac:LotTender', [])
            )
            if not isinstance(lot_tenders, list):
                lot_tenders = [lot_tenders]
            for lot_tender in lot_tenders:
                tender_lot = lot_tender.get('efac:TenderLot', {})
                tender_lot_id = None
                if isinstance(tender_lot, dict):
                    id_field = tender_lot.get('cbc:ID')
                    tender_lot_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
                if tender_lot_id == lot_id:
                    legal_total = lot_tender.get('cac:LegalMonetaryTotal', {})
                    payable = legal_total.get('cbc:PayableAmount', {})
                    if isinstance(payable, dict):
                        value = payable.get('#text')
                        if value is not None:
                            return float(value)
                    elif isinstance(payable, str):
                        return float(payable)
            return np.nan
        except Exception:
            return np.nan
    return np.nan

dataframe['LOT_AWARDED_PRICE'] = [
    get_lot_awarded_price(
        award_notices.loc[idx, 'donnees'],
        row['LOT_ID']
    )
    for idx, row in dataframe.iterrows()
]

dataframe.loc[dataframe['NUMBER_LOTS'] == 1, 'TOTAL_VALUE'] = dataframe.loc[dataframe['NUMBER_LOTS'] == 1, 'LOT_AWARDED_PRICE']


# Award criteria

environmental_lexicon = [
'ENVIRONNEM',
'ENVIRONEM',
'ECOLO',
'ÉCOLO',
'ÉCOSYST',
'ECOSYST',
'ÉCO-SYST',
'ECO-SYST',
'RECYCL',
'RECICL',
'SOUTENABI',
'DURAB',
'CLIMAT',
'CARBO',
'DUREE DE VIE',
'DURÉE DE VIE',
'POLLUT'
]

social_lexicon = [
'SOCIA',
'SOCIÉT',
'SOCIETA',
'ÉTHIQUE',
'ETHIQUE',
'TRACABILI',
'TRAÇABILI',
'INSERTION',
'HUMAIN',
' RSE',
'PERSONNEL'
]

delay_lexicon = [
'DELAI',
'DÉLAI',
'DURÉE',
'DUREE',
'PÉRIODE',
'PERIODE',
'TEMPS',
'PLANING',
'PLANNING'
]

technical_lexicon = [
'TECHNIQUE',
'TECHNOLO',
'METHOD',
'MÉTHOD',
'QUALIT',
'FONCTION',
'EXECUTION',
'EXÉCUTION',
'ÉXÉCUTION',
'ÉXECUTION',
'OPÉRAT',
'OPERAT'
]

quality_lexicon = [
    'QUALIT']

price_lexicon = [
    "PRIX",
    "VALEUR",
    "MONTANT",
    "TARIF",
    "COUT",
    "COÛT",
    "FINANCI"]

def classify_criterion(names):
    if not isinstance(names, list):
        return np.nan
    types = []
    env_lex = [kw.lower() for kw in environmental_lexicon]
    soc_lex = [kw.lower() for kw in social_lexicon]
    delay_lex = [kw.lower() for kw in delay_lexicon]
    qual_lex = [kw.lower() for kw in quality_lexicon]
    tech_lex = [kw.lower() for kw in technical_lexicon]
    price_lex = [kw.lower() for kw in price_lexicon]
    for name in names:
        if not isinstance(name, str):
            types.append(np.nan)
            continue
        name_lower = name.lower()
        has_env = any(keyword in name_lower for keyword in env_lex)
        has_soc = any(keyword in name_lower for keyword in soc_lex)
        if has_env and has_soc:
            types.append('ENVIRONMENTAL & SOCIAL')
            continue
        found = False
        for lexicon, label in [
            (env_lex, 'ENVIRONMENTAL'),
            (soc_lex, 'SOCIAL'),
            (delay_lex, 'DELAY'),
            (qual_lex, 'QUALITY'),
            (tech_lex, 'TECHNICAL'),
        ]:
            if any(keyword in name_lower for keyword in lexicon):
                types.append(label)
                found = True
                break
        if not found:
            if any(keyword in name_lower for keyword in price_lex) and 'prix' in name_lower:
                types.append('PRICE')
                found = True
        if not found:
            types.append('OTHER')
    return types

def extract_criteria_for_lot(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return [], []
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                awarding_terms = lot.get('cac:TenderingTerms', {}).get('cac:AwardingTerms', {})
                awarding_criterion = awarding_terms.get('cac:AwardingCriterion', {})
                if isinstance(awarding_criterion, dict):
                    awarding_criterion = [awarding_criterion]
                names = []
                weights = []
                for crit in awarding_criterion:
                    sub_criteria = crit.get('cac:SubordinateAwardingCriterion', [])
                    if isinstance(sub_criteria, dict):
                        sub_criteria = [sub_criteria]
                    for sub in sub_criteria:
                        desc = sub.get('cbc:Description', {})
                        name = desc.get('#text') if isinstance(desc, dict) else desc
                        param = (
                            sub.get('ext:UBLExtensions', {})
                               .get('ext:UBLExtension', {})
                               .get('ext:ExtensionContent', {})
                               .get('efext:EformsExtension', {})
                               .get('efac:AwardCriterionParameter', {})
                        )
                        weight = param.get('efbc:ParameterNumeric')
                        if name is not None:
                            names.append(name)
                        if weight is not None:
                            try:
                                weights.append(float(weight))
                            except Exception:
                                weights.append(weight)
                return names, weights
        return [], []
    except Exception:
        return [], []

def extract_numbers_from_names(names):
    numbers = []
    for name in names:
        if isinstance(name, str):
            found = re.findall(r'\d+(?:[.,]\d+)?', name)
            for num in found:
                num = num.replace(',', '.')
                try:
                    numbers.append(float(num))
                except Exception:
                    continue
    return numbers

def extract_criterion_types_from_json(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return []
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                awarding_terms = lot.get('cac:TenderingTerms', {}).get('cac:AwardingTerms', {})
                awarding_criterion = awarding_terms.get('cac:AwardingCriterion', {})
                if isinstance(awarding_criterion, dict):
                    awarding_criterion = [awarding_criterion]
                types = []
                for crit in awarding_criterion:
                    sub_criteria = crit.get('cac:SubordinateAwardingCriterion', [])
                    if isinstance(sub_criteria, dict):
                        sub_criteria = [sub_criteria]
                    for sub in sub_criteria:
                        crit_type = sub.get('cbc:AwardingCriterionTypeCode', {})
                        type_val = crit_type.get('#text') if isinstance(crit_type, dict) else crit_type
                        if type_val:
                            types.append(type_val.lower())
                        else:
                            types.append(None)
                return types
        return []
    except Exception:
        return []

def replace_cost_with_price(types):
    if isinstance(types, list):
        return ["PRICE" if t == "COST" else t for t in types]
    return types

dataframe['CRITERION_NAME'] = [
    extract_criteria_for_lot(
        award_notices.loc[idx, 'donnees'],
        row['LOT_ID']
    )[0]
    for idx, row in dataframe.iterrows()
]

dataframe['CRITERION_WEIGHT'] = [
    extract_criteria_for_lot(
        award_notices.loc[idx, 'donnees'],
        row['LOT_ID']
    )[1]
    for idx, row in dataframe.iterrows()
]

mask = dataframe['CRITERION_NAME'].apply(
    lambda names: any(
        isinstance(name, str) and ("Le prix n'est pas" in name or " RC" in name)
        for name in names
    ) if isinstance(names, list) else False
)

dataframe.loc[mask, 'CRITERION_NAME'] = np.nan

dataframe.loc[mask, 'CRITERION_WEIGHT'] = np.nan

dataframe = dataframe.reset_index(drop=True)

for idx, row in dataframe.iterrows():
    weights = row['CRITERION_WEIGHT']
    names = row['CRITERION_NAME']
    if (not weights or (isinstance(weights, float) and np.isnan(weights))) and isinstance(names, list):
        nums = extract_numbers_from_names(names)
        total = sum(nums)
        if (abs(total - 100) < 1 or abs(total - 1) < 0.01) and nums:
            dataframe.at[idx, 'CRITERION_WEIGHT'] = [float(x) for x in nums]

for idx, row in dataframe.iterrows():
    weights = row['CRITERION_WEIGHT']
    names = row['CRITERION_NAME']
    if (not weights or (isinstance(weights, float) and np.isnan(weights))) and isinstance(names, list):
        all_nums = []
        for name in names:
            nums = extract_numbers_from_names([name])
            if nums:
                all_nums.append(nums)
        flat = [num for sublist in all_nums for num in sublist]
        total_flat = sum(flat)
        if (abs(total_flat - 100) < 1 or abs(total_flat - 1) < 0.01) and flat:
            dataframe.at[idx, 'CRITERION_WEIGHT'] = [float(x) for x in flat]
        else:
            for sublist in all_nums:
                subtotal = sum(sublist)
                diff = 100 - subtotal
                for other in all_nums:
                    if other is not sublist and any(abs(num - diff) < 1 for num in other):
                        combined = sublist + [num for num in other if abs(num - diff) < 1]
                        if abs(sum(combined) - 100) < 1:
                            dataframe.at[idx, 'CRITERION_WEIGHT'] = [float(x) for x in combined]
                            break

for idx, weights in dataframe['CRITERION_WEIGHT'].items():
    if isinstance(weights, list):
        total = sum(weights)
        if not (abs(total - 100) < 1 or abs(total - 1) < 0.01):
            dataframe.at[idx, 'CRITERION_WEIGHT'] = np.nan

for idx, weights in dataframe['CRITERION_WEIGHT'].items():
    if isinstance(weights, list) and abs(sum(weights) - 1) < 0.01:
        dataframe.at[idx, 'CRITERION_WEIGHT'] = [w * 100 for w in weights]

dataframe['CRITERION_TYPE'] = dataframe['CRITERION_NAME'].apply(classify_criterion)

for idx, row in dataframe.iterrows():
    crit_types = row['CRITERION_TYPE']
    if isinstance(crit_types, list) and "OTHER" in crit_types:
        types_from_json = extract_criterion_types_from_json(
            award_notices.loc[row['AWARD_IDX'], 'donnees'],
            row['LOT_ID']
        )
        new_types = []
        for i, t in enumerate(crit_types):
            if t == "OTHER" and i < len(types_from_json) and types_from_json[i]:
                new_types.append(types_from_json[i].upper())
            else:
                new_types.append(t)
        dataframe.at[idx, 'CRITERION_TYPE'] = new_types

dataframe['CRITERION_TYPE'] = dataframe['CRITERION_TYPE'].apply(replace_cost_with_price)

for idx, row in dataframe.iterrows():
    crit_type = row['CRITERION_TYPE']
    if isinstance(crit_type, list) and len(crit_type) == 1 and crit_type[0].upper() in ["PRIX", "PRICE"]:
        dataframe.at[idx, 'CRITERION_WEIGHT'] = [100]


# Contract renewal

def extract_renewal_for_lot(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                contract_extension = (
                    lot.get('cac:ProcurementProject', {})
                       .get('cac:ContractExtension', {})
                )
                max_num = contract_extension.get('cbc:MaximumNumberNumeric')
                if max_num is not None:
                    try:
                        return int(max_num)
                    except Exception:
                        return np.nan
        return np.nan
    except Exception:
        return np.nan

dataframe['RENEWAL'] = [
    extract_renewal_for_lot(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    )
    for _, row in dataframe.iterrows()
]


# WTO agreement on public procurement

def extract_agp_for_lot(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                tendering_process = lot.get('cac:TenderingProcess', {})
                agp = tendering_process.get('cbc:GovernmentAgreementConstraintIndicator')
                if isinstance(agp, str):
                    return agp.lower() == "true"
                if isinstance(agp, bool):
                    return agp
        return np.nan
    except Exception:
        return np.nan

dataframe['AGP'] = [
    extract_agp_for_lot(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    )
    for _, row in dataframe.iterrows()
]


# Number of offers

def extract_number_offers_for_lot(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lot_results = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:NoticeResult', {})
                .get('efac:LotResult', [])
        )
        if not isinstance(lot_results, list):
            lot_results = [lot_results]
        for lot in lot_results:
            lot_id_field = lot.get('efac:TenderLot', {}).get('cbc:ID')
            current_id = lot_id_field.get('#text', None) if isinstance(lot_id_field, dict) else lot_id_field
            if current_id == lot_id:
                stats = lot.get('efac:ReceivedSubmissionsStatistics', [])
                if isinstance(stats, dict):
                    stats = [stats]
                for stat in stats:
                    code = stat.get('efbc:StatisticsCode', {})
                    code_val = code.get('#text') if isinstance(code, dict) else code
                    if code_val in ["tenders", "t-esubm"]:
                        num = stat.get('efbc:StatisticsNumeric')
                        if num is not None:
                            try:
                                return int(num)
                            except Exception:
                                return np.nan
        return np.nan
    except Exception:
        return np.nan

def extract_number_offers_sme(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lot_results = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:NoticeResult', {})
                .get('efac:LotResult', [])
        )
        if not isinstance(lot_results, list):
            lot_results = [lot_results]
        for lot in lot_results:
            tender_lot = lot.get('efac:TenderLot', {})
            lot_id_field = tender_lot.get('cbc:ID')
            current_id = lot_id_field.get('#text', None) if isinstance(lot_id_field, dict) else lot_id_field
            if current_id == lot_id:
                stats = lot.get('efac:ReceivedSubmissionsStatistics', [])
                if isinstance(stats, dict):
                    stats = [stats]
                for stat in stats:
                    code = stat.get('efbc:StatisticsCode', {})
                    code_val = code.get('#text') if isinstance(code, dict) else code
                    if code_val == "t-sme":
                        num = stat.get('efbc:StatisticsNumeric')
                        if num is not None:
                            try:
                                return int(num)
                            except Exception:
                                return np.nan
                return np.nan
        return np.nan
    except Exception:
        return np.nan

dataframe['NUMBER_OFFERS'] = [
    extract_number_offers_for_lot(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
        )
    for _, row in dataframe.iterrows()
    ]

dataframe['NUMBER_OFFERS_SME'] = [
    extract_number_offers_sme(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    )
    for _, row in dataframe.iterrows()
]


# Contract duration

def extract_duration_days(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                duration = (
                    lot.get('cac:ProcurementProject', {})
                       .get('cac:PlannedPeriod', {})
                       .get('cbc:DurationMeasure')
                )
                if duration is None:
                    duration = lot.get('cac:PlannedPeriod', {}).get('cbc:DurationMeasure')
                if isinstance(duration, dict):
                    value = duration.get('#text')
                    unit = duration.get('@unitCode', '').lower()
                else:
                    value = duration
                    unit = ''
                if value is not None:
                    try:
                        value = float(value)
                        if unit in ('month'):
                            return int(round(value * 30))
                        elif unit in ('day'):
                            return int(round(value))
                        elif unit in ('year'):
                            return int(round(value * 365))
                        elif unit in ('week'):
                            return int(round(value * 7))
                        else:
                            return int(round(value))
                    except Exception:
                        return np.nan
        return np.nan
    except Exception:
        return np.nan

dataframe['DURATION'] = [
    extract_duration_days(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    )
    for _, row in dataframe.iterrows()
]


# Contract start date

def extract_contract_start(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                start_date = (
                    lot.get('cac:ProcurementProject', {})
                       .get('cac:PlannedPeriod', {})
                       .get('cbc:StartDate')
                )
                if start_date is None:
                    start_date = lot.get('cac:PlannedPeriod', {}).get('cbc:StartDate')
                return start_date if start_date is not None else np.nan
        return np.nan
    except Exception:
        return np.nan

dataframe['CONTRACT_START'] = [
    extract_contract_start(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    )
    for _, row in dataframe.iterrows()
]


# Framework agreements

def extract_max_total_value_framework_agreement(donnee_json):
    if pd.isna(donnee_json):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        value = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:NoticeResult', {})
                .get('efbc:OverallMaximumFrameworkContractsAmount', {})
                .get('#text', None)
        )
        if value is not None:
            return float(value)
        return np.nan
    except Exception:
        return np.nan

def extract_framework_agreement(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        ext = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
        )
        lot_results = ext.get('efac:NoticeResult', {}).get('efac:LotResult', [])
        if not isinstance(lot_results, list):
            lot_results = [lot_results]
        contract_ids = []
        for lot in lot_results:
            tender_lot = lot.get('efac:TenderLot', {})
            lot_id_field = tender_lot.get('cbc:ID')
            current_lot_id = lot_id_field.get('#text', None) if isinstance(lot_id_field, dict) else lot_id_field
            if current_lot_id == lot_id:
                settled_contract = lot.get('efac:SettledContract', {})
                contract_id = settled_contract.get('cbc:ID')
                contract_id = contract_id.get('#text', None) if isinstance(contract_id, dict) else contract_id
                if contract_id:
                    contract_ids.append(contract_id)
        if not contract_ids:
            return np.nan
        settled_contracts = ext.get('efac:NoticeResult', {}).get('efac:SettledContract', [])
        if not isinstance(settled_contracts, list):
            settled_contracts = [settled_contracts]
        for contract in settled_contracts:
            contract_id_field = contract.get('cbc:ID')
            contract_id_val = contract_id_field.get('#text', None) if isinstance(contract_id_field, dict) else contract_id_field
            if contract_id_val in contract_ids:
                indicator = contract.get('efbc:ContractFrameworkIndicator')
                if isinstance(indicator, str):
                    return indicator.lower() == "true"
                if isinstance(indicator, bool):
                    return indicator
        return False
    except Exception:
        return np.nan

def extract_framework_agreement_type(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                contracting_systems = (
                    lot.get('cac:TenderingProcess', {})
                       .get('cac:ContractingSystem', [])
                )
                if isinstance(contracting_systems, dict):
                    contracting_systems = [contracting_systems]
                for cs in contracting_systems:
                    code = cs.get('cbc:ContractingSystemTypeCode', {})
                    if isinstance(code, dict):
                        if code.get('@listName') == 'framework-agreement':
                            val = code.get('#text')
                            if isinstance(val, str) and val.strip().lower() == "none":
                                return np.nan
                            return val
                    elif isinstance(code, str):
                        if code.strip().lower() == "none":
                            return np.nan
                        return code
        return np.nan
    except Exception:
        return np.nan

dataframe['MAX_TOTAL_VALUE_FRAMEWORK_AGREEMENT'] = [
    extract_max_total_value_framework_agreement(
        award_notices.loc[row['AWARD_IDX'], 'donnees']
    )
    for _, row in dataframe.iterrows()
]

fa_dict = {
    "fa-wo-rc": "no competition",
    "fa-w-rc": "competition",
    "fa-mix": "mixed"
}

dataframe['FRAMEWORK_AGREEMENT_TYPE'] = [
    fa_dict.get(
        extract_framework_agreement_type(
            award_notices.loc[row['AWARD_IDX'], 'donnees'],
            row['LOT_ID']
        ),
        extract_framework_agreement_type(
            award_notices.loc[row['AWARD_IDX'], 'donnees'],
            row['LOT_ID']
        )
    )
    for _, row in dataframe.iterrows()
]


# Strategic contracts

def has_social_objective_award(row):
    idx = row['AWARD_IDX']
    if pd.isna(idx):
        return np.nan
    donnees = award_notices.loc[idx, 'donnees']
    if pd.isna(donnees):
        return np.nan
    return '"social-objective"' in donnees

def has_environmental_objective_award(row):
    idx = row['AWARD_IDX']
    if pd.isna(idx):
        return np.nan
    donnees = award_notices.loc[idx, 'donnees']
    if pd.isna(donnees):
        return np.nan
    return '"environmental-impact"' in donnees

dataframe['STRATEGIC_SOCIAL'] = dataframe.apply(has_social_objective_award, axis=1)
dataframe['STRATEGIC_ENVIRONMENTAL'] = dataframe.apply(has_environmental_objective_award, axis=1)


# Estimated contract total value

def extract_estimated_total_value_award(donnee_json):
    if pd.isna(donnee_json):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        value = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProject', {})
                .get('cac:RequestedTenderTotal', {})
                .get('cbc:EstimatedOverallContractAmount', {})
                .get('#text', None)
        )
        if value is not None:
            try:
                return float(value)
            except Exception:
                return np.nan
        return np.nan
    except Exception:
        return np.nan

dataframe['ESTIMATED_TOTAL_VALUE'] = [
    extract_estimated_total_value_award(award_notices.loc[row['AWARD_IDX'], 'donnees'])
    for _, row in dataframe.iterrows()
]


# Contrat performance location

def extract_realized_location_award(award_json, lot_id):
    if pd.isna(award_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(award_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if isinstance(lots, dict):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                address = (
                    lot.get('cac:ProcurementProject', {})
                       .get('cac:RealizedLocation', {})
                       .get('cac:Address', {})
                )
                if not address:
                    continue
                return {
                    "StreetName": address.get('cbc:StreetName'),
                    "CityName": address.get('cbc:CityName'),
                    "PostalZone": address.get('cbc:PostalZone'),
                    "NUTS": address.get('cbc:CountrySubentityCode', {}).get('#text') if isinstance(address.get('cbc:CountrySubentityCode'), dict) else address.get('cbc:CountrySubentityCode'),
                    "Country": address.get('cac:Country', {}).get('cbc:IdentificationCode', {}).get('#text') if isinstance(address.get('cac:Country', {}).get('cbc:IdentificationCode'), dict) else address.get('cac:Country', {}).get('cbc:IdentificationCode')
                }
        return np.nan
    except Exception:
        return np.nan

dataframe['EXECUTION_SITE'] = [
    extract_realized_location_award(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    )
    for _, row in dataframe.iterrows()
]


# Lot estimated value

def extract_estimated_value_for_lot_award(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                value = (
                    lot.get('cac:ProcurementProject', {})
                       .get('cac:RequestedTenderTotal', {})
                       .get('cbc:EstimatedOverallContractAmount', {})
                       .get('#text', None)
                )
                if value is not None:
                    try:
                        return float(value)
                    except Exception:
                        return value
        return np.nan
    except Exception:
        return np.nan

dataframe['LOT_ESTIMATED_VALUE'] = [
    extract_estimated_value_for_lot_award(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    )
    for _, row in dataframe.iterrows()
]


# Award status

def extract_awarded_status_award(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lot_results = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:NoticeResult', {})
                .get('efac:LotResult', [])
        )
        if not isinstance(lot_results, list):
            lot_results = [lot_results]
        for lot in lot_results:
            tender_lot = lot.get('efac:TenderLot', {})
            lot_id_field = tender_lot.get('cbc:ID')
            current_id = lot_id_field.get('#text', None) if isinstance(lot_id_field, dict) else lot_id_field
            if current_id == lot_id:
                code = lot.get('cbc:TenderResultCode', {})
                code_val = code.get('#text') if isinstance(code, dict) else code
                return code_val
        return np.nan
    except Exception:
        return np.nan

status_dict = {
    'selec-w': 'selected',
    'clos-nw': 'no award',
    'open-nw': 'still open'
}

dataframe['AWARDED'] = [
    status_dict.get(
        extract_awarded_status_award(
            award_notices.loc[row['AWARD_IDX'], 'donnees'],
            row['LOT_ID']
        ),
        extract_awarded_status_award(
            award_notices.loc[row['AWARD_IDX'], 'donnees'],
            row['LOT_ID']
        )
    )
    for _, row in dataframe.iterrows()
]


# Identify awarded organizations

def get_awarded_orgs_for_lot_award(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return []
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        ext = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
        )
        lot_results = ext.get('efac:NoticeResult', {}).get('efac:LotResult', [])
        if not isinstance(lot_results, list):
            lot_results = [lot_results]
        tender_ids = []
        for lot in lot_results:
            tender_lot = lot.get('efac:TenderLot', {})
            lot_id_field = tender_lot.get('cbc:ID')
            current_id = lot_id_field.get('#text', None) if isinstance(lot_id_field, dict) else lot_id_field
            if current_id == lot_id:
                lot_tender = lot.get('efac:LotTender', {})
                tender_id = lot_tender.get('cbc:ID')
                tender_id = tender_id.get('#text', None) if isinstance(tender_id, dict) else tender_id
                if tender_id:
                    tender_ids.append(tender_id)
        lot_tenders = ext.get('efac:NoticeResult', {}).get('efac:LotTender', [])
        if not isinstance(lot_tenders, list):
            lot_tenders = [lot_tenders]
        tendering_party_ids = []
        for tender in lot_tenders:
            tid = tender.get('cbc:ID')
            tid_val = tid.get('#text', None) if isinstance(tid, dict) else tid
            if tid_val in tender_ids:
                tendering_party = tender.get('efac:TenderingParty', {})
                tendering_party_id = tendering_party.get('cbc:ID')
                tendering_party_id = tendering_party_id.get('#text', None) if isinstance(tendering_party_id, dict) else tendering_party_id
                if tendering_party_id:
                    tendering_party_ids.append(tendering_party_id)
        tendering_parties = ext.get('efac:NoticeResult', {}).get('efac:TenderingParty', [])
        if not isinstance(tendering_parties, list):
            tendering_parties = [tendering_parties]
        org_ids = []
        for party in tendering_parties:
            pid = party.get('cbc:ID')
            pid_val = pid.get('#text', None) if isinstance(pid, dict) else pid
            if pid_val in tendering_party_ids:
                tenderer = party.get('efac:Tenderer', {})
                org_id = tenderer.get('cbc:ID')
                org_id = org_id.get('#text', None) if isinstance(org_id, dict) else org_id
                if org_id:
                    org_ids.append(org_id)
        return org_ids
    except Exception:
        return []


# Split the dataframe for each awarded firm

dataframe['AWARDED_ORG_IDS'] = [
    get_awarded_orgs_for_lot_award(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    )
    for _, row in dataframe.iterrows()
]

dataframe = dataframe.explode('AWARDED_ORG_IDS').reset_index(drop=True)


# Identify contracting authorities

def extract_authority_id_for_lot_award(donnee_json, lot_id):
  if pd.isna(donnee_json) or not lot_id:
      return np.nan
  try:
      data = pd.read_json(io.StringIO(donnee_json))
      lots = (
          data.get('EFORMS', {})
              .get('ContractAwardNotice', {})
              .get('cac:ProcurementProjectLot', [])
      )
      if not isinstance(lots, list):
          lots = [lots]
      for lot in lots:
          id_field = lot.get('cbc:ID')
          current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
          if current_id == lot_id:
              break
      else:
          lot = None
      contracting_parties = (
          data.get('EFORMS', {})
              .get('ContractAwardNotice', {})
              .get('cac:ContractingParty', [])
      )
      if isinstance(contracting_parties, dict):
          contracting_parties = [contracting_parties]
      for party in contracting_parties:
          party_block = party.get('cac:Party', {})
          party_ident = party_block.get('cac:PartyIdentification', {})
          party_id = party_ident.get('cbc:ID')
          if isinstance(party_id, dict):
              return party_id.get('#text', None)
          elif isinstance(party_id, str):
              return party_id
      return np.nan
  except Exception:
      return np.nan

dataframe['CONTRACTING_ID'] = [
    extract_authority_id_for_lot_award(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['LOT_ID']
    )
    for _, row in dataframe.iterrows()
]


# Contracting authority address

def extract_address_contracting(donnee_json, contracting_id):
    if pd.isna(donnee_json) or pd.isna(contracting_id):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        orgs = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:Organizations', {})
                .get('efac:Organization', [])
        )
        if isinstance(orgs, dict):
            orgs = [orgs]
        for org in orgs:
            company = org.get('efac:Company', {})
            party_ident = company.get('cac:PartyIdentification', {}).get('cbc:ID', {})
            org_id = party_ident.get('#text') if isinstance(party_ident, dict) else party_ident
            if org_id == contracting_id:
                address = company.get('cac:PostalAddress', {})
                return {
                    "StreetName": address.get('cbc:StreetName'),
                    "AdditionalStreetName": address.get('cbc:AdditionalStreetName'),
                    "CityName": address.get('cbc:CityName'),
                    "PostalZone": address.get('cbc:PostalZone'),
                    "Country": address.get('cac:Country', {}).get('cbc:IdentificationCode', {}).get('#text')
                        if isinstance(address.get('cac:Country', {}).get('cbc:IdentificationCode'), dict)
                        else address.get('cac:Country', {}).get('cbc:IdentificationCode')
                }
        return np.nan
    except Exception:
        return np.nan

dataframe['CONTRACTING_ADDRESS'] = [
    extract_address_contracting(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['CONTRACTING_ID']
    )
    for _, row in dataframe.iterrows()
]


# Contracting authority SIREN

def get_contracting_official(donnee_json, contracting_id):
    if pd.isna(donnee_json) or pd.isna(contracting_id):
        return None
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        orgs = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:Organizations', {})
                .get('efac:Organization', [])
        )
        if isinstance(orgs, dict):
            orgs = [orgs]
        for org in orgs:
            company = org.get('efac:Company', {})
            party_ident = company.get('cac:PartyIdentification', {}).get('cbc:ID', {})
            org_id = party_ident.get('#text') if isinstance(party_ident, dict) else party_ident
            if org_id == contracting_id:
                legal_entity = company.get('cac:PartyLegalEntity', {})
                company_id = str(legal_entity.get('cbc:CompanyID', ''))
                if re.match(r'^(2023|2024|2025)', company_id):
                    return None
                siret_match = re.search(r'\b\d{14}\b', company_id)
                siren_match = re.search(r'\b\d{9}\b', company_id)
                if siret_match:
                    return siret_match.group(0)[:9]
                if siren_match:
                    return siren_match.group(0)
        return None
    except Exception:
        return None

dataframe['CONTRACTING_SIREN'] = [
    get_contracting_official(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['CONTRACTING_ID']
    )
    for _, row in dataframe.iterrows()
]

def clean_official(x):
    if isinstance(x, dict):
        siret = x.get('SIRET')
        siren = x.get('SIREN')
        if siret:
            return siret.replace(" ", "")[:9]
        if siren:
            return siren.replace(" ", "")
    if isinstance(x, str):
        x_clean = x.replace(" ", "")
        if re.fullmatch(r'\d{9}', x_clean):
            return x_clean
    return np.nan

dataframe['CONTRACTING_SIREN'] = dataframe['CONTRACTING_SIREN'].apply(clean_official)
dataframe['CONTRACTING_SIREN_MENTIONED'] = ~dataframe['CONTRACTING_SIREN'].isna()


# Awarded firm names

def extract_awarded_name(donnee_json, org_id):
    if pd.isna(donnee_json) or pd.isna(org_id):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        orgs = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:Organizations', {})
                .get('efac:Organization', [])
        )
        if isinstance(orgs, dict):
            orgs = [orgs]
        for org in orgs:
            company = org.get('efac:Company', {})
            party_ident = company.get('cac:PartyIdentification', {}).get('cbc:ID', {})
            this_id = party_ident.get('#text') if isinstance(party_ident, dict) else party_ident
            if this_id == org_id:
                party_name = company.get('cac:PartyName', {}).get('cbc:Name', {})
                name = party_name.get('#text') if isinstance(party_name, dict) else party_name
                return name
        return np.nan
    except Exception:
        return np.nan

dataframe['AWARDED_STATED_NAME'] = [
    extract_awarded_name(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['AWARDED_ORG_IDS']
    )
    for _, row in dataframe.iterrows()
]


# Awarded firm address

def extract_awarded_address(donnee_json, org_id):
    if pd.isna(donnee_json) or pd.isna(org_id):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        orgs = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:Organizations', {})
                .get('efac:Organization', [])
        )
        if isinstance(orgs, dict):
            orgs = [orgs]
        for org in orgs:
            company = org.get('efac:Company', {})
            party_ident = company.get('cac:PartyIdentification', {}).get('cbc:ID', {})
            this_id = party_ident.get('#text') if isinstance(party_ident, dict) else party_ident
            if this_id == org_id:
                address = company.get('cac:PostalAddress', {})
                return {
                    "StreetName": address.get('cbc:StreetName'),
                    "AdditionalStreetName": address.get('cbc:AdditionalStreetName'),
                    "CityName": address.get('cbc:CityName'),
                    "PostalZone": address.get('cbc:PostalZone'),
                    "Country": address.get('cac:Country', {}).get('cbc:IdentificationCode', {}).get('#text')
                        if isinstance(address.get('cac:Country', {}).get('cbc:IdentificationCode'), dict)
                        else address.get('cac:Country', {}).get('cbc:IdentificationCode')
                }
        return np.nan
    except Exception:
        return np.nan

dataframe['AWARDED_ADDRESS'] = [
    extract_awarded_address(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['AWARDED_ORG_IDS']
    )
    for _, row in dataframe.iterrows()
]


# Awarded firm SIREN

def extract_awarded_official(donnee_json, org_id):
    if pd.isna(donnee_json) or pd.isna(org_id):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        orgs = (
            data.get('EFORMS', {})
                .get('ContractAwardNotice', {})
                .get('ext:UBLExtensions', {})
                .get('ext:UBLExtension', {})
                .get('ext:ExtensionContent', {})
                .get('efext:EformsExtension', {})
                .get('efac:Organizations', {})
                .get('efac:Organization', [])
        )
        if isinstance(orgs, dict):
            orgs = [orgs]
        for org in orgs:
            company = org.get('efac:Company', {})
            party_ident = company.get('cac:PartyIdentification', {}).get('cbc:ID', {})
            this_id = party_ident.get('#text') if isinstance(party_ident, dict) else party_ident
            if this_id == org_id:
                legal_entity = company.get('cac:PartyLegalEntity', {})
                company_id = str(legal_entity.get('cbc:CompanyID', ''))
                if re.match(r'^(2023|2024|2025)', company_id):
                    return np.nan
                siret_match = re.search(r'\b\d{14}\b', company_id)
                siren_match = re.search(r'\b\d{9}\b', company_id)
                if siret_match:
                    return siret_match.group(0)[:9]
                if siren_match:
                    return siren_match.group(0)
        return np.nan
    except Exception:
        return np.nan

dataframe['AWARDED_SIREN'] = [
    extract_awarded_official(
        award_notices.loc[row['AWARD_IDX'], 'donnees'],
        row['AWARDED_ORG_IDS']
    )
    for _, row in dataframe.iterrows()
]

dataframe['AWARDED_SIREN'] = dataframe['AWARDED_SIREN'].apply(clean_official)
dataframe['AWARDED_SIREN_MENTIONED'] = ~dataframe['AWARDED_SIREN'].isna()


# Reserved contracts

def extract_reserved_status(contract_json):
    if pd.isna(contract_json):
        return np.nan
    try:
        data = pd.read_json(io.StringIO(contract_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if isinstance(lots, dict):
            lots = [lots]
        for lot in lots:
            tender_req = lot.get('cac:TenderingTerms', {}).get('cac:TendererQualificationRequest', {})
            spec_req = tender_req.get('cac:SpecificTendererRequirement', {})
            if isinstance(spec_req, list):
                for req in spec_req:
                    code = req.get('cbc:TendererRequirementTypeCode', {})
                    if isinstance(code, dict) and code.get('@listName') == 'reserved-procurement':
                        if code.get('#text', np.nan) == "none":
                            return False
                        else:
                            return True
            else:
                code = spec_req.get('cbc:TendererRequirementTypeCode', {})
                if isinstance(code, dict) and code.get('@listName') == 'reserved-procurement':
                    if code.get('#text', np.nan) == "none":
                        return False
                    else:
                        return True
        return np.nan
    except Exception:
        return np.nan

def get_reserved_for_row(row):
    contract_id = row['ID_BOAMP_CONTRACT']
    if pd.isna(contract_id):
        return np.nan
    match = contract_notices[contract_notices['idweb'] == contract_id]
    if match.empty:
        return np.nan
    contract_json = match.iloc[0]['donnees']
    return extract_reserved_status(contract_json)

dataframe['RESERVED'] = dataframe.apply(get_reserved_for_row, axis=1)


# Contract adapted to SMEs

def sme_suitable(row):
    contract_id = row['ID_BOAMP_CONTRACT']
    if pd.isna(contract_id):
        return np.nan
    match = contract_notices[contract_notices['idweb'] == contract_id]
    if match.empty:
        return np.nan
    contract_json = match.iloc[0]['donnees']
    if pd.isna(contract_json):
        return np.nan
    return '"cbc:SMESuitableIndicator"' in contract_json

dataframe['SME_FRIENDLY'] = dataframe.apply(sme_suitable, axis=1)


# Attempt to identify contract performance location with contract notices

def extract_realized_location(contract_json, lot_id):
    if pd.isna(contract_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(contract_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if isinstance(lots, dict):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                address = (
                    lot.get('cac:ProcurementProject', {})
                       .get('cac:RealizedLocation', {})
                       .get('cac:Address', {})
                )
                if not address:
                    continue
                return {
                    "StreetName": address.get('cbc:StreetName'),
                    "CityName": address.get('cbc:CityName'),
                    "PostalZone": address.get('cbc:PostalZone'),
                    "NUTS": address.get('cbc:CountrySubentityCode', {}).get('#text') if isinstance(address.get('cbc:CountrySubentityCode'), dict) else address.get('cbc:CountrySubentityCode'),
                    "Country": address.get('cac:Country', {}).get('cbc:IdentificationCode', {}).get('#text') if isinstance(address.get('cac:Country', {}).get('cbc:IdentificationCode'), dict) else address.get('cac:Country', {}).get('cbc:IdentificationCode')
                }
        return np.nan
    except Exception:
        return np.nan

def extract_realized_location_contract(row):
    contract_id = row['ID_BOAMP_CONTRACT']
    lot_id = row['LOT_ID']
    if pd.isna(contract_id) or pd.isna(lot_id):
        return np.nan
    match = contract_notices[contract_notices['idweb'] == contract_id]
    if match.empty:
        return np.nan
    contract_json = match.iloc[0]['donnees']
    return extract_realized_location(contract_json, lot_id)

mask_nan = dataframe['EXECUTION_SITE'].isna()
dataframe.loc[mask_nan, 'EXECUTION_SITE'] = dataframe[mask_nan].apply(extract_realized_location_contract, axis=1)


# Further attempt to find lot estimated value with contract notices

def extract_estimated_value_for_lot_contract(donnee_json, lot_id):
    if pd.isna(donnee_json) or not lot_id:
        return np.nan
    try:
        data = pd.read_json(io.StringIO(donnee_json))
        lots = (
            data.get('EFORMS', {})
                .get('ContractNotice', {})
                .get('cac:ProcurementProjectLot', [])
        )
        if not isinstance(lots, list):
            lots = [lots]
        for lot in lots:
            id_field = lot.get('cbc:ID')
            current_id = id_field.get('#text', None) if isinstance(id_field, dict) else id_field
            if current_id == lot_id:
                value = (
                    lot.get('cac:ProcurementProject', {})
                       .get('cac:RequestedTenderTotal', {})
                       .get('cbc:EstimatedOverallContractAmount', {})
                       .get('#text', None)
                )
                if value is not None:
                    try:
                        return float(value)
                    except Exception:
                        return value
        return np.nan
    except Exception:
        return np.nan

def get_lot_estimated_value_contract(row):
    contract_id = row['ID_BOAMP_CONTRACT']
    lot_id = row['LOT_ID']
    if pd.isna(contract_id) or pd.isna(lot_id):
        return np.nan
    match = contract_notices[contract_notices['idweb'] == contract_id]
    if match.empty:
        return np.nan
    contract_json = match.iloc[0]['donnees']
    return extract_estimated_value_for_lot_contract(contract_json, lot_id)

mask_nan = dataframe['LOT_ESTIMATED_VALUE'].isna()
dataframe.loc[mask_nan, 'LOT_ESTIMATED_VALUE'] = dataframe[mask_nan].apply(get_lot_estimated_value_contract, axis=1)


# Remove redundant variables

dataframe = dataframe.drop(columns=['AWARDED_ORG_IDS'])
dataframe = dataframe.drop(columns=['CONTRACTING_ID'])
dataframe = dataframe.drop(columns=['AWARD_IDX'])

dataframe = dataframe.astype(str)
dataframe = dataframe.replace("None", "nan")
dataframe.to_parquet('processed_data.parquet')
