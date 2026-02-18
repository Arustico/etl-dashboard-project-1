import re
import pandas as pd

from difflib import SequenceMatcher
import numpy as np
from typing import Tuple, Optional, Dict, List

import os
from pathlib import Path
from dotenv import load_dotenv
import logging

#----------------------------
# CONFIGURACIONES
#----------------------------
load_dotenv("./variables_local.env")

FOLDER_RAW_LOCAL = Path(os.getenv("FOLDER_RAW"))
FOLDER_TMP = Path(os.getenv("FOLDER_TMP"))
FOLDER_PROCESSED = os.getenv("FOLDER_PROCESSED")
COLNAMES_FILE = os.getenv("COLNAMES_FILE")
RAWDATANAME = os.getenv("RAWDATANAME", "dataRawHom")
HASH_LENGHT = os.getenv("HASH_LENGHT")
FILETMPNAME = os.getenv("FILETMPNAME","campos_hom_tmp")
BD_IMPORTADORES = os.getenv("BD_IMPORTADORES")

# Configuración básica para .log
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

#----------------------------
# FUNCIONES
#----------------------------
def normalize_name(text: str) -> str:
    """Normaliza nombres para comparación difusa."""
    if pd.isna(text):
        return ""
    text = str(text).upper()
    text = re.sub(r"[\t\.\-\s]+", "", text)
    return text

def prepare_catalog(bd_imp: pd.DataFrame) -> pd.DataFrame:
    """Prepara base maestra agregando versión normalizada."""
    catalog = bd_imp.copy()
    catalog["_norm"] = catalog["NOMBRE_EMP"].map(normalize_name)
    return catalog


def find_best_match(
    raw_name: str,
    catalog_norm_names: np.ndarray,
    threshold: float,
) -> Tuple[Optional[int], float]:
    """
    Retorna índice del mejor match y su score.
    Si no supera threshold, retorna (None, score).
    """
    norm_raw = normalize_name(raw_name)

    scores = np.array([
        SequenceMatcher(None, norm_raw, candidate).ratio()
        for candidate in catalog_norm_names
    ])

    best_idx = scores.argmax()
    best_score = scores[best_idx]

    if best_score >= threshold:
        return best_idx, best_score

    return None, best_score


def build_importer_mapping(
    data: pd.DataFrame,
    catalog: pd.DataFrame,
    threshold: float,
) -> Tuple[Dict[str, dict], List[str]]:
    """
    Genera diccionario de equivalencias:
    nombre_original -> datos estandarizados
    """
    unique_names = data["IMPORTADOR"].dropna().unique()
    catalog_norm = catalog["_norm"].to_numpy()

    mapping = {}
    not_found = []

    for name in unique_names:
        idx, score = find_best_match(name, catalog_norm, threshold)

        if idx is not None:
            row = catalog.iloc[idx]
            mapping[name] = {
                "IMPORTADOR_STD": row["NOMBRE_EMP"],
                "RUT": row["RUT"],
                "IMP_COD": row["COD_IMP"],
            }
        else:
            not_found.append(name)

    return mapping, not_found


def apply_importer_mapping(
    data: pd.DataFrame,
    mapping: Dict[str, dict],
) -> pd.DataFrame:
    """Aplica equivalencias al DataFrame original."""
    df = data.copy()

    if not mapping:
        return df

    map_df = pd.DataFrame.from_dict(mapping, orient="index")

    df = df.join(map_df, on="IMPORTADOR")

    df["IMPORTADOR"] = df["IMPORTADOR_STD"].fillna(df["IMPORTADOR"])
    df.drop(columns=["IMPORTADOR_STD"], inplace=True)

    return df


def standarize_importers(
    data: pd.DataFrame,
    bd_imp: pd.DataFrame,
    threshold: float = 0.6,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Orquesta proceso completo de estandarización de importadores.
    """

    logging.info("Preparando catálogo de importadores")
    catalog = prepare_catalog(bd_imp)

    logging.info("Construyendo tabla de equivalencias")
    mapping, not_found = build_importer_mapping(data, catalog, threshold)

    logging.info("Aplicando estandarización al dataset")
    df_std = apply_importer_mapping(data, mapping)

    logging.info("Proceso finalizado")

    return df_std, not_found



def standarize_importers_old(data: pd.DataFrame) -> pd.DataFrame:
    logging.info("Estandarización de nombres de Importadores")
    filename = f"{FOLDER_PROCESSED}{BD_IMPORTADORES}.csv"
    logging.info("Lectura de base de datos de importadores")
    bd_imp = pd.read_csv(filename)
    data = data.copy()

    # nombres de importadores
    imp_datanames = data['IMPORTADOR'].unique()
    imp_stndnames = bd_imp['NOMBRE_EMP'].unique()

    #print(imp_stndnames)
    score = []
    imp_not_found = []
    for name in imp_datanames:
        rat = [SequenceMatcher(lambda x: x in ["\t","."," ","-"],str(name),str(n)).ratio() for n in imp_stndnames]
        ix = np.argmax(rat)
        stdname = imp_stndnames[ix]
        df = bd_imp[bd_imp['NOMBRE_EMP']==stdname]
        score.append(np.max(rat))
        if np.max(rat)>0.6:
            data.loc[data['IMPORTADOR']==name,'RUT'] = df['RUT'].unique()[0]
            data.loc[data['IMPORTADOR']==name,'IMP_COD'] = df['COD_IMP'].unique()[0]
            data.loc[data['IMPORTADOR']==name,'IMPORTADOR'] = stdname
        else:
            imp_not_found.append(name)
            data.loc[data['IMPORTADOR']==name,'IMPORTADOR'] = name
    logging.info("Estandarización de importadores completada con éxito")
    return [data,imp_not_found]
