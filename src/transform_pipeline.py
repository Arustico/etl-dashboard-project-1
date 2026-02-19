#----------------------------
# LIBRERIAS
#----------------------------

import pandas as pd
import numpy as np
import re
from unidecode import unidecode
from difflib import SequenceMatcher

from header_standarizer_ruler import HeaderStandardizerRules
from header_identify_processing import identify_headers,identify_headers_old
from importer_standarizer import standarize_importers_old as standarize_importers

import os
from pathlib import Path
from dotenv import load_dotenv
import logging

#----------------------------
# FUNCIONES
#----------------------------
def read_xls_files(filename, num_sheets: int = 2) -> list[pd.DataFrame]:
    """
    Lee el xls que contiene los datos, cada conjunto de datos está separado por años y hoja.
    Devuelve una lista, cada índice es un dataframe correspondiente a una hoja del archivo excel.
    """
    sheets_list = list(range(0,num_sheets))
    data = pd.read_excel(filename,sheet_name=sheets_list, dtype=str)
    return data

# Transformaciones específicias
#1: datatime
def transform_datetime(df: pd.DataFrame, column: str = "FECHA_HOML") -> pd.DataFrame:
    """
    Transformación del datatime
    """
    df[column] = df[column].replace('-',pd.NA)
    df[column] = df[column].ffill()
    df[column] = pd.to_datetime(df[column])
    # Creacion de año
    df['AÑO'] = df[column].dt.year
    return df

#2: peso bruto
def transform_pbv(df: pd.DataFrame, column: str = "PESO_BRUTO_VH_KG") -> pd.DataFrame:
    """
    Transformación del peso bruto
    """
    df[column] = df[column].replace('-',pd.NA)
    df[column] = df[column].ffill() # El valor - coresponde al valor anterior
    df[column] = pd.to_numeric(df[column])
    return df

def transform_category_cols(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Estandariza columnas categóricas
    """
    pattern = r"^\s+|\s+$" # lipiar espacios vacíos
    for col in columns:
        try: df[col] = df[col].fillna("")
        except: continue
        df[col] = df[col].astype(str)
        df[col] = df[col].apply(lambda x: unidecode(x.lower()))
        df[col] = df[col].apply(lambda x: re.sub(pattern,"",x))
    return df

def transform_combustible(df: pd.DataFrame, column: str = "COMBUSTIBLE") -> pd.DataFrame:
    """
    Transforma combustible a unidecode
    """
    df[column] = df[column].apply(lambda x: unidecode(x.lower()))
    df[column] = df[column].replace("","electrico")
    return df

#3: categoria de propuslión
def transform_categoria(df: pd.DataFrame, column: str = "PROPULSION",
                        newcol: str="CATEGORIA_PROPULSION") -> pd.DataFrame:
    """
    Creación de columna categoria de propulsión
    """
    df['CATEGORIA_PROPULSION'] = ''
    df.loc[df[column].isin(["vehiculo electrico"]),newcol] = "bev"
    df.loc[df[column].isin(["combustion","electrico de rango extendido"]),newcol] = "ice"
    df.loc[df[column].isin(["vehiculos hibridos sin recarga exterior"]),newcol]='hev'
    df.loc[df[column].isin(["vehiculos celda de hidrogeno"]),newcol] = "h2"
    df.loc[df[column].isin(["vehiculos hibridos con recarga exterior",
                            "electrico hibrido con recarga exterior"]),newcol]='phev'
    return df

#4:
def compute_rendimiento(df,column,factor):
    rendimiento = df[column]*factor
    return rendimiento

def get_rend_equiv(df: pd.DataFrame, newcol: str = "REND_EQUIV_KML") -> pd.DataFrame:
    """
    Calcula y transforma los rendimientos, según propulsión, que determina la columna y combustible, que determina un factor de conversión.
    """
    mapping_prop = { # según PROPULSION
        "combustion":"MIXTO_REND_COMBUSTIBLE_KML",
        "vehiculo electrico": "REND_EV_VH_KMKWH",
        "vehiculos hibrido con recarga exterior": "COMB_REND_WLTC_KML",
        "electrico hibrido con recarga exterior": "COMB_REND_WLTC_KML",
        "vehiculos hibridos sin recarga exterior": "MIXTO_REND_COMBUSTIBLE_KML",
        "vehiculos celda de hidrogeno": "REND_LOW_H2_KG_100_KM_FCEV_VH_CELDA",
        "electrico de rango extendido": "MIXTO_REND_COMBUSTIBLE_KML",
        }
    factors_comb = { # según COMBUSTIBLE
        "gasolina": 1,
        "diesel": 0.87,
        "electrico": 8.60,
        "hidrogreno": 374.96,
        "gasolina/glp":1,
        "gasolina/gnc":1,
        "gasolina/hibrido":1
        }
    for prop,column in mapping_prop.items():
        for comb,factor in factors_comb.items():
            if comb in ["gasolina/glp","gasolina/gnc"]:
                column = "MIXTO_REND_GASOL_VH_GLP_GNC_KML"
            df[column] = df[column].replace("-",pd.NA)
            df[column] = pd.to_numeric(df[column], errors="coerce")
            bools = (df["PROPULSION"]==prop)&(df["COMBUSTIBLE"]==comb)
            if df.loc[bools,column].empty:
                continue
            else:
                df.loc[bools,newcol] = df.loc[bools,column]*factor
    df[newcol] = df[newcol].round(2)
    return df

def get_co2_emiss(df: pd.DataFrame, newcol: str = "EMIS_CO2_EQUIV") -> pd.DataFrame:
    mapping_comb = { # según PROPULSION
        "diesel":"EMIS_CO2_GKM",
        "gasolina": "EMIS_CO2_GKM",
        "gasolina/glp":"CO2_VH_GASOL_GLP_GNC_GRKM",
        "gasolina/gnc":"CO2_VH_GASOL_GLP_GNC_GRKM",
        "electrico": "EMIS_CO2_GKM",
        "gasolina/hibrido": "CO2_PHEV_REND_PONDERADO_VH_GKM",
        "hidrogeno": "EMIS_CO2_GKM"
        }
    for comb,column in mapping_comb.items():
        df[column] = df[column].replace("-",pd.NA)
        df[column] = pd.to_numeric(df[column], errors="coerce")
        # filtrado
        bools = (df["COMBUSTIBLE"]==comb)
        df.loc[bools,newcol] = df.loc[bools,column]
        if comb in ["vehiculo electrico"]:
            df.loc[bools, newcol] = 0
            df.loc[df["CATEGORIA_PROPULSION"]=="bev",newcol] = 0
    return df

def transform_headers(df:pd.DataFrame) -> pd.DataFrame:
    """
    Transformación de los encabezados
    """
    def _found_value(dicc, value):
        for k,v in dicc.items():
            if v==value: return v

    #1. Identificación de headers: Mapeo e identificación inicial de headers
    maxrow, map_headers_raw = identify_headers(df)
    headers_raw = map_headers_raw.values()
    unnamed_hds = list(map_headers_raw.keys())

    #2. Transformación de headers raw
    standardizer = HeaderStandardizerRules()
    mapping = standardizer.batch_standardize(headers_raw)
    standardizer.export_to_csv("tmp/mapping_final.csv")

    # Combinación
   # mapping_final = dict(zip(mapping.keys(), map(map_headers_raw.get, )))
    mapping_final = {unmkd:mapping[orig] for unmkd,orig in map_headers_raw.items()}

    #3. Transformación del dataframe
    df = df.rename(columns=mapping_final)
    used_columns = list(set(mapping_final.values()))
    df = df.loc[maxrow+2:,used_columns]
    return df


def save_data(data: pd.DataFrame) -> None:
    filename_tmp = FOLDER_TMP / "datos_tmp.csv"
    data.to_csv(filename_tmp)
    logging.info("Visualización temporal en: %s",filename_tmp)


def transform_tipe_ldv(df: pd.DataFrame, column: str = "PESO_BRUTO_VH_KG",
                       newcol: str = "TIPO_LDV") -> pd.DataFrame:
    df[column] = df[column].astype(float)
    df.loc[(df[column]<2700),newcol] = 'liviano'
    df.loc[(df[column]>=2700)&(df[column]<3860),newcol] = 'mediano'
    return df

def pipeline_transformation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplicación del pipeline
    """
    category_columns = ["PROPULSION","COMBUSTIBLE","CATEGORIA_VH","IMPORTADOR",
                    "MARCA","MODELO","EMIS_NORMA", "TIPO_CARROCERIA"]

    print("="*80)
    logging.info("Transformación de Headers")
    df = transform_headers(df)
    print("="*80)
    logging.info("Transformaciones de variables")
    df = transform_datetime(df)
    df = transform_category_cols(df,category_columns)
    df = transform_combustible(df)
    df = transform_categoria(df)
    df = transform_pbv(df)
    df = transform_tipe_ldv(df)
    print("="*80)
    print("\n COMPUTO DE RENDIMIENTO Y CO2")
    print("-"*60)
    df = get_rend_equiv(df)
    df = get_co2_emiss(df)
    # Tratamientos de valores faltantes
    df.loc[df["CATEGORIA_PROPULSION"]=="bev","EMIS_CO2_EQUIV"]=0
    df["EMIS_CO2_EQUIV"] = df["EMIS_CO2_EQUIV"].fillna(df["EMIS_CO2_EQUIV"].mean().round(2))
    df["REND_EQUIV_KML"] = df["REND_EQUIV_KML"].fillna(df["REND_EQUIV_KML"].mean().round(2))

    # Estandarización de importadores
    print("="*80)
    df,notfounds = standarize_importers(df)
    print("="*80)
    return df

#----------------------------
# CONFIGURACIONES
#----------------------------

if __name__ == "__main__":
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

    name_script = Path(__file__).name
    logger = logging.getLogger(f"Transformacion Encabezados ({name_script})...")

    # Lectura datos
    filename = f"{FOLDER_RAW_LOCAL}/{RAWDATANAME}.xls"
    df = read_xls_files(filename,num_sheets=3)[0]
    # Transformación de datos
    df = pipeline_transformation(df)
    save_data(df)
    print(df)









