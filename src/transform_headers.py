#%% LIBRERIAS
import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import json

import hashlib
from datetime import datetime
from dotenv import load_dotenv
from difflib import SequenceMatcher,get_close_matches
from functools import reduce

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

# Configuración básica para .log
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("extraction")


#----------------------------
#%% FUNCIONES
#----------------------------

def hashing(name: str,length: Optional[int] = HASH_LENGHT) -> int:
    return hashlib.sha256(name.encode()).hexdigest()[:int(length)]


def find_header_rows(df: pd.DataFrame) -> list:
    """Encuentra el índice de la fila donde comienzan los encabezados de la data xls."""
    header_row_index = []
    # Busca el primer valor no nulo, ese debe ser el "padre"
    for col in df.columns:
        first_valid = df[col].first_valid_index()
        header_row_index.append(first_valid)
    return header_row_index

def identify_headers(df:pd.DataFrame()) -> dict():
    """
    Identifica y transforma los headers de los dataframes.
    """
    # busqueda de niveles no nulos
    logging.info("Buscando headers")
    levels = find_header_rows(df)
    valid_levels = [x for x in levels if x is not None and x < 10]
    maxrow = max(valid_levels)
    levels = sorted(set(x for x in valid_levels if x < maxrow))

    header_rows = df.iloc[levels]

    # Transponer para trabajar por columnas
    header_cols = header_rows.T.dropna(how='all')
    header_cols.iloc[:,0] = header_cols.iloc[:,0].ffill()
    header_cols.iloc[(header_cols[levels[1]].isna())&(header_cols[levels[2]].isna()),1:3] = ""
    header_cols = header_cols.reset_index() # guardamos index

    logging.info("Reestructurando headers")
    # Reestructura los nombres de los headers uniendolos en un solo nombre para toda la columna según su padre
    newcolsname = []
    for parent,gr in header_cols.groupby(levels[0]):
        #print(f"PADRE:{parent} GRUPOS:{gr}\n")
        if len(gr)>1:
            gr[levels[1]] = gr[levels[1]].ffill()
            gr[levels[2]] = gr[levels[2]].fillna("")
        gr = gr.fillna("")
        newcolsname.append(gr)
    newcolsname = pd.concat(newcolsname)
    newcolsname['combcol'] = newcolsname[levels].apply(lambda row: "_".join([str(v) for v in row if (v != "")
                                                                             and("Unnamed:" not in v)]), axis=1)
    newcolsname = newcolsname[['index','combcol']]
    newcolsname = newcolsname.set_index('index')

    dictofnames = newcolsname['combcol'].to_dict()
    return [maxrow,dictofnames]


def renameCol(col,colnames: dict) -> str():
    """
    Renombre las columnas a partir de un estandarizado
    """
    hashval = hashing(col)
    for colname,values in colnames.items():
        if hashval in values['hash']:
            return colname
        else:
            return None


def check_hash(hashval,colnames: dict) -> bool:
    " Devuelve True si está en la BD de campos"
    for colname,values in colnames.items():
      if hashval in values['hash']: return True
      else: return False

def column_json2df(colnames: dict) -> pd.DataFrame:
    list_3cv_colnames = [v["default"] for v in colnames.values()]
    list_stnd_colnames = [v for v in colnames.keys()]
    list_hashs = [v["hash"] for v in colnames.values()]
    df = pd.DataFrame({"STANDARD_NAME":list_stnd_colnames,"3CV_NAMES":list_3cv_colnames,"HASH":list_hashs})
    df = df.explode("3CV_NAMES").explode("HASH")
    df = df.reset_index(drop=True)
    return df

def search_closest_colname(col: str, colnames_df: dict) -> str:
    # Entrega un pd.Series con los índices del dataframe y valores de ratio de semejanza entre nombres de columna del 3CV registrados en campos_hom.json y el nombre que no tiene un nombre estandar.
    closest_values = colnames_df['3CV_NAMES'].apply(lambda dcol: SequenceMatcher(lambda jnk: jnk in [" ","\n"], col,dcol).ratio())
    # Sacamos el más semejante y su nombre estandar registrado (este se encuentra en el registro COLNAMES)
    stdr_name = colnames_df.iloc[closest_values.argmax()]["STANDARD_NAME"]
    return stdr_name


def update_standard_column_names(colnames: dict) -> None:
    logging.info("Actualizando base de datos de campos estandarizados...")
    filename = f"{FOLDER_PROCESSED}campos_hom_tmp.json"
    if colnames:
        with open(filename, "w") as f:
            json.dump(colnames, f, indent=2)
            logging.info("Datos guardados en: %s", filename)
    else:
        raise ValueError("Datos vacíos: No guardado")


def estandarizacion_columnas(datos: pd.DataFrame, datacolnames: dict, colnames: dict) -> list:
    standars_colname = {}
    df_colnames = column_json2df(colnames)
    for unnamedk,defcolname in datacolnames.items():
        hashvalue = hashing(defcolname)
        if check_hash(hashvalue,colnames):
            standars_colname[unnamedk] = renameCol(defcolname,colnames)
        else:
            closest_stdr_name = search_closest_colname(defcolname,df_colnames)
            # update the colnames
            colnames[closest_stdr_name]["default"].append(defcolname)
            colnames[closest_stdr_name]["hash"].append(hashvalue)
            standars_colname[unnamedk] = closest_stdr_name
    return [colnames,standars_colname]

def transform_headers_main() -> list:
    #%% Lectura y actualización de columnas
    # Leemos las dos primeras hojas
    try:
        data_aux = pd.read_excel(f"{FOLDER_RAW_LOCAL}/{RAWDATANAME}.xls",sheet_name=[0,1], dtype=str)
    except FileNotFoundError():
        os.listdir(FOLDER_RAW_LOCAL)

    logging.info("Lectura de nombres de campos estandarizados...")
    # Lectura de los nombres de campos
    with open(f"{FOLDER_PROCESSED}{COLNAMES_FILE}") as jsonfile:
        COLNAMES = json.load(jsonfile)
    data = []
    for g,df in data_aux.items():
        maxrow,data_colnames = identify_headers(df)
        colnames_updated,standard_dic = estandarizacion_columnas(df,data_colnames,COLNAMES)
        df2 = df.rename(columns = standard_dic)
        df2 = df2.iloc[maxrow:,:]
        update_standard_column_names(colnames_updated)
        data.append(df2)
    return data


#nedf = transform_headers_main()

#FOLDER_TMP.mkdir(parents=True, exist_ok=True)
#filename = FOLDER_TMP / "datos_tmp.csv"
#logging.info("TESTEANDO:...")
#nedf[1].to_csv(filename)
#logging.info("Se guarda archivo test: %s",filename)





