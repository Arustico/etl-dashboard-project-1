#----------------------------
#%% LIBRERIAS
#----------------------------

import pandas as pd
#from header_standarizer_llm import HeaderStandardizer
from header_standarizer_ruler import HeaderStandardizerRules
from header_identify_processing import identify_headers,identify_headers_old

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

# Configuración básica para .log
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

name_script = Path(__file__).name
logger = logging.getLogger(f"Transformacion Encabezados ({name_script})...")

#----------------------------
# INICIO DE CÓDIGO
#----------------------------

data_aux = pd.read_excel(f"{FOLDER_RAW_LOCAL}/{RAWDATANAME}.xls",sheet_name=[0,1], dtype=str)

# Lectura datos
df = data_aux[0]

# Mapeo e identificación inicial de headers
maxrow, map_headers_raw = identify_headers(df)

headers_raw = map_headers_raw.values()

#print(headers_raw)
standardizer = HeaderStandardizerRules()
mapping = standardizer.batch_standardize(headers_raw)
#standardizer.export_to_csv(MAPPING_HEADERS_CSV)

#for orig, std in mapping.items():
#    print(f"\nCambio: {orig[:40]}... --> {std}")






