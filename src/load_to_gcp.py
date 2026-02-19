#----------------------------
# LIBRERÍAS
#----------------------------
import pandas as pd
from typing import Optional

from transform_pipeline import read_xls_files, pipeline_transformation
from extraction import init_gcp_client, upload_to_bucket

import os
from pathlib import Path
from dotenv import load_dotenv
import logging

#----------------------------
# CONFIGURACIONES
#----------------------------
load_dotenv("./variables_local.env")

FOLDER_RAW_LOCAL = Path(os.getenv("FOLDER_RAW", "data/raw"))
FOLDER_PROCESSED = os.getenv("FOLDER_PROCESSED")
CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv("BUCKET_NAME")
RAWDATANAME = os.getenv("RAWDATANAME", "dataRawHom")
DEFAULT_FILETYPE = os.getenv("DEFAULT_FILETYPE", "xls")
DEFAULT_VERIFY_TLS = bool(os.getenv("VERIFY_TLS"))
FOLDER_TMP = Path(os.getenv("FOLDER_TMP"))


usedcolumns =[# informativos
            'MARCA', 'MODELO','CODIGO_INFORME_TECNICO','FECHA_HOML','FOOT_PRINT_MT2',
            'AÑO', 'TIPO_LDV', 'CATEGORIA_PROPULSION', 'RUT', 'IMP_COD','EMIS_NORMA',
            'CATEGORIA_VH','PESO_BRUTO_VH_KG','TRANSMISION',
            # rendimientos
            'EMIS_CO2_EQUIV','REND_EQUIV_KML',
            # Gases
            'N2O_EMISION_EPA','MP_EMISION_EPA_MASA_PARTICULAS_GKM','HCHO_EMISION_EPA_MGKM', 'HC_EMISION_EPA_GKM','HCNM_EMISION_EPA_GKM','NMOG_NOX_EMISION_EPA','NOX_EMISION_EPA_GKM',
            'NMOG_EMISION_EPA_GKM','CO_EMISION_EPA_GKM', # EPA
            'HCHO_EMISION_EU_MGKM','EMISION_NPS_KM_EU_KM','HC_NOX_EMISION_EU_GKM','EMISION_NPS_KM_EU_KM',
            'NMOG_EMISION_EU_GKM','HCNM_EMISION_EU_GKM','CO_EMISION_EU_GKM','MP_EMISION_MASA_PARTICULAS_EU_GKM',
            'NOX_EMISION_EU_GKM','HC_EMISION_EU_GKM', #EU
            ]

#----------------------------
# FUNCIONES
#----------------------------
def set_filename(df: pd.DataFrame, column: str = "AÑO",
                 name:str="datos3cv") -> Path:
    """
    Configura el nombre de los archivos de datos limpiados
    """
    yrs = df[column].apply(['min','max']).to_list()
    filename = f"{name}_{yrs[0]}-{yrs[1]}.csv"
    pathfile = Path(f"{FOLDER_PROCESSED}{filename}")
    return pathfile

def save_data(df: pd.DataFrame, pathfile: Path, usedcolumns: Optional[list[str]]=usedcolumns) -> None:
    df = df[usedcolumns]
    df.to_csv(pathfile,index=False)

#----------------------------
# INICIO CODIGO
#----------------------------
# Lectura datos

filename_in = f"{FOLDER_RAW_LOCAL}/{RAWDATANAME}.xls"

print("="*80)

logging.info("Iniciando Lectura xls...")
data = read_xls_files(filename_in,num_sheets=2)
df = data[0] # Solo usaremos el primer datasheet
print("="*80)

#Transformación de datos
print("="*80)
logging.info("Iniciando transformaciones...")
df = pipeline_transformation(df)
print("="*80)


# Guardado local
filename_out = set_filename(df)
save_data(df, filename_out)
logging.info("Datos guardados en {filename_out}")
print("="*80)


# Uploead to bucket
logging.info("Iniciando subida a Bucket: {BUCKET_NAME}")
gcp_client = init_gcp_client(CREDENTIALS)
upload_to_bucket(gcp_client, BUCKET_NAME,local_file = filename_out, destination_prefix="data/processed/")


#
#


