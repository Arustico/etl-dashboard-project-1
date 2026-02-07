#====================================
#%% LIBRERIAS
#====================================
import os
import pandas as pd
import numpy as np

import logging
from dotenv import load_dotenv
from pathlib import Path

from difflib import SequenceMatcher
from transform_headers import transform_headers_main

#====================================
# DEFINICIONES
#====================================
# se cargan variables locales
load_dotenv("./variables_local.env")

FOLDER_RAW_LOCAL = Path(os.getenv("FOLDER_RAW"))
FOLDER_TMP = Path(os.getenv("FOLDER_TMP"))
FOLDER_PROCESSED = Path(os.getenv("FOLDER_PROCESSED"))
BD_IMPORTADORES = os.getenv("BD_IMPORTADORES")

# Configuración básica para .log
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("transformacion de Data")

#====================================
# FUNCIONES
#====================================
sep = "="*30

def select_columnas_utiles() -> pd.DataFrame:
    logging.info(f"TRANSFORMACIÓN DE ENCABEZADO...\n{sep}")
    data = transform_headers_main()

    logging.info(f"FILTRO DE COLUMNAS ÚTILES...\n{sep}")
    unified_colnames = [list(df.keys()) for df in data]
    unified_colnames = list(
        set.intersection(*map(set, unified_colnames))
        )
    unified_colnames = [col for col in unified_colnames if (("Unnamed" not in col)and("key_" not in col))]
    #print(unified_colnames)

    newdata = []
    for df in data:
        df2 = df[unified_colnames]
        newdata.append(df2)
    # Updating data
    data = pd.concat(newdata).reset_index(drop=True)
    logging.info("DATA CARGADA ...")
    print(f"{sep}\n{data.head(5)}\n{sep}")
    return data

def estandarizacion_importadores(data: pd.DataFrame) -> pd.DataFrame:
    logging.info("Estandarización de nombres de Importadores")
    filename = FOLDER_PROCESSED / f"{BD_IMPORTADORES}.csv"
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
    logging.info("Estandarización de impotadores completada con éxito")
    return [data,imp_not_found]

def check_data(data: pd.DataFrame) -> None:
    filename_tmp = FOLDER_TMP / "datos_tmp.csv"
    data.to_csv(filename_tmp)
    logging.info("Visualización temporal en: %s",filename_tmp)


#====================================
# INICIO CÓDIGO
#====================================
# lectura y filtrado de columnas útiles
data = select_columnas_utiles()
# Estandarizacion de nombres de importadores
data = estandarizacion_importadores(data) # un 9% de la data no estaba dentro de las bases de importadores
data = data[0]

#
# not_found_impt = data[1]
# data = data[0]
# N = len(data)
# ratio_not_found = list(map(lambda imp: 100*len(data[data["IMPORTADOR"]==imp])/N, not_found_impt))
# df_imp_notfound = pd.DataFrame({"IMPORTADOR":not_found_impt,"RATIO":ratio_not_found})
# df_imp_notfound = df_imp_notfound.sort_values(by="RATIO",ascending=False).reset_index()
# print(df_imp_notfound.head(10),df_imp_notfound.iloc[0:10,2].sum())

# Transformaciones dtype
logging.info("Transformaciones según tipo de datos por columna...")
# Fecha
data['FECHA_HOM'] = data['FECHA_HOM'].replace('-',pd.NA)
data['FECHA_HOM'] = data['FECHA_HOM'].ffill()
data['FECHA_HOM'] = pd.to_datetime(data['FECHA_HOM'])

# Rendimientos
#data['REND_PON_HIB_KML'] = pd.to_numeric(data['REND_PON_HIB_KML'],errors='coerce')
data['REND_MIXTO_KML'] = data['REND_MIXTO_KML'].replace('-',pd.NA)
data['REND_MIXTO_KML'] = pd.to_numeric(data['REND_MIXTO_KML'],errors='coerce')
data.loc[data['REND_MIXTO_KML'].isna(),'REND_MIXTO_KML'] = data.loc[:,'REND_MIXTO_KML'].mean()

# Peso bruto
# El valor - coresponde al valor anterior
data['PBV_KG'] = data['PBV_KG'].replace('-',pd.NA)
data['PBV_KG'] = data['PBV_KG'].ffill()
data['PBV_KG'] = pd.to_numeric(data['PBV_KG'])



check_data(data)
