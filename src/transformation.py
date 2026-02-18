#====================================
# PIPELINE
#====================================
"""
    1. Lectura de dataframes
    2. Identificación de headers por dataframe (crear funcion batch para df)
    3. Estandarización de headers por df
    4. Limpieza y transformación por df según columnas
        4.1 Transformaciones dtype
        4.2 Estandarización de importadores
        4.3 Estandarización de Categorias (phev, hev, ev, combustion)
        4.4 Cálculo de rendimiento según norma
    5. Guardado df según último año.



"""
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
    logging.info("Estandarización de importadores completada con éxito")
    return [data,imp_not_found]

def save_data(data: pd.DataFrame) -> None:
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
# Creacion de año
data['AÑO'] = data['FECHA_HOM'].dt.year

# Asignacion de tipo de propulsión
data['CATEG_PROP'] = ''
data.loc[data['PROPULSION'].isin(['Combustión','Vehículos híbridos sin recarga exterior',
                                     'Eléctrico de Rango Extendido']),'CATEG_PROP']='ICE'
data.loc[data['PROPULSION'].isin(['Combustión','Eléctrico de Rango Extendido']),'CATEG_PROP']='ICE'
data.loc[data['PROPULSION'].isin(['Vehículo eléctrico']),'CATEG_PROP']='BEV'
data.loc[data['PROPULSION'].isin(['Vehículos híbrido con recarga exterior']),'CATEG_PROP']='PHEV'
data.loc[data['PROPULSION'].isin(['Vehículos híbridos sin recarga exterior']),'CATEG_PROP']='HEV'

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


# Cálculo de rendimiento equivalente
data['REND_EQUIV'] = ''
# Para combustibles e híbridos no enchufables es el mixto
bools_hb = data['PROPULSION'].isin(
  ['Combustión','Vehículos híbridos sin recarga exterior',
   'Eléctrico de Rango Extendido'])
#data.loc[bools_hb,'REND_EQUIV'] = data.loc[bools_hb,'REND_MIXTO_KML']

bools_diesel = data['COMBUSTIBLE'].isin(['Diésel','Diésel '])
#data.loc[bools_diesel,'REND_EQUIV'] = data.loc[bools_diesel,'REND_EQUIV']*0.87

# Para híbridos enchufables es el de ciudad: Usaremos REND_PON_HIB_KML
# (más cercano al de cumplimiento. Se necesita valores de autonomía)
bools_hb2 = data['PROPULSION'].isin(
  ['Vehículos híbrido con recarga exterior',
   'Eléctrico híbrido con recarga exterior'
   ])
#data.loc[bools_hb2,'REND_EQUIV'] = data.loc[bools_hb2,'REND_PON_HIB_KML']

# Para eléctricos el factor de conversión es:
dens_ener_gas = 8.60
bools_ev = data['PROPULSION'].isin(['Vehículo eléctrico'])
#rnd = pd.to_numeric(data.loc[bools_ev,'REND_EV_KMKWH'].replace('-',np.nan))
#rnd = rnd.fillna(rnd.mean())
#data.loc[bools_ev,'REND_EQUIV'] = rnd*dens_ener_gas

#Chequeo
save_data(data)


