#%% LIBRERIAS
import pandas as pd
import logging
from transform_headers import transform_headers_main

sep = "="*30
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
newdata = pd.concat(newdata)
print(newdata)

