
import transform_headers_utils as tfu
import pandas as pd

# Cargar mapeos
simplifier = tfu.HeaderSimplifier(method="transformers")

#simplifier.load_from_json("../tmp/header_mappings.json")

# Supongamos que tienes un DataFrame con los encabezados originales
#df = pd.read_csv("tu_archivo.csv")

# Renombrar columnas usando el mapeo
#rename_dict = simplifier.get_simplified_dict()
#df.rename(columns=rename_dict, inplace=True)

# Ahora df tiene columnas simplificadas
#print(df.columns)
