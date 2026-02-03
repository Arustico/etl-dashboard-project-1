#%% LIBRERIAS
import pandas as pd
import os
import numpy as np
from difflib import SequenceMatcher
import json
import hashlib

#%% FUNCIONES
def hashing(name,length=12):
  return hashlib.sha256(name.encode()).hexdigest()[:length]

def find_header_rows(df: pd.DataFrame) -> list:
    """Encuentra el índice de la fila donde comienzan los encabezados de la data xls."""
    header_row_index = []
    # Busca el primer valor no nulo, ese debe ser el "padre"
    for col in df.columns:
        first_valid = df[col].first_valid_index()
        header_row_index.append(first_valid)
    return header_row_index

def transform_headers(df:pd.DataFrame()) -> dict():
    """
    Identifica los headers de los dataframes.
    """
    # busqueda de niveles no nulos
    print("Buscando headers")
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

    print("Reestructurando headers")
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
    return [maxrow,valid_levels,levels,dictofnames]

def selectColData(df,colnames):
    """
    Lee los datos y transforma correctamente los nombres de las columnas
    """
    maxrow,valid_levels,levels,diccols = transform_headers(df)
    col_data = {}
    for defaultcol,oldcolname in diccols.items():
        values = df.loc[maxrow:,defaultcol].tolist()
        newcolname = renameCol(oldcolname, colnames)
        col_data[newcolname] = values
        print(oldcolname,newcolname)
    # for hd,colname in zip(levels,df.columns):
    #     if hd is None: continue
    #     try:
    #         coldefname = diccols[colname]
    #         newcolname = renameCol(coldefname)
    #         values = df.loc[maxrow:,colname].tolist()
    #         col_data[newcolname] = values
    #     except: continue
    return col_data

def renameCol(col,colnames: dict) -> str():
    """
    Renombre las columnas a partir de un estandarizado
    """
    hashval = hashing(col)
    for colname,values in colnames.items():
        if hashval in values['hash']:
            return colname
        else: return None

from datetime import datetime

with open('./datos/homologacion/campos_hom_data.json') as jsonfile:
  COLNAMES = json.load(jsonfile)

dicolumns = transform_headers(df)
def check_hash(hashval,colnames=COLNAMES):
  for colname,values in colnames.items():
    if hashval in values['hash']:
      return True
  else: return False


def newhasvalues(dicolumns):
  dic = {}
  k = 0
  for dfvl,oln in dicolumns.items():
    hashval = hashing(oln)
    if check_hash(hashval): continue
    else:
      kval = f"key_{k}"
      dic.update(
        {kval:{'default':oln,
               'hash':hashval}
         })
    k+=1

  folder = "./datos/tmp"
  filebase = f"{folder}/campos_hom_tmp"
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  filename = f"{filebase}_{timestamp}.json"
  with open(filename, "w") as f:
     json.dump(dic, f, indent=2)
  print(f"Archivo guardado en {filename}")
newhasvalues(dicolumns)
















