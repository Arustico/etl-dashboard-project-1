#----------------------------
# Librerías
#----------------------------
import pandas as pd
"""
El módulo tiene como objetivo:
    1. Detectar qué filas parecen ser headers
    2. Filtrar niveles válidos
    3. Agrupar nombres de niveles y aplanarlos en una cadena
    5. Construir diccionario entre id columna con datos y columna aplanada

Devuelve metadata + resultado

"""

#----------------------------
# FUNCIONES
#----------------------------

def find_header_rows(df: pd.DataFrame) -> list[int|None]:
    """Encuentra el índice de la fila donde comienzan los encabezados por columna."""
    header_row_index = []
    # Busca el primer valor no nulo, ese debe ser el "padre"
    for col in df.columns:
        first_valid = df[col].first_valid_index()
        header_row_index.append(first_valid)
    return header_row_index


def select_header_levels(levels: list[int | None], max_scan: int = 10) -> list[int]:
    """Filtra niveles no nulos o plausible de encabezado."""
    valid_levels = [x for x in levels if x is not None and x < max_scan]

    if not valid_levels:
        raise ValueError("No se detectaron niveles de encabezado válidos.")

    maxrow = max(valid_levels)
    clean_levels = sorted(set(x for x in valid_levels if x < maxrow))

    return clean_levels

def extract_header_dataframe(df: pd.DataFrame, levels: list[int]) -> pd.DataFrame:
    """
    Extrae las filas que forman parte de la estructura jerárquica y construye un dataframe.
    """
    header_rows = df.iloc[levels]
    df_header_cols = header_rows.T.dropna(how='all')
    #df_header_cols = df_header_cols.reset_index() # se guardan índices originales
    return df_header_cols


def modify_header_structure(df_header_cols:pd.DataFrame, levels) -> pd.DataFrame:
    """
    Modifica la estructura del header-dataframe limpia los valores nan dependiendo de la cantidad de niveles de la columna.
    """
    # La primera columna corresponde al "padre" de la jerarquía
    df_header_cols.iloc[:,0] = df_header_cols.iloc[:,0].ffill()
    # Si los niveles son más de 2: Cambia los "nan" por "".
    # df_header_cols.iloc[
    #     (df_header_cols[levels[1]].isna())
    #     &(df_header_cols[levels[2]].isna()),1:3] = ""
    # df_header_cols = df_header_cols.fillna("")
    # Importante restear el índice acá para conservar estructura original
    df_header_cols = df_header_cols.reset_index()

    return df_header_cols


def build_flatten_columns_names(df_header_cols: pd.DataFrame, levels: list[int]) -> dict[int, str]:
    """ Reestructura los nombres de los headers uniendolos en un solo nombre para toda la columna según su padre y reconstruye el nombre de la columna de forma plana"""
    newcolsname = []
    for parent, gr in df_header_cols.groupby(levels[0]):
        #print(f"PADRE:{parent} GRUPOS:{gr}\n")
        if len(gr) > 1:
            if len(levels) >= 2:
                gr[levels[0]] = gr[levels[0]].ffill()
                gr[levels[1]] = gr[levels[1]].ffill()
                #gr[levels[2]] = gr[levels[2]].ffill()
        if len(levels) >= 3:
                gr[levels[1]] = gr[levels[1]].ffill()
                gr[levels[2]] = gr[levels[2]].ffill()#fillna("")
                #gr[levels[3]] = gr[levels[3]].ffill()#fillna("")

        newcolsname.append(gr.fillna(""))

    newcolsname = pd.concat(newcolsname)

    newcolsname["combcol"] = newcolsname[levels[::-1]].apply(
        lambda row: " ".join(
            str(v) for v in row
            if v != "" and "Unnamed:" not in str(v)
        ),
        axis=1
    )
    newcolsname = newcolsname.sort_index()
    newcolsname.to_csv("tmp/identificacion_hd.csv")
    return newcolsname.set_index("index")["combcol"].to_dict()


def identify_headers(df: pd.DataFrame) -> tuple[int, dict[int, str]]:
    """
    Pipeline completo de identificación de encabezados.
    """
    levels_raw = find_header_rows(df)
    levels = select_header_levels(levels_raw)

    maxrow = max(levels)
    header_matrix = extract_header_dataframe(df, levels)
#    print(f"\nPASO: 'extract_head...'\n {header_matrix}")

    header_matrix = modify_header_structure(header_matrix, levels)
    #print(f"\nPASO: 'modify_head...'\n {header_matrix}")

    column_names = build_flatten_columns_names(header_matrix, levels)
    #print(f"\nPASO: 'build_flat...'\n {column_names}")

    return maxrow, column_names



def identify_headers_old(df:pd.DataFrame()) -> dict():
    """
    Identifica y transforma los headers de los dataframes.
    """
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
    newcolsname['combcol'] = newcolsname[levels].apply(lambda row: " ".join([str(v) for v in row if (v != "")
                                                                             and("Unnamed:" not in v)]), axis=1)
    newcolsname = newcolsname[['index','combcol']]
    newcolsname = newcolsname.set_index('index')

    dictofnames = newcolsname['combcol'].to_dict()
    return [maxrow,dictofnames]

def main():
    from dotenv import load_dotenv
    from pathlib import Path
    import os
    load_dotenv("./variables_local.env")
    FOLDER_RAW_LOCAL = Path(os.getenv("FOLDER_RAW"))
    RAWDATANAME = os.getenv("RAWDATANAME", "dataRawHom")

    data_aux = pd.read_excel(f"{FOLDER_RAW_LOCAL}/{RAWDATANAME}.xls",sheet_name=[0,1], dtype=str)
    # Lectura datos
    df = data_aux[0]
    # Mapeo e identificación inicial de headers
    maxrow, map_headers_raw = identify_headers(df)
    headers_raw = map_headers_raw.values()
    print("="*40)
    #print(map_headers_raw)
    print("="*40)

if __name__ == "__main__":
    main()

