import pandas as pd
import requests
from io import BytesIO
from bs4 import BeautifulSoup

SEP = 20*"="
FOLDER_RAW = "./data/raw/"

print(f"{SEP}\n LEYENDO PÁGINA 3CV...")
print(f"{SEP}\n")

URL_3CV = "https://www.subtrans.gob.cl/3cv/homologacion-de-vehiculos-livianos-medianos-y-motocicletas/"

page = requests.get(URL_3CV,verify=False)
soup = BeautifulSoup(page.content, "html.parser")
link_data = soup.find(id="brxe-dqzlqf").get("href")

print(f"{SEP}\n")
print("EXTRACCIÓN DE DATOS")

# Obtención datos 3CV con url
response = requests.get(link_data,verify=False)
if response.status_code == 404:
    print("\nLink de descarga está desactualizado, o base de datos no subida por 3CV\n")
    raise Exception("Datos no encontrados")
elif response.status_code == 200:
    excel_bytes = BytesIO(response.content)
    data_df = pd.read_excel(excel_bytes)
    #  Bajando la data en raw
    filename = "{FOLDER_RAW}+data_3cv_homolog_raw.csv"
    data_df.to_csv(filename)

print(f"{SEP} +\n PROCESO FINALIZADO")
