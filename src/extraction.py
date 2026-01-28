import pandas as pd
import requests
from io import BytesIO
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv("./variables_local.env")

FOLDER_RAW = os.getenv("FOLDER_RAW")
URL_3CV = os.getenv("URL_3CV")
FILENAME = "{FOLDER_RAW}+dataraw.csv"
SEP = 20*"="

#print(FOLDER_RAW)

def extraction_from_3cv(URL_3CV, FILENAME):
    """
    Descarga datos directamente desde 3CV y los guarda con nombre FILENAME
    en la carpeta FOLDER_RAW
    """
    print(f"{SEP}\n LEYENDO PÁGINA 3CV...")
    print(f"{SEP}\n")

    page = requests.get(URL_3CV,verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    link_data = soup.find(id="brxe-dqzlqf").get("href")

    print(f"{SEP}\n")
    print("DESCARGA DE DATOS")

    # Obtención datos 3CV con url
    response = requests.get(link_data,verify=False)
    if response.status_code == 404:
        print("\nLink de descarga está desactualizado, o base de datos no subida por 3CV\n")
        print(f"LINK: {link_data}")
        raise Exception("Datos no encontrados")
    elif response.status_code == 200:
        with open(FILENAME, "wb") as f:
            f.write(response.content)
    print(f"{SEP} +\n PROCESO FINALIZADO")




from google.cloud import storage
CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv("BUCKET_NAME")
def init_cliente_gcp(CREDENTIALS):
    # crear variable local GOOGLE_CREDE
    client = storage.Client.from_service_account_json(CREDENTIALS)
    return client

print(f"{SEP}\n CONECTANDO A GCP STORAGE")
client = init_cliente_gcp(CREDENTIALS)
for n in client.list_blobs(BUCKET_NAME):
    print(n)

























