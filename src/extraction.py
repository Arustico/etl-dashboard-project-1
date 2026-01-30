import pandas as pd
import requests
from io import BytesIO
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv("./variables_local.env")

FOLDER_RAW_LOCAL = os.getenv("FOLDER_RAW")
URL_3CV = os.getenv("URL_3CV")

CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv("BUCKET_NAME")
RAWDATANAME = "dataRawHom"
SEP = 20*"="


def extraction_from_3cv(url=URL_3CV,folder=FOLDER_RAW_LOCAL,rawdataname=RAWDATANAME,filetype = 'xls'):
    """
    Descarga datos directamente desde 3CV y los guarda con nombre FILENAME
    en la carpeta FOLDER_RAW
    """
    print(f"{SEP}\n LEYENDO PÁGINA 3CV...")
    print(f"{SEP}\n")

    page = requests.get(url,verify=False)
    soup = BeautifulSoup(page.content, "html.parser")
    link_data = soup.find(id="brxe-dqzlqf").get("href")

    print(f"{SEP}\n")
    print("DESCARGA DE DATOS")

    # Obtención datos 3CV con url
    response = requests.get(link_data,verify=False)
    if response.status_code == 404:
        print("\nLink de descarga está desactualizado, o base de datos no subida por 3CV\n")
        print(f"LINK: {link_data}")
        return False
    elif response.status_code == 200:
        filename = os.path.join(folder,f"{rawdataname}.{filetype}")
        with open(filename, "wb") as f:
            f.write(response.content)
        return True
    print(f"{SEP} +\n PROCESO FINALIZADO")


def init_cliente_gcp(cred = CREDENTIALS):
    # crear variable local GOOGLE_CREDE
    try:
        client = storage.Client.from_service_account_json(cred)
    except:
        raise ConnectionError("No se pudo conectar al Bucket")
    else:
        print("Conexión Exitosa")
        return client

def write_on_bucket(storage_client_gcp, bucket_name=BUCKET_NAME,filename=RAWDATANAME,filetype='xls'):
    """
    Escribe desde un una carpeta local, el archivo para resguardarlo en un bucket
    """
    filename = f"{filename}.{filetype}"
    filenametoupload = os.path.join(FOLDER_RAW_LOCAL,filename)
    bucket = storage_client_gcp.bucket(bucket_name)
    blob = bucket.blob(f"data/raw/{filename}")

    blob.upload_from_filename(filenametoupload)

def read_from_bucket(storage_client_gcp, bucket_name=BUCKET_NAME,filetype='xls'):
    """
    Lee desde un bucket el archivo buck up.
    """
    print(f"{SEP}\nLEYENDO BACK UP")
    filename = f"{RAWDATANAME}.{filetype}"
    destination_file_name = f"{FOLDER_RAW_LOCAL}/{filename}"

    bucket = storage_client.get_bucket(bucket_name)
    # crear un objeto blob del archivo que se quiere
    try:
        blob = bucket.blob(f"data/raw/{filename}")
    except:
        print("Blobs disponibles:")
        blobs = storage_client.list_blobs(bucket_name)
        for blob in blobs:
            print(blob.name)
    try:
        # descarga a un archivo de destino
        blob.download_to_filename(destination_file_name)
        print("DESCARGA LOCAL EXITOSA")
    except:
        print("NO SE COMPLETO DESCARGA")
        raise Exception("No se llevó a cabo la descarga")

def extraction_main():
    """
    Función principal que combina las funciones anteriores
    """
    # Extracción directa desde 3CV
    filetype = 'xls'
    extraction_3cv_bool = extraction_from_3cv(url=URL_3CV, folder=FOLDER_RAW_LOCAL,filetype=filetype)
    if extraction_3cv_bool:
        print("Está archivo")
        print("Descarga local")
        try:# actualización de bucket
            print("ACTUALIZACIÓN DEL BACK UP")
            print(f"{SEP}\n CONECTANDO A GCP STORAGE")
            client = init_cliente_gcp(CREDENTIALS)
            write_on_bucket(client,bucket_name=BUCKET_NAME,filename=RAWDATANAME,filetype=filetype)
            print(f"ACTUALIZACIÓN BACK-UP COMPLETADA\n{SEP}")
        except:
            print("No se pudo actualizar back-up")
    else:
        client = init_cliente_gcp(CREDENTIALS)
        read_from_bucket(client, bucket_name=BUCKET_NAME,filetype='xls')

extraction_main()


















