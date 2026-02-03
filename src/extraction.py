# Refactor de src/extraction.py
# Mejoras: logging, manejo de errores, pathlib, type hints, corrección de bugs

import logging
from pathlib import Path
from typing import Optional
import os

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.cloud import storage

# Cargar variables de entorno
load_dotenv("./variables_local.env")

# Configuración básica
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("extraction")

# Variables de configuración (asegúrate que están definidas en el entorno)
FOLDER_RAW_LOCAL = Path(os.getenv("FOLDER_RAW", "data/raw"))
URL_3CV = os.getenv("URL_3CV")
CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv("BUCKET_NAME")
RAWDATANAME = os.getenv("RAWDATANAME", "dataRawHom")
DEFAULT_FILETYPE = os.getenv("DEFAULT_FILETYPE", "xls")
DEFAULT_VERIFY_TLS = os.getenv("VERIFY_TLS", "true").lower() != "false"

def ensure_configured() -> None:
    """Verifica que las variables de entorno mínimas estén presentes."""
    missing = []
    if not URL_3CV:
        missing.append("URL_3CV")
    if not BUCKET_NAME:
        missing.append("BUCKET_NAME")
    if not CREDENTIALS:
        logger.warning("GOOGLE_APPLICATION_CREDENTIALS no definido: algunas operaciones GCP fallarán.")
    if missing:
        raise EnvironmentError(f"Faltan variables de entorno requeridas: {', '.join(missing)}")

def extraction_from_3cv(
    url: str = URL_3CV,
    folder: Path = FOLDER_RAW_LOCAL,
    rawdataname: str = RAWDATANAME,
    filetype: str = DEFAULT_FILETYPE,
    verify_tls: bool = DEFAULT_VERIFY_TLS,
) -> Path:
    """
    Descarga los datos desde la página 3CV detectando el enlace y guardándolo localmente.
    Devuelve la Path al archivo guardado o lanza excepción si falla.
    """
    if not url:
        raise ValueError("No se proporcionó URL para la extracción (URL_3CV).")

    logger.info("Leyendo página 3CV para localizar enlace de descarga...")
    try:
        res = requests.get(url, timeout=15, verify=verify_tls)
        res.raise_for_status()
    except Exception as e:
        logger.exception("Error al leer la página 3CV")
        raise

    soup = BeautifulSoup(res.content, "html.parser")
    anchor = soup.find(id="brxe-dqzlqf")
    if anchor is None or not anchor.get("href"):
        raise LookupError("No se encontró el elemento con id 'brxe-dqzlqf' o no contiene href.")

    link_data = anchor.get("href")
    logger.info("Enlace de datos detectado: %s", link_data)

    logger.info("Descargando datos desde enlace detectado...")
    try:
        response = requests.get(link_data, timeout=30, verify=verify_tls)
        response.raise_for_status()
    except requests.HTTPError as he:
        if response.status_code == 404:
            logger.error("Link de descarga está desactualizado o la base no está subida (404).")
        logger.exception("Error HTTP al descargar datos.")
        raise
    except Exception:
        logger.exception("Error al descargar los datos desde 3CV.")
        raise

    # Guardar archivo
    folder.mkdir(parents=True, exist_ok=True)
    filename = folder / f"{rawdataname}.{filetype}"
    try:
        with open(filename, "wb") as f:
            f.write(response.content)
        logger.info("Archivo guardado localmente en %s", str(filename))
        return filename
    except Exception:
        logger.exception("No se pudo escribir el archivo descargado en disco.")
        raise

def init_gcp_client(cred: Optional[str] = CREDENTIALS) -> storage.Client:
    """
    Inicializa y devuelve un cliente de Google Cloud Storage usando credenciales JSON.
    Lanza excepción si no se puede inicializar.
    """
    try:
        if cred:
            client = storage.Client.from_service_account_json(cred)
        else:
            # Usa credenciales de entorno/entorno del runtime si no se pasa ruta
            client = storage.Client()
        logger.info("Conexión GCP Storage establecida.")
        return client
    except Exception:
        logger.exception("No se pudo conectar al Bucket GCP.")
        raise ConnectionError("No se pudo conectar al Bucket GCP.")

def upload_to_bucket(
    storage_client: storage.Client,
    bucket_name: str,
    local_file: Path,
    destination_prefix: str = "data/raw/",
) -> None:
    """
    Sube un archivo local al bucket en la ruta destination_prefix/<filename>.
    """
    if not local_file.exists():
        raise FileNotFoundError(f"Archivo a subir no encontrado: {local_file}")

    bucket = storage_client.bucket(bucket_name)
    destination_blob_name = f"{destination_prefix}{local_file.name}"
    blob = bucket.blob(destination_blob_name)
    try:
        blob.upload_from_filename(str(local_file))
        logger.info("Archivo subido a GCS: gs://%s/%s", bucket_name, destination_blob_name)
    except Exception:
        logger.exception("Error subiendo archivo al bucket.")
        raise

def download_from_bucket(
    storage_client: storage.Client,
    bucket_name: str,
    filename: str = f"{RAWDATANAME}.{DEFAULT_FILETYPE}",
    destination_folder: Path = FOLDER_RAW_LOCAL,
    destination_prefix: str = "data/raw/",
) -> Path:
    """
    Descarga desde GCS el archivo especificado y lo guarda en destination_folder.
    Devuelve la Path al archivo descargado.
    """
    destination_folder.mkdir(parents=True, exist_ok=True)
    destination_path = destination_folder / filename

    try:
        bucket = storage_client.bucket(bucket_name)
    except Exception:
        logger.exception("No se pudo obtener referencia al bucket.")
        raise

    blob = bucket.blob(f"{destination_prefix}{filename}")
    if not blob.exists():
        # listar blobs para ayuda al debug
        logger.error("El blob %s no existe en gs://%s", f"{destination_prefix}{filename}", bucket_name)
        blobs = list(storage_client.list_blobs(bucket_name, prefix=destination_prefix))
        if blobs:
            logger.info("Blobs disponibles en %s (prefijo %s):", bucket_name, destination_prefix)
            for b in blobs:
                logger.info(" - %s", b.name)
        raise FileNotFoundError(f"No existe el blob {destination_prefix}{filename} en el bucket {bucket_name}.")

    try:
        blob.download_to_filename(str(destination_path))
        logger.info("Descarga local exitosa: %s", str(destination_path))
        return destination_path
    except Exception:
        logger.exception("No se completó la descarga desde bucket.")
        raise


def extraction_main() -> None:
    """
    Orquestador principal:
    - intenta extraer desde 3CV y actualizar backup en GCS
    - si falla la extracción, intenta descargar el backup desde GCS
    """
    try:
        ensure_configured()
    except EnvironmentError as e:
        logger.error("Configuración incompleta: %s", e)
        raise

    filetype = DEFAULT_FILETYPE
    try:
        downloaded_file = extraction_from_3cv(filetype=filetype)
        logger.info("Extracción desde 3CV realizada: %s", downloaded_file)
        # subir backup
        try:
            client = init_gcp_client(CREDENTIALS)
            upload_to_bucket(client, BUCKET_NAME, downloaded_file)
            logger.info("Actualización del backup completada.")
        except Exception:
            logger.exception("No se pudo actualizar el backup en GCP.")
    except Exception:
        logger.warning("No se pudo extraer desde 3CV, intentando leer backup desde bucket...")
        try:
            client = init_gcp_client(CREDENTIALS)
            downloaded = download_from_bucket(client, BUCKET_NAME, filename=f"{RAWDATANAME}.{filetype}")
            logger.info("Archivo restaurado desde bucket: %s", downloaded)
        except Exception:
            logger.exception("No fue posible restaurar el archivo desde el bucket. Proceso abortado.")
            raise


if __name__ == "__main__":
    extraction_main()
