#!/usr/bin/env python3

import os
import sys
import requests
import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError, ClientError

API_URL = "https://www.datos.gov.co/resource/gt2j-8ykr.csv"

# 👉 CAMBIA ESTO POR TU BUCKET REAL
BUCKET_NAME = "nruizudatalake"

# Prefijo base en tu datalake
BASE_PREFIX = "raw/covid/api"

# Archivo temporal local
LOCAL_FILE = "/tmp/covid_api.csv"


def download_csv_from_api(url: str, local_path: str):

    print(f"[INFO] Descargando datos desde API: {url}")
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] Falló la descarga desde la API: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(local_path, "wb") as f:
            f.write(resp.content)
    except OSError as e:
        print(f"[ERROR] No se pudo escribir el archivo local {local_path}: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"[OK] CSV descargado y guardado en {local_path}")


def upload_to_s3(local_path: str, bucket: str, key: str):
    s3 = boto3.client("s3")

    print(f"[INFO] Subiendo archivo a S3: s3://{bucket}/{key}")
    try:
        s3.upload_file(local_path, bucket, key)
    except (BotoCoreError, NoCredentialsError, ClientError) as e:
        print(f"[ERROR] Error subiendo a S3: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"[OK] Archivo subido a s3://{bucket}/{key}")


def main():

    s3_key = f"raw/api/covid_api.csv"

    download_csv_from_api(API_URL, LOCAL_FILE)

    upload_to_s3(LOCAL_FILE, BUCKET_NAME, s3_key)

    print("[DONE] Proceso de ingesta por API completado exitosamente.")


if __name__ == "__main__":
    main()
