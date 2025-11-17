import pymysql
import csv
import boto3


RDS_HOST = "database-1.cnemggoqsr5h.us-east-1.rds.amazonaws.com"
RDS_USER = "admin"
RDS_PASSWORD = "Nico1973*"
RDS_DB = "capacidad_covid_db"

BUCKET = "nruizudatalake"
S3_KEY = "raw/rds/capacidad_atencion_por_municipio.csv"


conn = pymysql.connect(
    host=RDS_HOST,
    user=RDS_USER,
    password=RDS_PASSWORD,
    db=RDS_DB
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM capacidad_atencion_por_municipio")
rows = cursor.fetchall()
columns = [col[0] for col in cursor.description]


local_file = "/tmp/capacidad_atencion_por_municipio.csv"

with open(local_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columns)
    writer.writerows(rows)

s3 = boto3.client("s3")
s3.upload_file(local_file, BUCKET, S3_KEY)

print("Archivo exportado a S3:", f"s3://{BUCKET}/{S3_KEY}")
