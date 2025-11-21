import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    dayofmonth,
)

# ============================================================
# RUTAS S3 REALES DE TU PROYECTO
# ============================================================

BUCKET = ("nruizudatalake")

# Rutas reales
COVID_INPUT_PATH = f"s3://{BUCKET}/raw/api/covid_api.csv"
RDS_INPUT_PATH = f"s3://{BUCKET}/raw/rds/poblacion.csv"
TRUSTED_OUTPUT_PATH = f"s3://{BUCKET}/trusted/covid/"


# ============================================================
# CREAR SPARK SESSION (compatible con local y EMR)
# ============================================================

def create_spark_session(app_name: str):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# MAIN ETL
# ============================================================

def main():

    spark = create_spark_session("covid_raw_to_trusted")

    # ---------------------------
    # Leer datos COVID API (raw)
    # ---------------------------
    print(f"[INFO] Leyendo casos COVID desde: {COVID_INPUT_PATH}")
    df_covid = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(COVID_INPUT_PATH)
    )

    print(f"[INFO] Filas COVID cargadas: {df_covid.count()}")

    # ---------------------------
    # Leer datos RDS (demografía)
    # ---------------------------
    print(f"[INFO] Leyendo población desde: {RDS_INPUT_PATH}")
    df_demog = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RDS_INPUT_PATH)
    )

    print(f"[INFO] Filas demográficas cargadas: {df_demog.count()}")

    # ---------------------------
    # JOIN usando DIVIPOLA
    # covid.departamento  <->  demog.codigo_departamento
    # ---------------------------
    print("[INFO] Realizando JOIN DIVIPOLA (departamento)")

    df_join = (
        df_covid.alias("covid")
        .join(
            df_demog.alias("demog"),
            col("covid.departamento").cast("string") ==
            col("demog.codigo_departamento").cast("string"),
            "left"
        )
    )

    print("[INFO] JOIN completado")

    # ---------------------------
    # Parse de fechas + particiones
    # ---------------------------
    print("[INFO] Procesando columnas de fecha...")

    df_join = (
        df_join.withColumn("fecha_reporte_web_date",
                           to_date(col("covid.fecha_reporte_web")))
        .withColumn("anio", year(col("fecha_reporte_web_date")))
        .withColumn("mes", month(col("fecha_reporte_web_date")))
        .withColumn("dia", dayofmonth(col("fecha_reporte_web_date")))
    )

    # ---------------------------
    # Selección columnas finales
    # ---------------------------
    print("[INFO] Seleccionando columnas finales...")

    df_trusted = df_join.select(
        # COVID
        col("covid.id_de_caso"),
        col("covid.fecha_reporte_web"),
        col("fecha_reporte_web_date"),
        col("covid.fecha_de_notificaci_n"),
        col("anio"),
        col("mes"),
        col("dia"),
        col("covid.departamento").alias("codigo_divipola_departamento"),
        col("covid.departamento_nom"),
        col("covid.ciudad_municipio").alias("codigo_divipola_municipio"),
        col("covid.ciudad_municipio_nom").alias("nombre_municipio"),
        col("covid.edad"),
        col("covid.unidad_medida"),
        col("covid.sexo"),
        col("covid.fuente_tipo_contagio"),
        col("covid.ubicacion"),
        col("covid.estado"),
        col("covid.pais_viajo_1_cod"),
        col("covid.pais_viajo_1_nom"),
        col("covid.recuperado"),
        col("covid.fecha_inicio_sintomas"),
        col("covid.fecha_muerte"),
        col("covid.fecha_diagnostico"),
        col("covid.fecha_recuperado"),
        col("covid.tipo_recuperacion"),
        col("covid.per_etn_"),
        col("covid.nom_grupo_"),

        # Demografía enriquecida
        col("demog.codigo_departamento"),
        col("demog.departamento").alias("departamento_nombre_normalizado"),
        col("demog.poblacion"),
    )

    print(f"[INFO] Columnas finales: {len(df_trusted.columns)}")

    # ---------------------------
    # Escritura en TRUSTED
    # ---------------------------
    print(f"[INFO] Escribiendo TRUSTED en: {TRUSTED_OUTPUT_PATH}")

    (
        df_trusted
        .write
        .mode("overwrite")
        .partitionBy("anio", "mes", "dia")
        .parquet(TRUSTED_OUTPUT_PATH)
    )

    print("[OK] ETL COMPLETADO exitosamente.")
    spark.stop()


# ============================================================
# EJECUCIÓN
# ============================================================

if __name__ == "__main__":
    main()
