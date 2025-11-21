from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ================= CONFIGURACIÓN =========================
BUCKET = "nruizudatalake"
TRUSTED_PATH = f"s3://{BUCKET}/trusted/covid_trusted"
REFINED_BASE = f"s3://{BUCKET}/refined"

# ================= CREAR SESIÓN SPARK ====================
spark = SparkSession.builder \
    .appName("COVID_Analytics") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("=" * 80)
print("ANÁLISIS AVANZADO DE DATOS COVID COLOMBIA")
print("=" * 80)
print(f"TRUSTED INPUT: {TRUSTED_PATH}")
print(f"REFINED BASE:  {REFINED_BASE}")

# ================= LEER TRUSTED ==========================

df = spark.read.parquet(TRUSTED_PATH)

print(f"\nTotal registros TRUSTED: {df.count()}")

# Definir una fecha “principal” para análisis (diagnóstico o reporte)
df = df.withColumn(
    "fecha",
    F.coalesce(F.col("fecha_diagnostico"), F.col("fecha_reporte"))
)

df = df.filter(F.col("fecha").isNotNull())
df.cache()

print(f"Registros con fecha válida: {df.count()}")

# ================= 1. Casos por departamento ============

print("\n[1] Generando casos_por_departamento...")

df_depto = (
    df.groupBy("departamento_nom")
    .agg(
        F.count("*").alias("casos_totales"),
        F.sum(F.when(F.col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos"),
        F.sum(F.when(F.col("recuperado") == "RECUPERADO", 1).otherwise(0)).alias("recuperados")
    )
    .withColumn(
        "tasa_mortalidad",
        F.round(F.col("fallecidos") / F.col("casos_totales") * 100, 2)
    )
    .orderBy(F.col("casos_totales").desc())
)

df_depto.write.mode("overwrite").format("parquet") \
    .save(f"{REFINED_BASE}/casos_por_departamento")

# ================= 2. Tendencia temporal =================

print("\n[2] Generando tendencia_temporal...")

df_daily = (
    df.groupBy("fecha")
    .agg(F.count("*").alias("casos_diarios"))
    .orderBy("fecha")
)

w = Window.orderBy("fecha").rowsBetween(-6, 0)  # promedio móvil de 7 días

df_daily = df_daily.withColumn(
    "promedio_movil_7d",
    F.round(F.avg("casos_diarios").over(w), 2)
)

df_daily.write.mode("overwrite").format("parquet") \
    .save(f"{REFINED_BASE}/tendencia_temporal")

# ================= 3. Distribución edad/sexo =============

print("\n[3] Generando distribucion_edad_sexo...")

df_dist_edad_sexo = (
    df.groupBy("sexo")
    .agg(
        F.count("*").alias("casos_totales"),
        F.avg("edad").alias("edad_promedio")
    )
)

df_dist_edad_sexo.write.mode("overwrite").format("parquet") \
    .save(f"{REFINED_BASE}/distribucion_edad_sexo")

# ================= 4. Top municipios =====================

print("\n[4] Generando top_municipios...")

df_top_mun = (
    df.groupBy("municipio_nom")
    .agg(
        F.count("*").alias("casos_totales"),
        F.sum(F.when(F.col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos"),
        F.max("camas").alias("camas"),
        F.max("camas_uci").alias("camas_uci")
    )
    .withColumn(
        "casos_por_100_camas",
        F.when(F.col("camas") > 0,
               F.round(F.col("casos_totales") / F.col("camas") * 100, 2)
        ).otherwise(None)
    )
    .orderBy(F.col("casos_totales").desc())
    .limit(20)
)

df_top_mun.write.mode("overwrite").format("parquet") \
    .save(f"{REFINED_BASE}/top_municipios")

# ================= 5. Capacidad hospitalaria =============

print("\n[5] Generando capacidad_hospitalaria...")

df_capacidad = (
    df.groupBy("municipio_nom")
    .agg(
        F.max("camas").alias("camas"),
        F.max("camas_uci").alias("camas_uci"),
        F.count("*").alias("casos_totales")
    )
    .withColumn(
        "casos_por_100_camas",
        F.when(F.col("camas") > 0,
               F.round(F.col("casos_totales") / F.col("camas") * 100, 2)
        ).otherwise(None)
    )
)

df_capacidad.write.mode("overwrite").format("parquet") \
    .save(f"{REFINED_BASE}/capacidad_hospitalaria")

# ================= 6. Evolución mensual ==================

print("\n[6] Generando evolucion_mensual...")

df_mensual = (
    df.withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
    .groupBy("anio_mes", "departamento_nom")
    .agg(
        F.count("*").alias("casos_totales"),
        F.sum(F.when(F.col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos"),
        F.sum(F.when(F.col("recuperado") == "RECUPERADO", 1).otherwise(0)).alias("recuperados")
    )
    .orderBy("anio_mes", "departamento_nom")
)

df_mensual.write.mode("overwrite").format("parquet") \
    .save(f"{REFINED_BASE}/evolucion_mensual")

# ================= 7. Análisis de recuperación ===========

print("\n[7] Generando analisis_recuperacion...")

df_recuperacion = df.filter(
    (F.col("fecha_diagnostico").isNotNull()) &
    (F.col("fecha_recuperado").isNotNull())
).withColumn(
    "dias_recuperacion",
    F.datediff(F.col("fecha_recuperado"), F.col("fecha_diagnostico"))
)

df_rec_stats = (
    df_recuperacion.agg(
        F.count("*").alias("n_registros"),
        F.avg("dias_recuperacion").alias("promedio_dias"),
        F.expr("percentile_approx(dias_recuperacion, 0.5)").alias("mediana_dias")
    )
)

df_rec_stats.write.mode("overwrite").format("parquet") \
    .save(f"{REFINED_BASE}/analisis_recuperacion")

# ================= 8. Resumen ejecutivo ==================

print("\n[8] Generando resumen_ejecutivo...")

total_casos = df.count()
total_fallecidos = df.filter(F.col("estado") == "FALLECIDO").count()
total_recuperados = df.filter(F.col("recuperado") == "RECUPERADO").count()

df_resumen = spark.createDataFrame(
    [
        (
            total_casos,
            total_fallecidos,
            total_recuperados,
            round(total_fallecidos / total_casos * 100, 2) if total_casos > 0 else None
        )
    ],
    ["total_casos", "total_fallecidos", "total_recuperados", "tasa_mortalidad"]
)

df_resumen.write.mode("overwrite").format("parquet") \
    .save(f"{REFINED_BASE}/resumen_ejecutivo")

print("\n" + "=" * 80)
print("ARCHIVOS GENERADOS EN LA ZONA REFINED:")
print("=" * 80)
print("1. casos_por_departamento/")
print("2. tendencia_temporal/")
print("3. distribucion_edad_sexo/")
print("4. top_municipios/")
print("5. capacidad_hospitalaria/")
print("6. evolucion_mensual/")
print("7. analisis_recuperacion/")
print("8. resumen_ejecutivo/")

print("\n" + "=" * 80)
print("✓✓✓ ANÁLISIS COMPLETADO EXITOSAMENTE ✓✓✓")
print("=" * 80)

spark.stop()
