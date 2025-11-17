from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("COVID_ETL") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# CONFIGURACIÓN
BUCKET = "st0263-2024-2"

print("="*80)
print("INICIANDO PROCESO ETL - COVID COLOMBIA")
print("="*80)

# ===========================================
# PASO 1: LEER DATOS DE COVID API
# ===========================================

print("\n[1/4] Leyendo datos de COVID de la API del Ministerio...")
try:
    df_covid = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(f"s3://{BUCKET}/raw/api/covid_api.csv")
    
    print(f"   ✓ Registros leídos: {df_covid.count()}")
    print(f"   ✓ Columnas: {len(df_covid.columns)}")
    
    # Mostrar muestra
    print("\n   Muestra de datos COVID:")
    df_covid.show(3, truncate=True)
    
except Exception as e:
    print(f"   ✗ ERROR al leer COVID: {e}")
    sys.exit(1)

# ===========================================
# PASO 2: LEER DATOS DE RDS (CAPACIDAD HOSPITALARIA)
# ===========================================

print("\n[2/4] Leyendo datos de capacidad hospitalaria (RDS)...")
try:
    df_rds = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(f"s3://{BUCKET}/raw/rds/capacidad_atencion_por_municipio.csv")
    
    print(f"   ✓ Registros leídos: {df_rds.count()}")
    print(f"   ✓ Columnas: {df_rds.columns}")
    
    # Mostrar muestra
    print("\n   Muestra de datos RDS:")
    df_rds.show(3, truncate=False)
    
except Exception as e:
    print(f"   ✗ ERROR al leer RDS: {e}")
    sys.exit(1)

# ===========================================
# PASO 3: LIMPIEZA Y PREPARACIÓN DE DATOS
# ===========================================

print("\n[3/4] Limpiando y preparando datos...")

# ============ LIMPIEZA COVID ============
print("   → Procesando datos COVID...")

df_covid_clean = df_covid \
    .dropna(subset=["fecha_reporte_web", "departamento_nom"]) \
    .withColumn("fecha", to_date(col("fecha_reporte_web"), "yyyy-MM-dd")) \
    .withColumn("fecha_notificacion", to_date(col("fecha_de_notificaci_n"), "yyyy-MM-dd")) \
    .withColumn("fecha_sintomas", to_date(col("fecha_inicio_sintomas"), "yyyy-MM-dd")) \
    .withColumn("fecha_diagnostico", to_date(col("fecha_diagnostico"), "yyyy-MM-dd")) \
    .withColumn("fecha_muerte", to_date(col("fecha_muerte"), "yyyy-MM-dd")) \
    .withColumn("fecha_recuperado", to_date(col("fecha_recuperado"), "yyyy-MM-dd"))

# Convertir edad a entero y filtrar valores válidos
df_covid_clean = df_covid_clean \
    .withColumn("edad", col("edad").cast("integer")) \
    .filter(col("edad").isNotNull()) \
    .filter((col("edad") >= 0) & (col("edad") <= 120))

# Normalizar nombres de municipios (quitar espacios extra, mayúsculas)
df_covid_clean = df_covid_clean \
    .withColumn("municipio_normalizado", 
                upper(trim(col("ciudad_municipio_nom"))))

# Normalizar departamentos
df_covid_clean = df_covid_clean \
    .withColumn("departamento_normalizado", 
                upper(trim(col("departamento_nom"))))

# Limpiar estado (recuperado/fallecido)
df_covid_clean = df_covid_clean \
    .withColumn("estado_clean", upper(trim(col("estado")))) \
    .withColumn("recuperado_clean", upper(trim(col("recuperado")))) \
    .withColumn("sexo_clean", upper(trim(col("sexo"))))

print(f"   ✓ Datos COVID limpios: {df_covid_clean.count()} registros")

# ============ LIMPIEZA RDS ============
print("   → Procesando datos de capacidad hospitalaria...")

df_rds_clean = df_rds \
    .dropna() \
    .withColumn("municipio_normalizado", upper(trim(col("municipio")))) \
    .withColumn("camas_disponibles", col("camas").cast("integer")) \
    .withColumn("camas_uci_disponibles", col("camas_uci").cast("integer"))

print(f"   ✓ Datos RDS limpios: {df_rds_clean.count()} registros")

# ===========================================
# PASO 4: UNIÓN DE DATOS (JOIN POR MUNICIPIO)
# ===========================================

print("\n[4/4] Uniendo datos COVID con capacidad hospitalaria por municipio...")

# Realizar JOIN por municipio
df_joined = df_covid_clean.join(
    df_rds_clean.select(
        col("municipio_normalizado"),
        col("camas_disponibles"),
        col("camas_uci_disponibles"),
        col("id_municipio").alias("id_municipio_rds")
    ),
    on="municipio_normalizado",
    how="left"
)

print(f"   ✓ Registros después del JOIN: {df_joined.count()}")

# Calcular estadística: cuántos registros tienen info hospitalaria
registros_con_hospitales = df_joined.filter(col("camas_disponibles").isNotNull()).count()
porcentaje = round((registros_con_hospitales / df_joined.count()) * 100, 2)
print(f"   ✓ Registros con datos hospitalarios: {registros_con_hospitales} ({porcentaje}%)")

# Seleccionar y renombrar columnas finales
df_final = df_joined.select(
    # Información temporal
    col("fecha").alias("fecha_reporte"),
    col("fecha_notificacion"),
    col("fecha_sintomas"),
    col("fecha_diagnostico"),
    col("fecha_muerte"),
    col("fecha_recuperado"),
    
    # Identificadores
    col("id_de_caso").alias("id_caso"),
    col("departamento").alias("cod_departamento"),
    col("departamento_nom").alias("departamento"),
    col("ciudad_municipio").alias("cod_municipio"),
    col("ciudad_municipio_nom").alias("municipio"),
    
    # Datos del paciente
    col("edad"),
    col("unidad_medida").alias("unidad_edad"),
    col("sexo_clean").alias("sexo"),
    col("per_etn_").alias("pertenencia_etnica"),
    col("nom_grupo_").alias("nombre_grupo_etnico"),
    
    # Estado y evolución
    col("estado_clean").alias("estado"),
    col("recuperado_clean").alias("recuperado"),
    col("tipo_recuperacion"),
    col("ubicacion"),
    
    # Contexto
    col("fuente_tipo_contagio"),
    col("pais_viajo_1_cod").alias("pais_viaje_cod"),
    col("pais_viajo_1_nom").alias("pais_viaje"),
    
    # Datos hospitalarios (del JOIN)
    col("camas_disponibles"),
    col("camas_uci_disponibles"),
    col("id_municipio_rds")
)

# ===========================================
# PASO 5: GUARDAR EN ZONA TRUSTED
# ===========================================

print("\n[GUARDANDO] Escribiendo datos en zona TRUSTED...")

output_path = f"s3://{BUCKET}/trusted/covid_procesado/"

try:
    # Particionar por fecha para optimizar consultas
    df_final.write \
        .mode("overwrite") \
        .partitionBy("fecha_reporte") \
        .parquet(output_path)
    
    print(f"   ✓✓✓ Datos guardados exitosamente en: {output_path}")
    
except Exception as e:
    print(f"   ✗ ERROR al guardar: {e}")
    sys.exit(1)

# ===========================================
# ESTADÍSTICAS FINALES
# ===========================================

print("\n" + "="*80)
print("ESTADÍSTICAS DEL ETL:")
print("="*80)

stats = df_final.agg(
    count("*").alias("total_registros"),
    countDistinct("departamento").alias("departamentos_unicos"),
    countDistinct("municipio").alias("municipios_unicos"),
    min("fecha_reporte").alias("fecha_inicio"),
    max("fecha_reporte").alias("fecha_fin"),
    sum(when(col("estado") == "FALLECIDO", 1).otherwise(0)).alias("total_fallecidos"),
    sum(when(col("recuperado") == "RECUPERADO", 1).otherwise(0)).alias("total_recuperados"),
    round(avg("edad"), 1).alias("edad_promedio"),
    sum(when(col("sexo") == "F", 1).otherwise(0)).alias("casos_femenino"),
    sum(when(col("sexo") == "M", 1).otherwise(0)).alias("casos_masculino"),
    countDistinct(when(col("camas_disponibles").isNotNull(), col("municipio"))).alias("municipios_con_datos_hospitalarios")
).collect()[0]

print(f"\nTotal de registros procesados:     {stats['total_registros']:,}")
print(f"Departamentos únicos:               {stats['departamentos_unicos']}")
print(f"Municipios únicos:                  {stats['municipios_unicos']}")
print(f"Municipios con datos hospitalarios: {stats['municipios_con_datos_hospitalarios']}")
print(f"Período:                            {stats['fecha_inicio']} a {stats['fecha_fin']}")
print(f"Total fallecidos:                   {stats['total_fallecidos']:,}")
print(f"Total recuperados:                  {stats['total_recuperados']:,}")
print(f"Edad promedio:                      {stats['edad_promedio']} años")
print(f"Casos femenino:                     {stats['casos_femenino']:,}")
print(f"Casos masculino:                    {stats['casos_masculino']:,}")

# Mostrar muestra final
print("\n" + "="*80)
print("MUESTRA DE DATOS PROCESADOS (5 registros):")
print("="*80)
df_final.show(5, truncate=False)

print("\n" + "="*80)
print("✓✓✓ ETL COMPLETADO EXITOSAMENTE ✓✓✓")
print("="*80)

spark.stop()