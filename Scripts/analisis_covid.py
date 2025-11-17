from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("COVID_Analytics") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# CONFIGURACIÓN
BUCKET = "st0263-2024-2"

print("="*80)
print("ANÁLISIS AVANZADO DE DATOS COVID COLOMBIA")
print("="*80)

# ===========================================
# LEER DATOS PROCESADOS
# ===========================================

print("\n[Cargando datos] Leyendo datos de TRUSTED...")
try:
    df = spark.read.parquet(f"s3://{BUCKET}/trusted/covid_procesado/")
    total_registros = df.count()
    print(f"   ✓ Registros cargados: {total_registros:,}")
    
    if total_registros == 0:
        print("   ✗ ERROR: No hay datos para analizar")
        sys.exit(1)
    
except Exception as e:
    print(f"   ✗ ERROR: {e}")
    sys.exit(1)

# Crear vista SQL
df.createOrReplaceTempView("covid")

# ===========================================
# ANÁLISIS 1: CASOS POR DEPARTAMENTO
# ===========================================

print("\n" + "="*80)
print("[ANÁLISIS 1] CASOS POR DEPARTAMENTO")
print("="*80)

casos_dept = df.groupBy("departamento") \
    .agg(
        count("*").alias("total_casos"),
        countDistinct("municipio").alias("municipios_afectados"),
        round(avg("edad"), 1).alias("edad_promedio"),
        sum(when(col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos"),
        sum(when(col("recuperado") == "RECUPERADO", 1).otherwise(0)).alias("recuperados"),
        sum(when(col("sexo") == "F", 1).otherwise(0)).alias("casos_mujeres"),
        sum(when(col("sexo") == "M", 1).otherwise(0)).alias("casos_hombres"),
        sum(when(col("camas_disponibles").isNotNull(), 1).otherwise(0)).alias("casos_con_info_hospitalaria")
    ) \
    .withColumn("tasa_letalidad", 
                round((col("fallecidos") / col("total_casos")) * 100, 2)) \
    .withColumn("tasa_recuperacion",
                round((col("recuperados") / col("total_casos")) * 100, 2)) \
    .orderBy(desc("total_casos"))

print("\nTOP 15 DEPARTAMENTOS MÁS AFECTADOS:")
casos_dept.show(15, truncate=False)

# Guardar
output_1 = f"s3://{BUCKET}/refined/casos_por_departamento/"
casos_dept.write.mode("overwrite").parquet(output_1)
print(f"✓ Guardado en: {output_1}")

# ===========================================
# ANÁLISIS 2: TENDENCIA TEMPORAL DIARIA
# ===========================================

print("\n" + "="*80)
print("[ANÁLISIS 2] TENDENCIA TEMPORAL DIARIA")
print("="*80)

tendencia_diaria = df.groupBy("fecha_reporte") \
    .agg(
        count("*").alias("casos_diarios"),
        round(avg("edad"), 1).alias("edad_promedio"),
        sum(when(col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos_diarios"),
        sum(when(col("recuperado") == "RECUPERADO", 1).otherwise(0)).alias("recuperados_diarios")
    ) \
    .orderBy("fecha_reporte")

# Calcular promedio móvil de 7 días
window_7d = Window.orderBy("fecha_reporte").rowsBetween(-6, 0)
tendencia_diaria = tendencia_diaria \
    .withColumn("casos_promedio_7dias", 
                round(avg("casos_diarios").over(window_7d), 2)) \
    .withColumn("fallecidos_promedio_7dias",
                round(avg("fallecidos_diarios").over(window_7d), 2))

print("\nÚLTIMOS 14 DÍAS:")
tendencia_diaria.orderBy(desc("fecha_reporte")).show(14, truncate=False)

# Guardar
output_2 = f"s3://{BUCKET}/refined/tendencia_temporal/"
tendencia_diaria.write.mode("overwrite").parquet(output_2)
print(f"✓ Guardado en: {output_2}")

# ===========================================
# ANÁLISIS 3: DISTRIBUCIÓN POR EDAD Y SEXO
# ===========================================

print("\n" + "="*80)
print("[ANÁLISIS 3] DISTRIBUCIÓN POR EDAD Y SEXO")
print("="*80)

df_edad = df.withColumn(
    "rango_edad",
    when(col("edad") < 10, "00-09")
    .when((col("edad") >= 10) & (col("edad") < 20), "10-19")
    .when((col("edad") >= 20) & (col("edad") < 30), "20-29")
    .when((col("edad") >= 30) & (col("edad") < 40), "30-39")
    .when((col("edad") >= 40) & (col("edad") < 50), "40-49")
    .when((col("edad") >= 50) & (col("edad") < 60), "50-59")
    .when((col("edad") >= 60) & (col("edad") < 70), "60-69")
    .when((col("edad") >= 70) & (col("edad") < 80), "70-79")
    .otherwise("80+")
)

distribucion = df_edad.groupBy("rango_edad", "sexo") \
    .agg(
        count("*").alias("casos"),
        sum(when(col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos"),
        round(avg("edad"), 1).alias("edad_promedio_grupo")
    ) \
    .withColumn("tasa_letalidad",
                round((col("fallecidos") / col("casos")) * 100, 2)) \
    .orderBy("rango_edad", "sexo")

print("\nDISTRIBUCIÓN COMPLETA:")
distribucion.show(30, truncate=False)

# Guardar
output_3 = f"s3://{BUCKET}/refined/distribucion_edad_sexo/"
distribucion.write.mode("overwrite").parquet(output_3)
print(f"✓ Guardado en: {output_3}")

# ===========================================
# ANÁLISIS 4: TOP MUNICIPIOS MÁS AFECTADOS
# ===========================================

print("\n" + "="*80)
print("[ANÁLISIS 4] MUNICIPIOS MÁS AFECTADOS")
print("="*80)

municipios = df.groupBy("departamento", "municipio") \
    .agg(
        count("*").alias("total_casos"),
        sum(when(col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos"),
        sum(when(col("recuperado") == "RECUPERADO", 1).otherwise(0)).alias("recuperados"),
        max("camas_disponibles").alias("camas_hospital"),
        max("camas_uci_disponibles").alias("camas_uci")
    ) \
    .withColumn("tasa_letalidad",
                round((col("fallecidos") / col("total_casos")) * 100, 2)) \
    .orderBy(desc("total_casos"))

print("\nTOP 20 MUNICIPIOS:")
municipios.show(20, truncate=False)

# Guardar
output_4 = f"s3://{BUCKET}/refined/top_municipios/"
municipios.write.mode("overwrite").parquet(output_4)
print(f"✓ Guardado en: {output_4}")

# ===========================================
# ANÁLISIS 5: CAPACIDAD HOSPITALARIA vs CASOS
# ===========================================

print("\n" + "="*80)
print("[ANÁLISIS 5] CAPACIDAD HOSPITALARIA vs CASOS")
print("="*80)

# Filtrar solo municipios con datos hospitalarios
hospitales = df.filter(col("camas_disponibles").isNotNull()) \
    .groupBy("municipio", "camas_disponibles", "camas_uci_disponibles") \
    .agg(
        count("*").alias("total_casos"),
        sum(when(col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos")
    ) \
    .withColumn("ratio_casos_camas",
                round(col("total_casos") / col("camas_disponibles"), 2)) \
    .withColumn("ratio_fallecidos_uci",
                round(col("fallecidos") / col("camas_uci_disponibles"), 2)) \
    .orderBy(desc("ratio_casos_camas"))

print("\nMUNICIPIOS CON MAYOR PRESIÓN HOSPITALARIA (casos/camas):")
hospitales.show(15, truncate=False)

# Guardar
output_5 = f"s3://{BUCKET}/refined/capacidad_hospitalaria/"
hospitales.write.mode("overwrite").parquet(output_5)
print(f"✓ Guardado en: {output_5}")

# ===========================================
# ANÁLISIS 6: EVOLUCIÓN MENSUAL
# ===========================================

print("\n" + "="*80)
print("[ANÁLISIS 6] EVOLUCIÓN MENSUAL")
print("="*80)

evolucion_mensual = spark.sql("""
    SELECT 
        year(fecha_reporte) as anio,
        month(fecha_reporte) as mes,
        COUNT(*) as casos_mensuales,
        SUM(CASE WHEN estado = 'FALLECIDO' THEN 1 ELSE 0 END) as fallecidos_mensuales,
        SUM(CASE WHEN recuperado = 'RECUPERADO' THEN 1 ELSE 0 END) as recuperados_mensuales,
        ROUND(AVG(edad), 1) as edad_promedio,
        COUNT(DISTINCT departamento) as departamentos_activos,
        COUNT(DISTINCT municipio) as municipios_activos
    FROM covid
    GROUP BY year(fecha_reporte), month(fecha_reporte)
    ORDER BY anio, mes
""")

print("\nEVOLUCIÓN COMPLETA:")
evolucion_mensual.show(50, truncate=False)

# Guardar
output_6 = f"s3://{BUCKET}/refined/evolucion_mensual/"
evolucion_mensual.write.mode("overwrite").parquet(output_6)
print(f"✓ Guardado en: {output_6}")

# ===========================================
# ANÁLISIS 7: ANÁLISIS DE RECUPERACIÓN
# ===========================================

print("\n" + "="*80)
print("[ANÁLISIS 7] ANÁLISIS DE RECUPERACIÓN")
print("="*80)

# Calcular tiempo de recuperación (solo para casos con fechas completas)
tiempo_recuperacion = df \
    .filter(col("fecha_recuperado").isNotNull()) \
    .filter(col("fecha_sintomas").isNotNull()) \
    .withColumn("dias_recuperacion", 
                datediff(col("fecha_recuperado"), col("fecha_sintomas")))

stats_recuperacion = tiempo_recuperacion.groupBy("departamento") \
    .agg(
        count("*").alias("casos_con_fecha_recuperacion"),
        round(avg("dias_recuperacion"), 1).alias("dias_promedio_recuperacion"),
        min("dias_recuperacion").alias("dias_minimo"),
        max("dias_recuperacion").alias("dias_maximo")
    ) \
    .filter(col("casos_con_fecha_recuperacion") > 10) \
    .orderBy("dias_promedio_recuperacion")

print("\nTIEMPO DE RECUPERACIÓN POR DEPARTAMENTO:")
stats_recuperacion.show(20, truncate=False)

# Guardar
output_7 = f"s3://{BUCKET}/refined/analisis_recuperacion/"
stats_recuperacion.write.mode("overwrite").parquet(output_7)
print(f"✓ Guardado en: {output_7}")

# ===========================================
# ANÁLISIS 8: CONSULTAS SQL AVANZADAS
# ===========================================

print("\n" + "="*80)
print("[ANÁLISIS 8] DEPARTAMENTOS CON MAYOR TASA DE LETALIDAD")
print("="*80)

letalidad = spark.sql("""
    SELECT 
        departamento,
        COUNT(*) as total_casos,
        SUM(CASE WHEN estado = 'FALLECIDO' THEN 1 ELSE 0 END) as fallecidos,
        ROUND(SUM(CASE WHEN estado = 'FALLECIDO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as tasa_letalidad,
        ROUND(AVG(edad), 1) as edad_promedio,
        COUNT(DISTINCT municipio) as municipios
    FROM covid
    GROUP BY departamento
    HAVING COUNT(*) > 100
    ORDER BY tasa_letalidad DESC
    LIMIT 15
""")

print("\nDEPARTAMENTOS CON MAYOR LETALIDAD:")
letalidad.show(15, truncate=False)

# ===========================================
# RESUMEN FINAL EJECUTIVO
# ===========================================

print("\n" + "="*80)
print("RESUMEN EJECUTIVO DEL ANÁLISIS")
print("="*80)

resumen_ejecutivo = spark.sql("""
    SELECT 
        COUNT(*) as total_casos,
        COUNT(DISTINCT departamento) as departamentos,
        COUNT(DISTINCT municipio) as municipios,
        SUM(CASE WHEN estado = 'FALLECIDO' THEN 1 ELSE 0 END) as total_fallecidos,
        SUM(CASE WHEN recuperado = 'RECUPERADO' THEN 1 ELSE 0 END) as total_recuperados,
        ROUND(SUM(CASE WHEN estado = 'FALLECIDO' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as tasa_letalidad_nacional,
        ROUND(AVG(edad), 1) as edad_promedio,
        SUM(CASE WHEN sexo = 'F' THEN 1 ELSE 0 END) as casos_femenino,
        SUM(CASE WHEN sexo = 'M' THEN 1 ELSE 0 END) as casos_masculino,
        MIN(fecha_reporte) as fecha_inicio,
        MAX(fecha_reporte) as fecha_fin,
        COUNT(DISTINCT CASE WHEN camas_disponibles IS NOT NULL THEN municipio END) as municipios_con_datos_hospitalarios
    FROM covid
""")

print("\n")
resumen_ejecutivo.show(truncate=False, vertical=True)

# Guardar resumen
output_resumen = f"s3://{BUCKET}/refined/resumen_ejecutivo/"
resumen_ejecutivo.write.mode("overwrite").parquet(output_resumen)
print(f"\n✓ Resumen guardado en: {output_resumen}")

# ===========================================
# LISTADO DE TODOS LOS ANÁLISIS GENERADOS
# ===========================================

print("\n" + "="*80)
print("ARCHIVOS GENERADOS EN /refined/")
print("="*80)
print("1. casos_por_departamento/      - Estadísticas por departamento")
print("2. tendencia_temporal/          - Serie temporal diaria con promedios móviles")
print("3. distribucion_edad_sexo/      - Distribución demográfica")
print("4. top_municipios/              - Municipios más afectados")
print("5. capacidad_hospitalaria/      - Análisis de presión hospitalaria")
print("6. evolucion_mensual/           - Evolución agregada mensual")
print("7. analisis_recuperacion/       - Tiempos de recuperación")
print("8. resumen_ejecutivo/           - Resumen general ejecutivo")

print("\n" + "="*80)
print("✓✓✓ ANÁLISIS COMPLETADO EXITOSAMENTE ✓✓✓")
print("="*80)

spark.stop()