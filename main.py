from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date

# Crear una sesión de Spark
spark = SparkSession.builder.appName("VentasPorDia").getOrCreate()

# Leer el archivo CSV
df = spark.read.csv("ticket.csv", header=True, inferSchema=True)

# Convertir la columna created_at a formato de fecha
df = df.withColumn("created_at", to_date(col("created_at"), "yyyy-MM-dd"))

# Agrupar por día y sumar los totales
ventas_por_dia = df.groupBy("created_at").agg(_sum("total").alias("total_ventas"))

# Ordenar por total de ventas en orden descendente y seleccionar el primer registro
dia_max_ventas = ventas_por_dia.orderBy(col("total_ventas").desc()).first()

print(f"El día con más ventas es: {dia_max_ventas['created_at']} con un total de ventas de: {dia_max_ventas['total_ventas']}")