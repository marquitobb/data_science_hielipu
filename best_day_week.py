from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date, date_format

# Crear una sesión de Spark
spark = SparkSession.builder.appName("VentasPorDia").getOrCreate()

# Leer el archivo CSV
df = spark.read.csv("ticket.csv", header=True, inferSchema=True)

# Convertir la columna created_at a formato de fecha
df = df.withColumn("created_at", to_date(col("created_at"), "yyyy-MM-dd"))

# Extraer el día de la semana de la columna created_at
df = df.withColumn("dia_semana", date_format(col("created_at"), "EEEE"))

# Agrupar por día de la semana y sumar los totales
ventas_por_dia_semana = df.groupBy("dia_semana").agg(_sum("total").alias("total_ventas"))

# Ordenar por total de ventas en orden descendente
ventas_ordenadas = ventas_por_dia_semana.orderBy(col("total_ventas").desc()).collect()

# Imprimir los días de la semana ordenados por ventas
print("Lista de los mejores días de la semana en ventas:")
for i, row in enumerate(ventas_ordenadas, start=1):
    # print(f"{i}. {row['dia_semana']} con un total de ventas de: {row['total_ventas']}")
    print(f"{i}. {row.dia_semana}")