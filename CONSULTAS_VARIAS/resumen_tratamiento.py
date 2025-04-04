from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Iniciar la sesión de Spark
spark = SparkSession.builder.appName("Tratamiento de Datos").getOrCreate()

# Crear un DataFrame de ejemplo
data = [
    ("2023-01-01", "Producto A", 10.0, 5, None),
    ("2023-01-02", "Producto B", 20.0, None, "Tienda 1"),
    ("2023-01-03", "Producto A", None, 8, "Tienda 2"),
    ("2023-02-01", "Producto C", 15.0, None, "Tienda 1"),
    ("2023-02-02", "Producto B", 20.0, 6, "Tienda 2"),
    ("2023-03-01", "Producto A", 10.0, 3, "Tienda 1"),
    ("2023-03-03", "Producto C", 12.0, 7, None),
    ("2023-03-05", "Producto B", 18.0, 2, "Tienda 2"),
    ("2023-04-01", "Producto A", None, 5, "Tienda 1"),
    ("2023-04-01", "Producto B", 19.0, 4, "Tienda 2")
]

columns = ["date", "producto", "precio_unitario", "cantidad", "tienda"]

df = spark.createDataFrame(data, columns)

# Convertir la columna `date` en tipo timestamp
df = df.withColumn("date", to_timestamp("date"))

df.show()
# Calcular la media de las columnas numéricas
media_precio = df.select(mean("precio_unitario")).collect()[0][0]
media_cantidad = df.select(mean("cantidad")).collect()[0][0]

# Rellenar los valores nulos con la media
df = df.fillna({"precio_unitario": media_precio, "cantidad": media_cantidad})
# Calcular la moda de la columna 'tienda'
tienda_moda = df.groupBy("tienda").count().orderBy(desc("count")).first()[0]

# Rellenar los valores nulos en la columna 'tienda' con la moda
df = df.fillna({"tienda": tienda_moda})
# Extraer el mes y el año de la columna `date`
df = df.withColumn("mes", date_format("date", "yyyy-MM"))

df.show()

'4.1 Filtrar por productos que hayan tenido ventas mayores a una cantidad específica
'
'Por ejemplo, queremos encontrar todos los productos que hayan tenido más de 5 unidades vendidas.
'
df.filter(col("cantidad") > 5).show()

'4.2 Agrupar por producto y calcular el total de ventas por producto
'
'Agrupamos por producto y calculamos la suma de las ventas totales (es decir, precio_unitario * cantidad).
'
df.withColumn("total_venta", col("precio_unitario") * col("cantidad")) \
  .groupBy("producto") \
  .agg(
      sum("total_venta").alias("total_venta")
  ).show()

'4.3 Agrupar por mes y calcular el total de ventas y la cantidad vendida'

'Agrupamos por mes y calculamos tanto la suma de total_venta como la suma de cantidad.'

df.withColumn("total_venta", col("precio_unitario") * col("cantidad")) \
  .groupBy("mes") \
  .agg(
      sum("total_venta").alias("total_venta_mes"),
      sum("cantidad").alias("cantidad_mes")
  ).orderBy("mes").show()

'''4.4 Filtrar productos con ventas totales mayores a un umbral específico (ejemplo: 100)'''

df.withColumn("total_venta", col("precio_unitario") * col("cantidad")) \
  .groupBy("producto") \
  .agg(sum("total_venta").alias("total_venta")) \
  .filter(col("total_venta") > 100) \
  .show()

4.5 Contar el número de registros por tienda

df.groupBy("tienda").count().show()

from pyspark.sql.functions import date_format, sum, col

# Agrupar por mes (sin agregar columna "mes") y realizar algunas agregaciones
df_agrupado = df.withColumn("mes", date_format("date", "yyyy-MM")) \
    .groupBy("mes", "producto") \
    .agg(
        sum("precio_unitario").alias("total_precio_unitario"),
        sum("cantidad").alias("total_cantidad"),
        sum("precio_unitario" * "cantidad").alias("total_venta")
    ) \
    .orderBy("mes")  # Ordenar por mes

df_agrupado.show()

# Crear una nueva columna llamada 'ganancias'
df = df.withColumn("ganancias", col("precio_unitario") * col("cantidad"))

# Ver el DataFrame con la nueva columna
df.show()