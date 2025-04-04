from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("VentasEjemplo").getOrCreate()

data = [
    ("2023-01-01", "Producto A", "Tienda 1", 10.0, 2),
    ("2023-01-02", "Producto B", "Tienda 1", 20.0, 1),
    ("2023-01-03", "Producto A", "Tienda 2", 10.0, 5),
    ("2023-02-01", "Producto C", "Tienda 1", 15.0, 3),
    ("2023-02-05", "Producto B", "Tienda 2", 20.0, 2),
    ("2023-03-10", "Producto A", "Tienda 1", 10.0, 4),
    ("2024-01-01", "Producto A", "Tienda 2", 10.0, 6),
    ("2024-01-15", "Producto C", "Tienda 1", 15.0, 1),
]

columns = ["fecha", "producto", "tienda", "precio_unitario", "cantidad"]
df = spark.createDataFrame(data, columns).withColumn("fecha", to_date("fecha"))
df = df.withColumn("total_venta", col("precio_unitario") * col("cantidad"))
df.show()
#Agrupar por producto
df.groupBy("producto").agg(sum("total_venta").alias("ventas_totales")).show()
#Agrupar por tienda y mes
df.withColumn("mes", month("fecha")) \
  .groupBy("tienda", "mes") \
  .agg(sum("total_venta").alias("ventas_mes")) \
  .orderBy("tienda", "mes") \
  .show()
#Agrupar por año y producto
df.withColumn("año", year("fecha")) \
  .groupBy("año", "producto") \
  .agg(count("*").alias("num_ventas"), sum("cantidad").alias("unidades")) \
  .show()
#Agrupacion por multiples columnas
df.groupBy("producto", "tienda") \
  .agg(
      sum("total_venta").alias("total"),
      avg("cantidad").alias("prom_cantidad"),
      max("cantidad").alias("max_cantidad")
  ).show()

