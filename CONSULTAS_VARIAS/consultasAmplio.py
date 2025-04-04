from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("GroupByAvanzado").getOrCreate()

data = [
    ("2023-01-01", "Producto A", "Tienda 1", 10.0, 2),
    ("2023-01-02", "Producto B", "Tienda 1", 20.0, 1),
    ("2023-01-03", "Producto A", "Tienda 2", 10.0, 5),
    ("2023-02-01", "Producto C", "Tienda 1", 15.0, 3),
    ("2023-02-05", "Producto B", "Tienda 2", 20.0, 2),
    ("2023-03-10", "Producto A", "Tienda 1", 10.0, 4),
    ("2024-01-01", "Producto A", "Tienda 2", 10.0, 6),
    ("2024-01-15", "Producto C", "Tienda 1", 15.0, 1),
    ("2024-02-10", "Producto B", "Tienda 2", 20.0, 3),
    ("2024-02-15", "Producto A", "Tienda 2", 10.0, 2),
]

columns = ["fecha", "producto", "tienda", "precio_unitario", "cantidad"]
df = spark.createDataFrame(data, columns).withColumn("fecha", to_date("fecha"))
df = df.withColumn("total_venta", col("precio_unitario") * col("cantidad"))
🔹 1. GroupBy con múltiples funciones agregadas
df.groupBy("producto", "tienda") \
  .agg(
      count("*").alias("n_ventas"),
      sum("cantidad").alias("unidades_vendidas"),
      round(avg("total_venta"), 2).alias("promedio_venta"),
      max("total_venta").alias("venta_max"),
  ).orderBy("producto", "tienda").show()
🔹 2. GroupBy + withColumn para agregar columna temporal (año, mes)
df.withColumn("año", year("fecha")) \
  .groupBy("año", "producto") \
  .agg(sum("total_venta").alias("total_anual")) \
  .orderBy("año", "producto") \
  .show()
🔹 3. GroupBy + HAVING (ventas > X)
ventas_por_producto = df.groupBy("producto") \
    .agg(sum("total_venta").alias("ventas_totales"))

ventas_por_producto.filter(col("ventas_totales") > 100).show()

from pyspark.sql.window import Window
🔹 4. GroupBy dentro de subconsulta (top producto por tienda)
# Subconsulta con row_number()
windowSpec = Window.partitionBy("tienda").orderBy(col("total_venta").desc())

df_ranked = df.withColumn("rank", row_number().over(windowSpec))

# Nos quedamos solo con el producto top por tienda
df_ranked.filter(col("rank") == 1).select("tienda", "producto", "total_venta").show()
🔹 5. GroupBy con pivot (tipo tabla dinámica)
df = df.withColumn("mes", date_format("fecha", "yyyy-MM"))

df.groupBy("mes") \
  .pivot("producto") \
  .agg(sum("total_venta")) \
  .orderBy("mes") \
  .show()
🔹 7. GroupBy + Join: comparar tiendas
ventas = df.groupBy("producto", "tienda") \
    .agg(sum("total_venta").alias("total_venta"))

ventas_1 = ventas.filter(col("tienda") == "Tienda 1").select("producto", col("total_venta").alias("venta_t1"))
ventas_2 = ventas.filter(col("tienda") == "Tienda 2").select("producto", col("total_venta").alias("venta_t2"))

ventas_comparadas = ventas_1.join(ventas_2, on="producto", how="outer").fillna(0)
ventas_comparadas.withColumn("diferencia", col("venta_t1") - col("venta_t2")).show()
