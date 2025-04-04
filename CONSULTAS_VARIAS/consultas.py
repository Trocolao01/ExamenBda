#Filtrar ventas
df.filter(col("total_venta") > 40).show()
#🔹 Consulta 2: Ordenar por fecha descendente
df.orderBy(col("fecha").desc()).show()
#🔹 Consulta 3: Total vendido por producto y tienda en 2023
df.filter(year("fecha") == 2023) \
  .groupBy("producto", "tienda") \
  .agg(sum("total_venta").alias("total_2023")) \
  .orderBy("total_2023", ascending=False) \
  .show()
#🔹 Consulta 4: ¿Qué producto vendió más unidades en total?
df.groupBy("producto").agg(sum("cantidad").alias("total_unidades")) \
  .orderBy("total_unidades", ascending=False) \
  .limit(1).show()

from pyspark.sql.window import Window
#🔹 Consulta 5: Ranking de ventas por tienda usando ventanas
windowSpec = Window.partitionBy("tienda").orderBy(col("total_venta").desc())

df.withColumn("rank", row_number().over(windowSpec)) \
  .filter(col("rank") == 1) \
  .select("tienda", "producto", "total_venta", "rank") \
  .show()

#🔹 Consulta 6: Porcentaje de participación por producto (por tienda)
total_por_tienda = df.groupBy("tienda") \
    .agg(sum("total_venta").alias("venta_total"))

df_pct = df.groupBy("tienda", "producto") \
    .agg(sum("total_venta").alias("venta_producto"))

df_join = df_pct.join(total_por_tienda, on="tienda") \
    .withColumn("porcentaje", round((col("venta_producto") / col("venta_total")) * 100, 2))

df_join.select("tienda", "producto", "porcentaje").orderBy("tienda", "porcentaje", ascending=False).show()

#🔹 Consulta 7: Detectar caída de ventas mes a mes (usando lag)
windowMes = Window.partitionBy("producto").orderBy("fecha")

df_mensual = df.groupBy("producto", month("fecha").alias("mes")) \
    .agg(sum("total_venta").alias("ventas_mes"))

df_mensual = df_mensual.withColumn("ventas_anterior", lag("ventas_mes").over(Window.partitionBy("producto").orderBy("mes"))) \
                       .withColumn("caida", col("ventas_mes") < col("ventas_anterior"))

df_mensual.show()
