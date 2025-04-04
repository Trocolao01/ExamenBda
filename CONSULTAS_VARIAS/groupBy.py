#Agrupación por fecha
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, weekofyear, to_date

spark = SparkSession.builder.appName("Ejemplos").getOrCreate()

data = [
    ("2023-01-15", 100),
    ("2023-01-20", 150),
    ("2023-02-10", 200),
    ("2024-01-15", 300)
]
df = spark.createDataFrame(data, ["fecha", "ventas"])
df = df.withColumn("fecha", to_date("fecha"))
df.show()
#Agrupar por otras cosas
df.groupBy(year("fecha").alias("año")).sum("ventas").show()

df.groupBy(month("fecha").alias("mes")).sum("ventas").show()

df.groupBy(dayofmonth("fecha").alias("día")).sum("ventas").show()

df.groupBy(weekofyear("fecha").alias("semana")).sum("ventas").show()
#Tratamiento nulls
df.filter(df["ventas"].isNull()).show()
# Reemplaza valores nulos en todas las columnas numéricas por 0
df.fillna(0).show()
# Reemplaza solo la columna 'ventas'
df.fillna({"ventas": 0}).show()
df.na.drop().show()
from pyspark.sql.functions import col, sum, when
#Contar los nulos de cada columna
df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).show()
#Manejo de datos tipo ERROR (strings mal escritos, datos inválidos, etc.)
data = [("100",), ("200",), ("ERROR",), (None,)]
df_err = spark.createDataFrame(data, ["valor"])
df_err.show()
#Valores que no son numericos
df_valid = df_err.filter(col("valor").rlike("^[0-9]+$"))
df_valid.show()
#Reemplazar errores con null y luego tratar
df_clean = df_err.withColumn(
    "valor_int",
    when(col("valor").rlike("^[0-9]+$"), col("valor").cast("int")).otherwise(None)
)
df_clean.show()