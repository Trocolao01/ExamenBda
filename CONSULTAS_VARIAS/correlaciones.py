from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

spark = SparkSession.builder.appName("Correlaciones").getOrCreate()

data = [
    (10.0, 2, 0.0),
    (20.0, 1, 0.1),
    (10.0, 5, 0.05),
    (15.0, 3, 0.0),
    (20.0, 2, 0.15),
    (10.0, 4, 0.0),
    (10.0, 6, 0.05),
    (15.0, 1, 0.1),
]

columns = ["precio_unitario", "cantidad", "descuento_aplicado"]
df = spark.createDataFrame(data, columns)
df = df.withColumn("total_venta", col("precio_unitario") * col("cantidad") * (1 - col("descuento_aplicado")))
df.show()

df.stat.corr("cantidad", "total_venta")

df.stat.corr("precio_unitario", "total_venta")

df.stat.corr("descuento_aplicado", "total_venta")

num_cols = ["precio_unitario", "cantidad", "descuento_aplicado", "total_venta"]

for i in range(len(num_cols)):
    for j in range(i+1, len(num_cols)):
        col1 = num_cols[i]
        col2 = num_cols[j]
        corr_val = df.stat.corr(col1, col2)
        print(f"Correlación entre {col1} y {col2}: {round(corr_val, 3)}")

assembler = VectorAssembler(inputCols=num_cols, outputCol="features")
df_vector = assembler.transform(df)

corr_matrix = Correlation.corr(df_vector, "features", "pearson").head()[0]
print("Matriz de correlación:")
print(corr_matrix)
