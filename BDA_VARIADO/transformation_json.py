from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, to_date, mean, from_unixtime, lit
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.ml.feature import Imputer
import pandas as pd
import numpy as np

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("PROCESAMIENTO DE JSON") \
    .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar:/opt/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar:/opt/spark/jars/*") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Leer datos desde el bucket S3 en formato JSON
df_json = spark.read \
    .format("json") \
    .load("s3a://sample-bucket/output_json/")  # Cambia esta ruta por la ubicación de tu JSON en S3

# Mostrar el esquema para ver cómo se estructura el DataFrame
df_json.printSchema()

# Mostrar las primeras filas del DataFrame para ver los datos
print(df_json.count())
