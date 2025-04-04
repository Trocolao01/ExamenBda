
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, monotonically_increasing_id, first, dense_rank,row_number
from pyspark.sql.window import Window
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# ========================
# 1️⃣ Crear la sesión de Spark
# ========================
spark = SparkSession.builder \
    .appName("READ TXT FROM S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# ========================
# 2️⃣ Leer archivos TXT desde S3
# ========================
df_txt = spark.read.text("s3a://sample-bucket/txt_files")

# Crear una columna con el índice de la línea dentro de cada grupo de 4
df_txt = df_txt.withColumn("line_num", row_number().over(Window.orderBy("value")))

# Agrupar de 4 en 4 líneas y pivotear a columnas
df_pivot = df_txt.groupBy((col("line_num") - 1) // 4).agg(
    F.first("value").alias("Evento"),
    F.first("value").alias("Nombre del entrenador"),
    F.first("value").alias("Equipo de Pokemon"),
    F.first("value").alias("Resultado de la batalla")
)

# Aplicar expresiones regulares
df_final = df_pivot.select(
    regexp_extract(col("Evento"), r"Evento: (.+)", 1).alias("Evento"),
    regexp_extract(col("Nombre del entrenador"), r"Nombre del entrenador: (.+)", 1).alias("Nombre del entrenador"),
    regexp_extract(col("Equipo de Pokemon"), r"Equipo de Pokemon: (.+)", 1).alias("Equipo de Pokemon"),
    regexp_extract(col("Resultado de la batalla"), r"Resultado de la batalla: (.+)", 1).alias("Resultado de la batalla")
)

df_final.show(truncate=False)