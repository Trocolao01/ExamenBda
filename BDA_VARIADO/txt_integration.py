from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("UPLOAD TXT TO S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# ========================
# 1️⃣ Leer archivos TXT desde una carpeta local
# ========================
ruta_local_txt = "/opt/spark-data/txt/pokemon.txt"  # Carpeta donde están los archivos TXT
df_txt = spark.read.text(ruta_local_txt)  # Carga todos los archivos TXT de la carpeta

# ========================
# 2️⃣ Subir los archivos TXT a S3
# ========================
s3_path = "s3a://sample-bucket/txt_files/"
df_txt.write.mode("overwrite").text(s3_path)

print(f"Archivos TXT subidos correctamente a {s3_path}")

# ========================
# 3️⃣ Verificar que los archivos están en S3 (leyéndolos)
# ========================
df_verificacion = spark.read.text(s3_path)
df_verificacion.show(truncate=False)

# Cerrar la sesión de Spark
spark.stop()
