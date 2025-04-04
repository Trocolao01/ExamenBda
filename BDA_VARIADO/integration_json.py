from pyspark.sql import SparkSession

# Claves de acceso para S3 (localstack en este caso)
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("SPARK S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Subir archivo JSON a S3
try:
    # Leer archivo JSON desde el directorio local
    df_json = spark.read.option("multiline", "true").json("/opt/spark-data/json/eventos.json")
    
    # Escribir el DataFrame en S3 como JSON
    df_json \
    .write \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
    .mode('overwrite') \
    .json(path='s3a://sample-bucket/output_json')  # Escribir como JSON en S3
    
    # Detener Spark
    spark.stop()

except Exception as e:
    print("Error al leer o escribir archivo JSON")
    print(e)
