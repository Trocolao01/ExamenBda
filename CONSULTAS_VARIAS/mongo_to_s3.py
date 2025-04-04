from pyspark.sql import SparkSession

# Configuración de acceso a S3 (LocalStack)
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Configuración de Spark
spark = SparkSession.builder \
    .appName("MongoDB to S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Conectar a MongoDB
mongo_uri = "mongodb://root:contraseñaroot@mongo:27017/retail-db"

# Realizar una consulta a la colección 'tienda' de MongoDB
query_tienda = '{"store_id": {"$gt": 10}, "city": "CiudadX"}'  # Tiendas con store_id > 10 y en CiudadX
df_tienda = spark.read.format("mongo") \
    .option("uri", f"{mongo_uri}.tienda") \
    .option("spark.mongodb.input.query", query_tienda) \
    .load()

# Realizar una consulta a la colección 'ventas' de MongoDB
query_ventas = '{"sales_amount": {"$gt": 100}, "sales_date": {"$gte": "2025-01-01", "$lte": "2025-03-31"}}'  # Ventas con monto > 100 y dentro de un rango de fechas
df_ventas = spark.read.format("mongo") \
    .option("uri", f"{mongo_uri}.ventas") \
    .option("spark.mongodb.input.query", query_ventas) \
    .load()

# Hacer INNER JOIN entre las colecciones
df_joined = df_tienda.join(df_ventas, df_tienda["store_id"] == df_ventas["store_id"], "inner")

# Guardar en S3
df_joined.write.mode('overwrite').csv('s3a://bucket/mongo_data', header=True, sep=',')

spark.stop()
