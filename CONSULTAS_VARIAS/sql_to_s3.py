from pyspark.sql import SparkSession

# Configuración de acceso a S3 (LocalStack)
aws_access_key_id = 'test'
aws_secret_access_key = 'test'

# Configuración de Spark
spark = SparkSession.builder \
    .appName("MySQL to S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.33.jar") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Conectar a MySQL
mysql_url = "jdbc:mysql://mysql:3306/retail-db"
properties = {"user": "root", "password": "root", "driver": "com.mysql.cj.jdbc.Driver"}

# Definir la consulta SQL que se desea ejecutar
query = """
SELECT t.store_id, t.store_name, v.sales_amount
FROM tienda t
INNER JOIN ventas v ON t.store_id = v.store_id
WHERE v.sales_date BETWEEN '2025-01-01' AND '2025-03-31'
"""

# Leer el resultado de la consulta directamente desde MySQL
df_result = spark.read.jdbc(mysql_url, f"({query}) as query_result", properties=properties)

# Guardar el resultado en S3
df_result.write.mode('overwrite').csv('s3a://bucket/mysql_data', header=True, sep=',')

spark.stop()
