import mysql.connector
import json
from pywebhdfs.webhdfs import PyWebHdfsClient

# Conexión a MySQL
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='my-secret-pw',
    port=6969,
    database='test_db',  # Cambia esto por el nombre de tu base de datos
    auth_plugin='mysql_native_password'  # 🔹 Esto evita problemas de autenticación
)

# Consulta MySQL
query = "SELECT * FROM users"  # Asegúrate de que la tabla sea correcta
cursor = connection.cursor()
cursor.execute(query)
result = cursor.fetchall()

# Guardar resultado en TXT
txt_file_path = "resultado_mysql.txt"
with open(txt_file_path, 'w') as txt_file:
    for row in result:
        txt_file.write(str(row) + '\n')

# Subir a HDFS
hdfs = PyWebHdfsClient(host='localhost', port='9870', user_name='root')
with open(txt_file_path, 'r') as file:
    content = file.read()

hdfs.create_file("/data/mysql/resultado_mysql.txt", content)
print("Resultado de MySQL subido a HDFS en formato TXT")

# Cerrar conexión
cursor.close()
connection.close()
