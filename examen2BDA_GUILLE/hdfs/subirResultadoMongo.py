from pymongo import MongoClient
import json
from pywebhdfs.webhdfs import PyWebHdfsClient

# Conexión a MongoDB
client = MongoClient("mongodb://root:my-secret-pw@localhost:27018/")
db = client['test_db']  # Cambia por tu base de datos
collection = db['users']  # Cambia por tu colección

# Consulta MongoDB usando lookup (simulación de JOIN)
pipeline = [
    {
        "$lookup": {
            "from": "orders",  # Nombre de la colección a unir
            "localField": "user_id",  # Campo en la colección "users"
            "foreignField": "user_id",  # Campo correspondiente en "orders"
            "as": "user_orders"  # Nombre del nuevo campo que contendrá la lista de órdenes
        }
    },
    {
        "$match": {  # Filtro adicional, por ejemplo, para usuarios activos
            "status": "active"
        }
    },
    {
        "$sort": {  # Ordenar por fecha de creación
            "created_at": -1
        }
    },
    {
        "$limit": 100  # Limitar el número de resultados
    }
]

# Ejecutar la consulta
result = list(collection.aggregate(pipeline))

# Guardar resultado en JSON
json_file_path = "resultado_mongo.json"
with open(json_file_path, 'w') as json_file:
    json.dump(result, json_file, default=str)

# Subir a HDFS
hdfs = PyWebHdfsClient(host='localhost', port='9870', user_name='root')
with open(json_file_path, 'r') as file:
    content = file.read()

hdfs.create_file("/data/mongo/resultado_mongo.json", content)
print("Resultado de MongoDB subido a HDFS en formato JSON")
