from pymongo import MongoClient
import json

# Conexión a MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['red_social']  # Base de datos de ejemplo
collection = db['usuarios']  # Colección de usuarios

# Consulta compleja: Obtener usuarios con más de 100 amigos y proyectar solo nombre y amigos
query = {"numero_amigos": {"$gt": 100}}  # Filtro: más de 100 amigos
projection = {"nombre": 1, "amigos": 1, "_id": 0}  # Proyección: solo nombre y amigos

# Realizar la consulta
resultados = collection.find(query, projection)

# Convertir los resultados a una lista
resultados_list = list(resultados)

# Exportar a JSON
with open('resultados_mongo_complejo.json', 'w') as f:
    json.dump(resultados_list, f, default=str, indent=4)

print("Datos exportados a resultados_mongo_complejo.json")
