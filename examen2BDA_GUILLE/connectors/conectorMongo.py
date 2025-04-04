from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure  # Usamos ServerSelectionTimeoutError ahora
import csv
import json
import requests

def connect_to_mongo(host='localhost', port=27018, user='root', password='my-secret-pw', database=None):
    """Conecta a MongoDB y devuelve el cliente."""
    try:
        uri = f"mongodb://{user}:{password}@{host}:{port}/"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)  # Añadimos un timeout para la selección de servidor
        client.server_info()  # Esto verifica la conexión al servidor MongoDB
        print("✅ Conexión exitosa a MongoDB")
        return client[database] if database else client
    except ServerSelectionTimeoutError as e:
        print(f"❌ Error al conectar a MongoDB: {e}")
        return None

def create_database(db_name):
    """Crea una base de datos en MongoDB (en realidad se crea cuando se insertan datos)."""
    client = connect_to_mongo()
    if client:
        try:
            db = client[db_name]
            print(f"📂 Base de datos '{db_name}' creada o ya existente.")
            return db
        except OperationFailure as e:
            print(f"❌ Error al crear la base de datos: {e}")

def create_collection(db_name, collection_name):
    """Crea una colección en la base de datos."""
    db = connect_to_mongo(database=db_name)
    if db is not None:
        try:
            collection = db[collection_name]
            print(f"📋 Colección '{collection_name}' creada en '{db_name}'.")
            return collection
        except OperationFailure as e:
            print(f"❌ Error al crear la colección: {e}")

def insert_data(db_name, collection_name, data):
    """Inserta datos en una colección."""
    db = connect_to_mongo(database=db_name)
    if db is not None:  # Cambiado aquí para evitar el error
        try:
            collection = db[collection_name]
            result = collection.insert_one(data)
            print(f"✅ Documento insertado con el ID: {result.inserted_id}")
        except OperationFailure as e:
            print(f"❌ Error al insertar datos: {e}")

def update_data(db_name, collection_name, filter_query, update_values):
    """Actualiza documentos en una colección."""
    db = connect_to_mongo(database=db_name)
    if db is not None:
        try:
            collection = db[collection_name]
            result = collection.update_one(filter_query, {'$set': update_values})
            if result.matched_count > 0:
                print("✅ Documento(s) actualizado(s) correctamente.")
            else:
                print("❌ No se encontraron documentos para actualizar.")
        except OperationFailure as e:
            print(f"❌ Error al actualizar datos: {e}")

def delete_data(db_name, collection_name, filter_query):
    """Elimina documentos de una colección."""
    db = connect_to_mongo(database=db_name)
    if db is not None:
        try:
            collection = db[collection_name]
            result = collection.delete_one(filter_query)
            if result.deleted_count > 0:
                print("🗑️ Documento eliminado correctamente.")
            else:
                print("❌ No se encontraron documentos para eliminar.")
        except OperationFailure as e:
            print(f"❌ Error al eliminar datos: {e}")

def select_data(db_name, collection_name, query={}):
    """Recupera documentos de una colección."""
    db = connect_to_mongo(database=db_name)
    if db is not None:  # Cambiado aquí para evitar el error
        try:
            collection = db[collection_name]
            results = collection.find(query)
            for document in results:
                print(document)
            return results
        except OperationFailure as e:
            print(f"❌ Error al consultar datos: {e}")

################################################################## INSERTORES DESDE AQUÍ ##############################################################################################################
def insert_data_from_csv(db_name, collection_name, csv_file):
    """Inserta datos en una colección desde un archivo CSV."""
    db = connect_to_mongo(database=db_name)
    if db is not None:
        try:
            collection = db[collection_name]
            with open(csv_file, newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    collection.insert_one(row)
            print("✅ Datos insertados desde CSV correctamente.")
        except OperationFailure as e:
            print(f"❌ Error al insertar datos desde CSV: {e}")

def insert_data_from_json(db_name, collection_name, json_file):
    """Inserta datos en una colección desde un archivo JSON."""
    db = connect_to_mongo(database=db_name)
    if db is not None:
        try:
            collection = db[collection_name]
            with open(json_file, encoding='utf-8') as file:
                data = json.load(file)
                collection.insert_many(data)
            print("✅ Datos insertados desde JSON correctamente.")
        except OperationFailure as e:
            print(f"❌ Error al insertar datos desde JSON: {e}")

def insert_data_from_txt(db_name, collection_name, txt_file):
    """Inserta datos en una colección desde un archivo de texto."""
    db = connect_to_mongo(database=db_name)
    if db is not None:
        try:
            collection = db[collection_name]
            with open(txt_file, encoding='utf-8') as file:
                data = file.read().splitlines()
                for line in data:
                    collection.insert_one({'line': line})
            print("✅ Datos insertados desde TXT correctamente.")
        except OperationFailure as e:
            print(f"❌ Error al insertar datos desde TXT: {e}")


def insert_data_from_api(db_name, collection_name, api_url):
    """Inserta datos en una colección desde una API."""
    db = connect_to_mongo(database=db_name)
    if db is not None:
        try:
            collection = db[collection_name]
            response = requests.get(api_url)
            if response.status_code == 200:
                data = response.json()
                collection.insert_many(data)
                print("✅ Datos insertados desde API correctamente.")
            else:
                print(f"❌ Error en la solicitud API: Código {response.status_code}")
        except OperationFailure as e:
            print(f"❌ Error al insertar datos desde API: {e}")

# 🛠️ EJEMPLO DE USO
if __name__ == "__main__":
    create_database('test_db')
    create_collection('test_db', 'users')
    insert_data('test_db', 'users', {'name': 'Juan', 'age': 30})
    select_data('test_db', 'users')
    insert_data_from_csv('test_db', 'users', 'users.csv')
    insert_data_from_txt('test_db', 'users', 'users.txt')
    insert_data_from_json('test_db', 'users', 'users.json')
    insert_data_from_api('test_db', 'users', 'https://pokeapi.co/api/v2/pokemon/ditto')
