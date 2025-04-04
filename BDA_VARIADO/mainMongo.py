import csv
from pymongo import MongoClient

# Función para leer archivos CSV y convertirlos en una lista de diccionarios
def read_csv_file(filename):
    data = []
    try:
        with open(filename, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)  # Usa los encabezados como claves
            for row in reader:
                # Convertir valores numéricos si es necesario
                for key in row:
                    if row[key].isdigit():
                        row[key] = int(row[key])
                data.append(row)
        return data
    except FileNotFoundError:
        print(f"Error: El archivo {filename} no se encontró.")
        return None

class MongoDBOperations:
    def __init__(self, database_name, port, username, password, host="localhost"):
        # Construir la URI de conexión con autenticación
        mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/"
        
        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]
        
    def insert_data(self, collection_name, data_list):
        """ Inserta múltiples documentos en una colección de MongoDB """
        if data_list:
            self.db[collection_name].insert_many(data_list)
            print(f"Insertados {len(data_list)} documentos en la colección '{collection_name}'")

# Configuración del contenedor MongoDB
MONGO_HOST = "localhost"  # Cambia a la IP del contenedor si usas Docker en una VM
MONGO_PORT = "27017"
MONGO_USER = "admin"
MONGO_PASSWORD = "adminpassword"
MONGO_DB_NAME = "data_import"

# Leer archivos CSV y convertirlos en listas de diccionarios


projects = read_csv_file("projects.csv")
teams = read_csv_file("teams.csv")
works_in_team = read_csv_file("works_in_team.csv")

favourite_pokemon = read_csv_file("favourite_pokemon.csv")


# Conectar con MongoDB usando las credenciales
mongo_operations = MongoDBOperations(MONGO_DB_NAME, MONGO_PORT, MONGO_USER, MONGO_PASSWORD, MONGO_HOST)


# Insertar datos en MongoDB
mongo_operations.insert_data("teams", teams)
mongo_operations.insert_data("projects", projects)
mongo_operations.insert_data("works_in_team", works_in_team)

mongo_operations.insert_data("favourite_pokemon", favourite_pokemon)

