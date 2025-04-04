import csv
import mysql.connector


# Queries para crear las tablas
create_table_queryLocations = """
        CREATE TABLE IF NOT EXISTS Locations (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            city VARCHAR(255)
        )
        """

create_table_querySkills = """
        CREATE TABLE IF NOT EXISTS Skills (
            id INT PRIMARY KEY,
            name VARCHAR(255)
        )
        """

create_table_queryHasSkill = """
        CREATE TABLE IF NOT EXISTS Has_Skill (
            person_id INT,
            skill_id INT,
            proficiency VARCHAR(255),
            PRIMARY KEY (person_id, skill_id, proficiency)
        )
        """

create_table_queryPokemon = """
        CREATE TABLE IF NOT EXISTS Pokemon (
            pokemon_id INT PRIMARY KEY,
            description TEXT,
            pokeGame VARCHAR(255)
        )
        """


# Función para leer el archivo CSV
def read_csv_file(filename):
    data = []
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for element in reader:
            data.append(element)        
    return data

# Clase para interactuar con la base de datos
class Database:
    def __init__(self, host, user, password, database, port):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def create_table(self, stringCreate):
        self.cursor.execute(stringCreate)
        self.connection.commit()

    def insert_data(self, query, params):
        self.cursor.execute(query, params)
        self.connection.commit()

# Leer los archivos CSV
readerLocations = read_csv_file("locations.csv")
readerSkills = read_csv_file("skills.csv")
readerHasSkill = read_csv_file("has_skill.csv")
readerPokemon = read_csv_file("pokemon.csv")

# Configuración de la base de datos
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "tu_contraseña"
DB_DATABASE = "data_import"  # Cambia esto por el nombre de tu base de datos
DB_PORT = "8888"  # Asegúrate de que el puerto sea el correcto

# Crear la conexión con la base de datos
db = Database(DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE, DB_PORT)

# Crear las tablas
db.create_table(create_table_queryLocations)
db.create_table(create_table_querySkills)
db.create_table(create_table_queryHasSkill)
db.create_table(create_table_queryPokemon)


# Insertar datos en la tabla Locations
for element in readerLocations[1:]:  # Ignorar la primera fila (encabezado)
    insert_query = "INSERT INTO Locations (id, name, city) VALUES (%s, %s, %s)"
    data = (element[1], element[0], element[2])  # Cambié el orden aquí
    db.insert_data(insert_query, data)

# Insertar datos en la tabla Skills
for element in readerSkills[1:]:
    insert_query = "INSERT INTO Skills (id, name) VALUES (%s, %s)"
    data = (element[0], element[1])
    db.insert_data(insert_query, data)




for element in readerHasSkill[1:]:
    insert_query = "INSERT IGNORE INTO Has_Skill (person_id, skill_id, proficiency) VALUES (%s, %s, %s)"
    data = (element[1], element[2], element[0])
    db.insert_data(insert_query, data)


# Insertar datos en la tabla Pokemon
for element in readerPokemon[1:]:
    insert_query = "INSERT IGNORE INTO Pokemon (pokemon_id, description, pokeGame) VALUES (%s, %s, %s)"
    data = (element[0], element[2], element[1])  # Cambié el orden aquí
    db.insert_data(insert_query, data)
