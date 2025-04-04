import csv
import json
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import pandas as pd
import requests

class Neo4jDatabase:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="my-secret-pw"):
        """Inicializa la conexión a Neo4j."""
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None
        self.connect()

    def connect(self):
        """Conecta a la base de datos Neo4j."""
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            print("✅ Conexión exitosa a Neo4j")
        except ServiceUnavailable as e:
            print(f"❌ Error al conectar a Neo4j: {e}")
            self.driver = None

    def close(self):
        """Cierra la conexión a la base de datos."""
        if self.driver:
            self.driver.close()
            print("🔒 Conexión cerrada correctamente.")

    def create_node(self, label, **properties):
        """Crea un nodo en Neo4j."""
        with self.driver.session() as session:
            query = f"CREATE (n:{label} {{ {', '.join([f'{key}: \'{value}\'' for key, value in properties.items()])} }}) RETURN n"
            try:
                result = session.run(query)
                print("✅ Nodo creado exitosamente.")
                return result.single()
            except Exception as e:
                print(f"❌ Error al crear nodo: {e}")

    def create_relationship(self, node1_label, node1_id, relationship_type, node2_label, node2_id):
        """Crea una relación entre dos nodos en Neo4j."""
        with self.driver.session() as session:
            query = f"""
            MATCH (a:{node1_label}), (b:{node2_label})
            WHERE a.id = {node1_id} AND b.id = {node2_id}
            CREATE (a)-[r:{relationship_type}]->(b)
            RETURN r
            """
            try:
                result = session.run(query)
                print("✅ Relación creada exitosamente.")
                return result.single()
            except Exception as e:
                print(f"❌ Error al crear relación: {e}")


    def query_data(self, query):
        """Ejecuta una consulta en Neo4j y devuelve los resultados."""
        with self.driver.session() as session:
            try:
                result = session.run(query)
                for record in result:
                    print(record)
                return result
            except Exception as e:
                print(f"❌ Error al consultar datos: {e}")

    def update_node(self, label, node_id, **properties):
        """Actualiza un nodo en Neo4j."""
        with self.driver.session() as session:
            query = f"""
            MATCH (n:{label}) WHERE n.id = {node_id}
            SET {', '.join([f'n.{key} = \'{value}\'' for key, value in properties.items()])}
            RETURN n
            """
            try:
                result = session.run(query)
                print("✅ Nodo actualizado exitosamente.")
                return result.single()
            except Exception as e:
                print(f"❌ Error al actualizar nodo: {e}")

    def delete_node(self, label, node_id):
        """Elimina un nodo en Neo4j."""
        with self.driver.session() as session:
            query = f"""
            MATCH (n:{label}) WHERE n.id = {node_id}
            DELETE n
            """
            try:
                session.run(query)
                print(f"🗑️ Nodo con ID {node_id} eliminado exitosamente.")
            except Exception as e:
                print(f"❌ Error al eliminar nodo: {e}")

    def delete_relationship(self, node1_label, node1_id, relationship_type, node2_label, node2_id):
        """Elimina una relación entre dos nodos."""
        with self.driver.session() as session:
            query = f"""
            MATCH (a:{node1_label})-[r:{relationship_type}]->(b:{node2_label})
            WHERE a.id = {node1_id} AND b.id = {node2_id}
            DELETE r
            """
            try:
                session.run(query)
                print("🗑️ Relación eliminada exitosamente.")
            except Exception as e:
                print(f"❌ Error al eliminar relación: {e}")

    # Verificar los nodos creados
    def check_nodes(self):
        with self.driver.session() as session:
            query = "MATCH (n:Person) RETURN n"
            result = session.run(query)
            for record in result:
                print(record)

##################################################################INSERTORES DESDE AQUI##############################################################################################################
    def insert_data_from_csv(self, label, fields, csv_file):
            """Inserta datos desde un archivo CSV."""
            with open(csv_file, mode='r') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    data = {field: row[field] for field in fields}
                    self.create_node(label, **data)

    def insert_data_from_json(self, label, fields, json_file):
        """Inserta datos desde un archivo JSON."""
        with open(json_file, mode='r') as file:
            data = json.load(file)
            for item in data:
                node_data = {field: item[field] for field in fields}
                self.create_node(label, **node_data)

    def insert_data_from_txt(self, label, fields, txt_file, delimiter='|'):
        """Inserta datos desde un archivo de texto (TXT)."""
        data = pd.read_csv(txt_file, delimiter=delimiter)
        for index, row in data.iterrows():
            node_data = {field: row[field] for field in fields}
            self.create_node(label, **node_data)

    def insert_data_from_api(self, label, fields, api_url):
        """Inserta datos desde una API."""
        response = requests.get(api_url)
        data = response.json()
        for item in data:
            node_data = {field: item[field] for field in fields}
            self.create_node(label, **node_data)


# 🛠️ EJEMPLO DE USO
if __name__ == "__main__":
    # Conectar a Neo4j
    neo4j_db = Neo4jDatabase(uri="bolt://localhost:7687", user="neo4j", password="my-secret-pw")

    # Crear nodos
    neo4j_db.create_node("Person", id=1, name="Juan", age=30)
    neo4j_db.create_node("Person", id=2, name="Maria", age=25)
    neo4j_db.create_node("Person", id=3, name="Carlos", age=35)
    neo4j_db.create_node("Person", id=4, name="Ana", age=40)

    # Crear relación
    neo4j_db.create_relationship("Person", 1, "KNOWS", "Person", 2)
    neo4j_db.create_relationship("Person", 1, "KNOWS", "Person", 3)
    neo4j_db.create_relationship("Person", 2, "KNOWS", "Person", 4)

    # Consultar datos
    query = "MATCH (n:Person) RETURN n"
    neo4j_db.query_data(query)
    neo4j_db.check_nodes()
    neo4j_db.update_node("Person", 1, name="Juan Carlos", age=31)
    neo4j_db.delete_node("Person", 2)
    neo4j_db.delete_relationship("Person", 1, "KNOWS", "Person", 2)
    neo4j_db.insert_data_from_csv('Person', ['name', 'age'], 'users.csv')
    neo4j_db.insert_data_from_json('Person', ['name', 'age'], 'users.json')
    neo4j_db.insert_data_from_txt('Person', ['name', 'age'], 'users.txt', delimiter='|')
    neo4j_db.insert_data_from_api('Person', ['name', 'age'], 'https://pokeapi.co/api/v2/pokemon/ditto')
    neo4j_db.close()
