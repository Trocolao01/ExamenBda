from neo4j import GraphDatabase
import csv

# Función para leer el archivo CSV
def read_csv_file(filename):
    data = []
    with open(filename, 'r', encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)  # Salta la primera línea (cabecera)
        for element in reader:
            data.append(element)
    return data

class Neo4jCRUD:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None
        self._connect()

    def _connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    # Método para crear nodos
    def create_node(self, label, properties):
        with self._driver.session() as session:
            session.write_transaction(self._create_node, label, properties)

    @staticmethod
    def _create_node(tx, label, properties):
        query = (
            f"CREATE (n:{label} $props)"
        )
        tx.run(query, props=properties)

    # Método para crear relaciones
    def create_relationship(self, labelOrigin, propertyOrigin, labelEnd, propertyEnd, relationshipName, fecha):
        with self._driver.session() as session:
            session.write_transaction(self._create_relationship, labelOrigin, propertyOrigin, labelEnd, propertyEnd, relationshipName, fecha)

    @staticmethod
    def _create_relationship(tx, labelOrigin, propertyOrigin, labelEnd, propertyEnd, relationshipName, fecha):
        query = (
            f"MATCH (n:{labelOrigin}), (c:{labelEnd}) "
            f"WHERE n.id = $propertyOrigin AND c.id = $propertyEnd "
            f"CREATE (n)-[:{relationshipName} {{fecha: $fecha}}]->(c)"
        )
        tx.run(query, propertyOrigin=propertyOrigin, propertyEnd=propertyEnd, fecha=fecha)

# Conexión a Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "tu_contraseña"

neo4j_crud = Neo4jCRUD(uri, user, password)

# Leer archivos CSV
readerPersons = read_csv_file("persons.csv")
readerEmpresas = read_csv_file("empresas.csv")
readerWorks = read_csv_file("works_at.csv")

# Insertar nodos de Personas
for element in readerPersons:
    node_properties = {
        "id": element[0], 
        "name": element[1],
        "age": element[2]
    }
    neo4j_crud.create_node("Personas", node_properties)

# Insertar nodos de Empresas
for element in readerEmpresas:
    node_properties = {
        "id": element[0], 
        "name": element[1],
        "sector": element[2]
    }
    neo4j_crud.create_node("Empresas", node_properties)

# Insertar relaciones "WORKS_AT" entre Personas y Empresas
for element in readerWorks:
    person_id = element[0]  # ID de la persona
    company_id = element[2]  # ID de la empresa (location_id)
    role = element[1]  # Rol de la persona en la empresa
    neo4j_crud.create_relationship("Personas", person_id, "Empresas", company_id, "WORKS_AT", role)

# Cerrar conexión con Neo4j
neo4j_crud.close()
