from neo4j import GraphDatabase

class Neo4jQueries:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))
    
    def close(self):
        if self._driver is not None:
            self._driver.close()
    
    def get_persons_roles_in_company(self, company_name):
        query = (
            """
            MATCH (p:Personas)-[r:WORKS_AT]->(e:Empresas)
            WHERE e.name = $company_name
            RETURN p.name AS person, r.fecha AS rol
            """
        )
        with self._driver.session() as session:
            result = session.run(query, company_name=company_name)
            return [record for record in result]
    
    def get_persons_with_same_role(self, rol):
        query = (
            """
            MATCH (p:Personas)-[r:WORKS_AT]->(e:Empresas)
            WHERE r.fecha = $rol
            RETURN p.name AS person, e.name AS company
            """
        )
        with self._driver.session() as session:
            result = session.run(query, rol=rol)
            return [record for record in result]
        
    def get_person_names_by_ids(self, person_ids):
        query = (
            """
            MATCH (p:Personas)
            WHERE toInteger(p.id) IN $person_ids
            RETURN p.id AS person_id, p.name AS person_name
            """
        )
        with self._driver.session() as session:
            result = session.run(query, person_ids=[int(pid) for pid in person_ids])  # Convertimos a enteros
            return {record["person_id"]: record["person_name"] for record in result}  # Retornamos un diccionario {id: nombre}


    
    def get_common_companies_between_people(self, person1, person2):
        query = (
            """
            MATCH (p1:Personas)-[:WORKS_AT]->(e:Empresas)<-[:WORKS_AT]-(p2:Personas)
            WHERE p1.name = $person1 AND p2.name = $person2
            RETURN e.name AS company
            """
        )
        with self._driver.session() as session:
            result = session.run(query, person1=person1, person2=person2)
            return [record for record in result]