-- Consultas avanzadas de Neo4j

-- 1. Encontrar todas las personas que trabajan en una empresa específica
  MATCH (p:Person)-[:WORKS_AT]->(e:Empresa {nombre: 'TechCorp'})
  RETURN p.nombre;

-- 2. Obtener el número total de empleados por empresa
  MATCH (p:Person)-[:WORKS_AT]->(e:Empresa)
  RETURN e.nombre, COUNT(p) AS total_empleados;

-- 3. Encontrar empleados que trabajen en más de una empresa
  MATCH (p:Person)-[:WORKS_AT]->(e:Empresa)
  WITH p, COUNT(e) AS total_empresas
  WHERE total_empresas > 1
  RETURN p.nombre;

-- 4. Insertar un nuevo nodo de persona con relaciones a una empresa
  CREATE (p:Person {nombre: 'Carlos', edad: 30})
  MATCH (e:Empresa {nombre: 'TechCorp'})
  CREATE (p)-[:WORKS_AT]->(e);

-- 5. Actualizar el departamento de una persona
  MATCH (p:Person {nombre: 'Luis'})-[:WORKS_AT]->(e:Empresa)
  SET p.departamento = 'IT';

-- 6. Eliminar empleados sin relaciones a ninguna empresa
  MATCH (p:Person)
  WHERE NOT (p)-[:WORKS_AT]->()
  DELETE p;

-- 7. Mostrar todos los proyectos y las personas asignadas a ellos
  MATCH (p:Person)-[:ASSIGNED_TO]->(pr:Project)
  RETURN pr.nombre, COLLECT(p.nombre) AS personas_asignadas;

-- 8. Ordenar empresas por el número de empleados de mayor a menor
  MATCH (p:Person)-[:WORKS_AT]->(e:Empresa)
  RETURN e.nombre, COUNT(p) AS total_empleados
  ORDER BY total_empleados DESC;

-- 9. Encontrar proyectos con más de 3 personas asignadas
  MATCH (p:Person)-[:ASSIGNED_TO]->(pr:Project)
  WITH pr, COUNT(p) AS total_personas
  WHERE total_personas > 3
  RETURN pr.nombre;

-- 10. Crear una relación de amistad entre dos personas si no existe
  MATCH (p1:Person {nombre: 'Ana'}), (p2:Person {nombre: 'Carlos'})
  MERGE (p1)-[:FRIEND_WITH]->(p2);

-- 11. Obtener la persona con más conexiones laborales
  MATCH (p:Person)-[:WORKS_AT]->(e:Empresa)
  RETURN p.nombre, COUNT(e) AS total_empresas
  ORDER BY total_empresas DESC
  LIMIT 1;

-- 12. Contar el número total de proyectos por equipo
  MATCH (t:Team)-[:ASSIGNED_TO]->(pr:Project)
  RETURN t.nombre, COUNT(pr) AS total_proyectos;

-- 13. Encontrar equipos con más de 5 miembros
  MATCH (p:Person)-[:MEMBER_OF]->(t:Team)
  WITH t, COUNT(p) AS total_miembros
  WHERE total_miembros > 5
  RETURN t.nombre;

-- 14. Obtener la relación jerárquica de un equipo (miembro a líder)
  MATCH (p:Person)-[:MEMBER_OF]->(t:Team {nombre: 'Development'}),
        (t)-[:LEAD_BY]->(l:Person)
  RETURN p.nombre, l.nombre AS lider;

-- 15. Encontrar personas que trabajaron en el mismo proyecto más de una vez
  MATCH (p:Person)-[r:ASSIGNED_TO]->(pr:Project)
  WITH p, pr, COUNT(r) AS asignaciones
  WHERE asignaciones > 1
  RETURN p.nombre, pr.nombre;

-- 16. Mostrar la ruta más corta entre dos personas en la misma empresa
  MATCH path = shortestPath((p1:Person {nombre: 'Ana'})-[:FRIEND_WITH|WORKS_AT*]-(p2:Person {nombre: 'Luis'}))
  RETURN path;

-- 17. Encontrar todas las empresas en las que trabajó una persona específica
  MATCH (p:Person {nombre: 'Carlos'})-[:WORKS_AT]->(e:Empresa)
  RETURN e.nombre;

-- 18. Obtener empleados que cambiaron de empresa más de dos veces
  MATCH (p:Person)-[:WORKS_AT]->(e:Empresa)
  WITH p, COUNT(e) AS cambios_empresa
  WHERE cambios_empresa > 2
  RETURN p.nombre;

-- 19. Crear una nueva empresa y asignar empleados existentes a ella
  CREATE (e:Empresa {nombre: 'NewCorp', sector: 'Tecnología'})
  MATCH (p:Person {nombre: 'Ana'})
  CREATE (p)-[:WORKS_AT]->(e);

-- 20. Mostrar los empleados y su antigüedad en la empresa
  MATCH (p:Person)-[r:WORKS_AT]->(e:Empresa)
  RETURN p.nombre, e.nombre, r.fecha_inicio, duration.between(date(r.fecha_inicio), date()).years AS antiguedad_años;
