-- 1. Seleccionar empleados con el salario más alto en cada departamento
SELECT nombre, salario, departamento 
FROM empleados 
WHERE salario = (SELECT MAX(salario) FROM empleados e2 WHERE e2.departamento = empleados.departamento);

-- 2. Obtener la diferencia de salario entre el empleado con mayor salario y el de menor salario
SELECT MAX(salario) - MIN(salario) AS diferencia_salario FROM empleados;

-- 3. Encontrar empleados que trabajan en más de un proyecto
SELECT nombre FROM empleados 
JOIN asignacion_proyectos ON empleados.id = asignacion_proyectos.empleado_id 
GROUP BY empleados.id HAVING COUNT(proyecto_id) > 1;

-- 4. Seleccionar todos los departamentos y contar empleados (incluso si no tienen empleados asignados)
SELECT departamentos.nombre, COUNT(empleados.id) AS total_empleados 
FROM departamentos 
LEFT JOIN empleados ON departamentos.id = empleados.departamento_id 
GROUP BY departamentos.nombre;

-- 5. Obtener los empleados cuyo salario es superior al promedio de su departamento
SELECT nombre, salario, departamento 
FROM empleados e1 
WHERE salario > (SELECT AVG(salario) FROM empleados e2 WHERE e2.departamento = e1.departamento);

-- 6. Encontrar el proyecto con el mayor número de empleados asignados
SELECT proyecto_id, COUNT(empleado_id) AS total_empleados 
FROM asignacion_proyectos 
GROUP BY proyecto_id 
ORDER BY total_empleados DESC 
LIMIT 1;

-- 7. Usar una subconsulta correlacionada para buscar empleados con el segundo salario más alto
SELECT nombre, salario 
FROM empleados 
WHERE salario = (SELECT DISTINCT salario FROM empleados ORDER BY salario DESC LIMIT 1 OFFSET 1);

-- 8. Insertar un nuevo empleado y asignarle un proyecto en una transacción
START TRANSACTION;
INSERT INTO empleados (nombre, departamento, salario) VALUES ('Carlos', 'IT', 4500);
SET @empleado_id = LAST_INSERT_ID();
INSERT INTO asignacion_proyectos (empleado_id, proyecto_id) VALUES (@empleado_id, 3);
COMMIT;

-- 9. Obtener la duración promedio de los proyectos en días
SELECT AVG(DATEDIFF(fecha_fin, fecha_inicio)) AS duracion_promedio_dias 
FROM proyectos;

-- 10. Mostrar empleados que no tienen ninguna asignación de proyecto
SELECT nombre FROM empleados 
WHERE id NOT IN (SELECT empleado_id FROM asignacion_proyectos);

-- 11. Calcular el salario promedio y la mediana de salarios en la empresa
SELECT AVG(salario) AS salario_promedio FROM empleados;

-- 12. Encontrar los empleados que comparten el mismo salario
SELECT salario, GROUP_CONCAT(nombre) AS empleados 
FROM empleados 
GROUP BY salario HAVING COUNT(*) > 1;

-- 13. Crear una vista de los empleados y sus proyectos
CREATE VIEW empleados_proyectos AS 
SELECT empleados.nombre, proyectos.nombre AS proyecto 
FROM empleados 
JOIN asignacion_proyectos ON empleados.id = asignacion_proyectos.empleado_id 
JOIN proyectos ON asignacion_proyectos.proyecto_id = proyectos.id;

-- 14. Seleccionar empleados que comenzaron en la empresa el mismo día que otro empleado
SELECT e1.nombre, e1.fecha_contratacion 
FROM empleados e1 
JOIN empleados e2 ON e1.fecha_contratacion = e2.fecha_contratacion AND e1.id <> e2.id;

-- 15. Encontrar departamentos donde todos los empleados ganan más de 3000
SELECT departamento 
FROM empleados 
GROUP BY departamento 
HAVING MIN(salario) > 3000;

-- 16. Contar el número de proyectos activos por mes en el último año
SELECT MONTH(fecha_inicio) AS mes, COUNT(*) AS proyectos_activos 
FROM proyectos 
WHERE fecha_inicio >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) 
GROUP BY MONTH(fecha_inicio);

-- 17. Mostrar el nombre de cada proyecto y el nombre del empleado con el mayor salario asignado al proyecto
SELECT proyectos.nombre AS proyecto, empleados.nombre AS empleado, empleados.salario 
FROM proyectos 
JOIN asignacion_proyectos ON proyectos.id = asignacion_proyectos.proyecto_id 
JOIN empleados ON asignacion_proyectos.empleado_id = empleados.id 
WHERE empleados.salario = (SELECT MAX(salario) FROM empleados e2 
                            JOIN asignacion_proyectos ap2 ON e2.id = ap2.empleado_id 
                            WHERE ap2.proyecto_id = proyectos.id);

-- 18. Obtener el historial de cambios de salario de los empleados
SELECT empleado_id, salario_anterior, nuevo_salario, fecha_cambio 
FROM historial_salarios 
ORDER BY fecha_cambio DESC;

-- 19. Encontrar empleados cuyo salario es mayor al salario promedio de todos los departamentos
SELECT nombre, salario 
FROM empleados 
WHERE salario > (SELECT AVG(salario) FROM empleados);

-- 20. Obtener el nombre y el número total de proyectos en los que trabaja cada empleado
SELECT empleados.nombre, COUNT(asignacion_proyectos.proyecto_id) AS total_proyectos 
FROM empleados 
LEFT JOIN asignacion_proyectos ON empleados.id = asignacion_proyectos.empleado_id 
GROUP BY empleados.id;

-- 21. Calcular el porcentaje de empleados en cada departamento en comparación con el total de empleados
SELECT departamento, (COUNT(*) / (SELECT COUNT(*) FROM empleados) * 100) AS porcentaje_empleados 
FROM empleados 
GROUP BY departamento;

-- 22. Mostrar los 3 empleados con los salarios más altos en cada departamento
SELECT nombre, salario, departamento 
FROM (SELECT nombre, salario, departamento, 
             ROW_NUMBER() OVER (PARTITION BY departamento ORDER BY salario DESC) AS ranking 
      FROM empleados) AS subquery 
WHERE ranking <= 3;

-- 23. Identificar empleados cuyo salario no ha cambiado en los últimos 2 años
SELECT empleados.nombre, empleados.salario 
FROM empleados 
LEFT JOIN historial_salarios ON empleados.id = historial_salarios.empleado_id 
WHERE historial_salarios.fecha_cambio < DATE_SUB(CURDATE(), INTERVAL 2 YEAR) 
      OR historial_salarios.fecha_cambio IS NULL;

-- 24. Encontrar empleados asignados a proyectos en los que el departamento no coincide con su asignación original
SELECT empleados.nombre, proyectos.nombre AS proyecto, empleados.departamento 
FROM empleados 
JOIN asignacion_proyectos ON empleados.id = asignacion_proyectos.empleado_id 
JOIN proyectos ON asignacion_proyectos.proyecto_id = proyectos.id 
WHERE proyectos.departamento_id <> empleados.departamento_id;

-- 25. Mostrar el historial de proyectos completados en orden de duración, del más largo al más corto
SELECT nombre, DATEDIFF(fecha_fin, fecha_inicio) AS duracion_dias 
FROM proyectos 
WHERE fecha_fin IS NOT NULL 
ORDER BY duracion_dias DESC;
