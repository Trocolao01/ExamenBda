20 Ejemplos de Subconsultas Difíciles en SQL

    Obtener los empleados con el salario más alto en cada departamento

SELECT * 
FROM employees e 
WHERE salary = (
  SELECT MAX(salary) 
  FROM employees 
  WHERE department_id = e.department_id
);

    Encontrar clientes que nunca han realizado un pedido

SELECT * 
FROM customers 
WHERE id NOT IN (
  SELECT customer_id 
  FROM orders
);

    Obtener productos cuyo precio está por encima del promedio de su categoría

SELECT * 
FROM products p 
WHERE price > (
  SELECT AVG(price) 
  FROM products 
  WHERE category_id = p.category_id
);

    Encontrar estudiantes que tienen la misma puntuación que el puntaje máximo en su curso

SELECT * 
FROM students s 
WHERE score = (
  SELECT MAX(score) 
  FROM students 
  WHERE course_id = s.course_id
);

    Listar departamentos con más empleados que el promedio de empleados por departamento

SELECT department_id 
FROM departments 
WHERE (
  SELECT COUNT(*) 
  FROM employees 
  WHERE department_id = departments.id
) > (
  SELECT AVG(emp_count) 
  FROM (
    SELECT COUNT(*) AS emp_count 
    FROM employees 
    GROUP BY department_id
  ) subquery
);

    Obtener los productos que se han vendido más de 10 veces

SELECT * 
FROM products 
WHERE id IN (
  SELECT product_id 
  FROM orders 
  GROUP BY product_id 
  HAVING COUNT(*) > 10
);

    Encontrar los empleados que ganan más que su gerente

SELECT e.name 
FROM employees e 
WHERE salary > (
  SELECT salary 
  FROM employees 
  WHERE id = e.manager_id
);

    Obtener el nombre del cliente con el pedido más caro

SELECT name 
FROM customers 
WHERE id = (
  SELECT customer_id 
  FROM orders 
  ORDER BY total_price DESC 
  LIMIT 1
);

    Listar productos que nunca se han vendido

SELECT * 
FROM products 
WHERE id NOT IN (
  SELECT DISTINCT product_id 
  FROM orders
);

    Encontrar las ciudades con más ventas totales que el promedio global

SELECT city 
FROM orders 
WHERE (
  SELECT SUM(amount) 
  FROM orders o2 
  WHERE o2.city = orders.city
) > (
  SELECT AVG(total_sales) 
  FROM (
    SELECT SUM(amount) AS total_sales 
    FROM orders 
    GROUP BY city
  ) subquery
);

    Mostrar departamentos que tienen más hombres que mujeres empleados

SELECT department_id 
FROM employees 
WHERE (
  SELECT COUNT(*) 
  FROM employees e 
  WHERE gender = 'Male' AND e.department_id = employees.department_id
) > (
  SELECT COUNT(*) 
  FROM employees e 
  WHERE gender = 'Female' AND e.department_id = employees.department_id
);

    Listar empleados que trabajan en oficinas en ciudades sin ventas

SELECT * 
FROM employees 
WHERE office_id IN (
  SELECT id 
  FROM offices 
  WHERE city NOT IN (
    SELECT DISTINCT city 
    FROM orders
  )
);

    Obtener el cliente con la mayor cantidad de pedidos realizados

SELECT * 
FROM customers 
WHERE id = (
  SELECT customer_id 
  FROM orders 
  GROUP BY customer_id 
  ORDER BY COUNT(*) DESC 
  LIMIT 1
);

    Listar empleados cuyo salario es mayor que el promedio de su departamento

SELECT * 
FROM employees e 
WHERE salary > (
  SELECT AVG(salary) 
  FROM employees 
  WHERE department_id = e.department_id
);

    Obtener las categorías con productos cuyo precio mínimo es mayor que el promedio global

SELECT category_id 
FROM products 
WHERE (
  SELECT MIN(price) 
  FROM products p 
  WHERE p.category_id = products.category_id
) > (
  SELECT AVG(price) 
  FROM products
);

    Encontrar empleados con el mayor número de subordinados

SELECT * 
FROM employees 
WHERE id = (
  SELECT manager_id 
  FROM employees 
  GROUP BY manager_id 
  ORDER BY COUNT(*) DESC 
  LIMIT 1
);

    Obtener los clientes con pedidos superiores al promedio de sus propias compras

SELECT customer_id 
FROM orders o1 
WHERE amount > (
  SELECT AVG(amount) 
  FROM orders o2 
  WHERE o2.customer_id = o1.customer_id
);

    Listar cursos donde ningún estudiante obtuvo una calificación reprobatoria

SELECT * 
FROM courses 
WHERE NOT EXISTS (
  SELECT * 
  FROM students 
  WHERE students.course_id = courses.id AND students.grade < 60
);

    Encontrar los empleados cuyo salario es mayor que el máximo salario de otro departamento

SELECT * 
FROM employees 
WHERE salary > (
  SELECT MAX(salary) 
  FROM employees 
  WHERE department_id <> employees.department_id
);

    Listar productos vendidos en todas las ciudades disponibles

SELECT product_id 
FROM orders 
GROUP BY product_id 
HAVING COUNT(DISTINCT city) = (
  SELECT COUNT(DISTINCT city) 
  FROM orders
);