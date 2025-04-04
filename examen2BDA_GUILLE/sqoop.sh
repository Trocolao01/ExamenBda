sqoop import \
--connect jdbc:mysql://localhost/empleados \
--username usuario \
--password clave \
--table empleados \
--target-dir /data/empleados \
--num-mappers 4

sqoop export 
 --connect jdbc:mysql://localhost/examenBDA 
 --username root 
 --password bdaPass 
 --table nombre_de_tabla 
 --export-dir /user/hadoop/resultados

sqoop export \
  --connect jdbc:mysql://VicenteBD:3306/examenBDA \
  --username root --password mi_contraseña \
  --table ventas \
  --export-dir /user/hadoop/ventas_data \
  --input-fields-terminated-by ','

sqoop export \
--connect jdbc:mysql://localhost/empleados \
--username usuario \
--password clave \
--table empleados \
--export-dir /data/empleados

sqoop export \
  --connect jdbc:mysql://VicenteBD:3306/empleados \
  --username root \
  --password root \
  --table empleados \
  --export-dir /ruta/a/mi/archivo_en_hdfs \
  --update-key id_columna \
  --input-fields-terminated-by ',' \
  --lines-terminated-by '\n'

sqoop export \
  --connect jdbc:mongodb://localhost:27017/mi_basededatos \
  --table mi_coleccion \
  --export-dir /ruta/a/mi/archivo_en_hdfs \
  --input-fields-terminated-by ',' \
  --lines-terminated-by '\n' \
  --mongo-url mongodb://localhost:27017 \
  --mongo-database mi_basededatos \
  --mongo-collection mi_coleccion




# 1. Importar empleados con salario mayor a 50000
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM empleados WHERE salario > 50000 AND \$CONDITIONS" \
--target-dir /data/empleados_salario --num-mappers 4

# 2. Importar clientes registrados después de 2020
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM clientes WHERE fecha_registro > '2020-01-01' AND \$CONDITIONS" \
--target-dir /data/clientes_nuevos --num-mappers 4

# 3. Importar ventas del último trimestre
sqoop import --connect jdbc:mysql://localhost/tienda --username user --password pass \
--query "SELECT * FROM ventas WHERE fecha >= '2023-10-01' AND \$CONDITIONS" \
--target-dir /data/ventas_Q4 --num-mappers 4

# 4. Importar productos con stock menor a 10
sqoop import --connect jdbc:mysql://localhost/inventario --username user --password pass \
--query "SELECT * FROM productos WHERE stock < 10 AND \$CONDITIONS" \
--target-dir /data/productos_bajo_stock --num-mappers 4

# 5. Importar pedidos con monto superior a 1000 dólares
sqoop import --connect jdbc:mysql://localhost/tienda --username user --password pass \
--query "SELECT * FROM pedidos WHERE monto_total > 1000 AND \$CONDITIONS" \
--target-dir /data/pedidos_grandes --num-mappers 4

# 6. Importar datos de empleados activos
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM empleados WHERE estado = 'activo' AND \$CONDITIONS" \
--target-dir /data/empleados_activos --num-mappers 4

# 7. Importar clientes de una región específica
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM clientes WHERE region = 'Norte' AND \$CONDITIONS" \
--target-dir /data/clientes_norte --num-mappers 4

# 8. Importar transacciones de enero 2024
sqoop import --connect jdbc:mysql://localhost/banco --username user --password pass \
--query "SELECT * FROM transacciones WHERE fecha >= '2024-01-01' AND fecha < '2024-02-01' AND \$CONDITIONS" \
--target-dir /data/transacciones_enero --num-mappers 4

# 9. Importar detalles de productos electrónicos
sqoop import --connect jdbc:mysql://localhost/tienda --username user --password pass \
--query "SELECT * FROM productos WHERE categoria = 'Electrónica' AND \$CONDITIONS" \
--target-dir /data/electronicos --num-mappers 4

# 10. Importar datos de facturación de los últimos 6 meses
sqoop import --connect jdbc:mysql://localhost/facturas --username user --password pass \
--query "SELECT * FROM facturacion WHERE fecha >= DATE_SUB(NOW(), INTERVAL 6 MONTH) AND \$CONDITIONS" \
--target-dir /data/facturacion_reciente --num-mappers 4

# 11. Importar órdenes de compra con estado 'Enviado'
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM ordenes WHERE estado = 'Enviado' AND \$CONDITIONS" \
--target-dir /data/ordenes_enviadas --num-mappers 4

# 12. Importar clientes que realizaron compras en los últimos 3 meses
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM clientes WHERE ultima_compra >= DATE_SUB(NOW(), INTERVAL 3 MONTH) AND \$CONDITIONS" \
--target-dir /data/clientes_compras_recientes --num-mappers 4

# 13. Importar empleados que trabajan en el departamento de TI
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM empleados WHERE departamento = 'TI' AND \$CONDITIONS" \
--target-dir /data/empleados_ti --num-mappers 4

# 14. Importar pagos realizados con tarjeta de crédito
sqoop import --connect jdbc:mysql://localhost/finanzas --username user --password pass \
--query "SELECT * FROM pagos WHERE metodo_pago = 'Tarjeta de Crédito' AND \$CONDITIONS" \
--target-dir /data/pagos_tarjeta --num-mappers 4

# 15. Importar clientes con más de 5 pedidos
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM clientes WHERE pedidos_totales > 5 AND \$CONDITIONS" \
--target-dir /data/clientes_frecuentes --num-mappers 4

# 16. Importar transacciones mayores a 5000 dólares
sqoop import --connect jdbc:mysql://localhost/banco --username user --password pass \
--query "SELECT * FROM transacciones WHERE monto > 5000 AND \$CONDITIONS" \
--target-dir /data/transacciones_altas --num-mappers 4

# 17. Importar productos con descuento superior al 20%
sqoop import --connect jdbc:mysql://localhost/tienda --username user --password pass \
--query "SELECT * FROM productos WHERE descuento > 20 AND \$CONDITIONS" \
--target-dir /data/productos_descuento --num-mappers 4

# 18. Importar compras realizadas en la tienda online
sqoop import --connect jdbc:mysql://localhost/tienda --username user --password pass \
--query "SELECT * FROM compras WHERE canal = 'Online' AND \$CONDITIONS" \
--target-dir /data/compras_online --num-mappers 4

# 19. Importar empleados que han recibido bonos
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM empleados WHERE bono > 0 AND \$CONDITIONS" \
--target-dir /data/empleados_bonos --num-mappers 4

# 20. Importar información de envíos internacionales
sqoop import --connect jdbc:mysql://localhost/logistica --username user --password pass \
--query "SELECT * FROM envios WHERE tipo = 'Internacional' AND \$CONDITIONS" \
--target-dir /data/envios_internacionales --num-mappers 4

# 21. Importar clientes con facturas impagas
sqoop import --connect jdbc:mysql://localhost/finanzas --username user --password pass \
--query "SELECT * FROM clientes WHERE facturas_pendientes > 0 AND \$CONDITIONS" \
--target-dir /data/clientes_impagos --num-mappers 4

# 22. Importar empleados con más de 5 años de experiencia
sqoop import --connect jdbc:mysql://localhost/empresa --username user --password pass \
--query "SELECT * FROM empleados WHERE experiencia >= 5 AND \$CONDITIONS" \
--target-dir /data/empleados_experiencia --num-mappers 4

# 23. Importar productos más vendidos
sqoop import --connect jdbc:mysql://localhost/tienda --username user --password pass \
--query "SELECT * FROM productos WHERE ventas_totales > 1000 AND \$CONDITIONS" \
--target-dir /data/productos_mas_vendidos --num-mappers 4

# 24. Importar registros de soporte con alta prioridad
sqoop import --connect jdbc:mysql://localhost/soporte --username user --password pass \
--query "SELECT * FROM tickets WHERE prioridad = 'Alta' AND \$CONDITIONS" \
--target-dir /data/tickets_alta_prioridad --num-mappers 4

# 25. Importar pagos pendientes de más de 30 días
sqoop import --connect jdbc:mysql://localhost/finanzas --username user --password pass \
--query "SELECT * FROM pagos WHERE estado = 'Pendiente' AND DATEDIFF(NOW(), fecha_pago) > 30 AND \$CONDITIONS" \
--target-dir /data/pagos_pendientes --num-mappers 4

# 26. Exportar empleados activos a MySQL
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table empleados --export-dir /data/empleados_activos

# 27. Exportar clientes de la región Norte
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table clientes --export-dir /data/clientes_norte

# 28. Exportar pedidos grandes
sqoop export --connect jdbc:mysql://localhost/tienda --username user --password pass \
--table pedidos --export-dir /data/pedidos_grandes

# 29. Exportar facturación reciente
sqoop export --connect jdbc:mysql://localhost/facturas --username user --password pass \
--table facturacion --export-dir /data/facturacion_reciente

# 30. Exportar productos electrónicos a MySQL
sqoop export --connect jdbc:mysql://localhost/tienda --username user --password pass \
--table productos --export-dir /data/electronicos

# 31. Exportar transacciones de enero a MySQL
sqoop export --connect jdbc:mysql://localhost/banco --username user --password pass \
--table transacciones --export-dir /data/transacciones_enero

# 32. Exportar ventas del cuarto trimestre (Q4)
sqoop export --connect jdbc:mysql://localhost/tienda --username user --password pass \
--table ventas --export-dir /data/ventas_Q4

# 33. Exportar empleados con salario mayor a 50000
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table empleados --export-dir /data/empleados_salario

# 34. Exportar clientes nuevos registrados después de 2020
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table clientes --export-dir /data/clientes_nuevos

# 35. Exportar productos con stock menor a 10
sqoop export --connect jdbc:mysql://localhost/inventario --username user --password pass \
--table productos --export-dir /data/productos_bajo_stock

# 36. Exportar datos de empleados activos
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table empleados --export-dir /data/empleados_activos

# 37. Exportar clientes de una región específica (Norte)
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table clientes --export-dir /data/clientes_norte

# 38. Exportar facturación de los últimos 6 meses
sqoop export --connect jdbc:mysql://localhost/facturas --username user --password pass \
--table facturacion --export-dir /data/facturacion_reciente

# 39. Exportar pedidos con monto mayor a 1000 dólares
sqoop export --connect jdbc:mysql://localhost/tienda --username user --password pass \
--table pedidos --export-dir /data/pedidos_grandes

# 40. Exportar datos de productos electrónicos
sqoop export --connect jdbc:mysql://localhost/tienda --username user --password pass \
--table productos --export-dir /data/electronicos

# 41. Exportar clientes que realizaron compras en los últimos 3 meses
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table clientes --export-dir /data/clientes_compras_recientes

# 42. Exportar empleados que trabajan en el departamento de TI
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table empleados --export-dir /data/empleados_ti

# 43. Exportar órdenes de compra con estado 'Enviado'
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table ordenes --export-dir /data/ordenes_enviadas

# 44. Exportar pagos realizados con tarjeta de crédito
sqoop export --connect jdbc:mysql://localhost/finanzas --username user --password pass \
--table pagos --export-dir /data/pagos_tarjeta

# 45. Exportar datos de clientes con más de 5 pedidos
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table clientes --export-dir /data/clientes_frecuentes

# 46. Exportar transacciones mayores a 5000 dólares
sqoop export --connect jdbc:mysql://localhost/banco --username user --password pass \
--table transacciones --export-dir /data/transacciones_altas

# 47. Exportar productos con descuento superior al 20%
sqoop export --connect jdbc:mysql://localhost/tienda --username user --password pass \
--table productos --export-dir /data/productos_descuento

# 48. Exportar compras realizadas en la tienda online
sqoop export --connect jdbc:mysql://localhost/tienda --username user --password pass \
--table compras --export-dir /data/compras_online

# 49. Exportar empleados que han recibido bonos
sqoop export --connect jdbc:mysql://localhost/empresa --username user --password pass \
--table empleados --export-dir /data/empleados_bonos

# 50. Exportar información de envíos internacionales
sqoop export --connect jdbc:mysql://localhost/logistica --username user --password pass \
--table envios --export-dir /data/envios_internacionales

