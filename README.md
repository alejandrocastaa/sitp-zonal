# SITP Zonal: bodega de datos, cubos OLAP y dashboard

Proyecto de aula de **Data Warehouse y Big Data** (Politécnico Grancolombiano, 2026). Construimos una bodega de datos dimensional con las validaciones diarias de tullave del componente zonal del SITP de Bogotá, la analizamos con consultas OLAP y la presentamos en un dashboard de gestión en Power BI.

## Datos

- **Fuente:** datos abiertos de TransMilenio, archivos diarios `ValidacionZonal` (https://storage.googleapis.com/validaciones_tmsa/ValidacionZonal.html).
- **Periodo cargado:** 14 días, del 21 de septiembre al 4 de octubre de 2026.
- **Volumen:** 22.175.277 validaciones cargadas.

## Modelo dimensional

Esquema en estrella (enfoque Kimball). El grano de la tabla de hechos es **una validación de tarjeta tullave**.

- **Hechos:** `fact_validaciones` (valor, saldo previo, saldo posterior, bandera de saldo incoherente). `numero_tarjeta` y `dispositivo` se manejan como dimensiones degeneradas.
- **Dimensiones:** `dim_tiempo` (fecha y hora), `dim_estacion`, `dim_ruta`, `dim_operador`, `dim_perfil`, `dim_vehiculo`, `dim_tarifa`.

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `01_ETL_SITP_Zonal.ipynb` | Notebook de Colab con la descarga, la limpieza, la carga a SQLite y las consultas OLAP, con sus salidas |
| `consultas_olap.sql` | Las 12 consultas OLAP (roll-up, drill-down, slice, dice y pivot) para las tres preguntas de BI |

## Cómo reproducir el ETL

1. Abrir `01_ETL_SITP_Zonal.ipynb` en Google Colab.
2. Ejecutar las celdas en orden. El notebook descarga los archivos diarios, los limpia y los carga en una base SQLite (`dw_sitp.db`).
3. El proceso es idempotente: si se interrumpe, se puede volver a ejecutar y retoma desde los días que no terminó.
4. La base resultante pesa varios GB y no se incluye en el repositorio.

El ETL registra los controles de calidad: conciliación de filas (leídas = cargadas + rechazadas + duplicadas), tabla de registros rechazados y verificación de integridad referencial.

## Preguntas de negocio

1. **Eficiencia operativa:** variación de la demanda por ruta, operador y franja horaria.
2. **Sostenibilidad financiera:** recaudo por operador y ruta, y proporción de perfiles subsidiados frente a tarifa plena.
3. **Equidad y acceso social:** distribución de validaciones por perfil tarifario y zona de operación.

## Notas sobre los datos

- El campo `Valor` es la tarifa nominal de la validación, por lo que el recaudo se presenta como recaudo nominal.
- La columna `Hora_Pico_SN` del origen trae siempre el mismo valor, así que la hora pico se definió a partir de la demanda observada (5 a 8 h y 16 a 19 h).
- Los datos no traen una zona de operación; se usa el operador como equivalente de zona.
- Entre el 8 % y el 12 % de las filas tienen saldos que no cuadran. Quedan marcadas con una bandera y no se descartan.

## Integrantes

- Alejandro Castaño (@alejandrocastaa)
- _Nombre integrante 2_
- _Nombre integrante 3_
