# ETL en Airflow
Trabajo final del curso Data Engineering de Coderhouse. 

## Objetivo
Crear un pipeline que extraiga datos diarios de una API pública y colocarlos en un Datawarehouse.

## Resumen
Se extrae información de la API de “Variables Principales” del BCRA que brinda información diaria de importantes indicadores económicos. Mediante un proceso ETL, se busca cargar diariamente esta información en una base de datos en Redshift. 

## Pipeline
Corresponde a un script en Python que levanta contenedores en Docker y permite ejecutar Airflow. El mismo esta compuesto por DAG´s con las siguientes tareas:
1. Extracción de datos de la API de “Variables principales” del BCRA.
2. Transformación de los datos; incluyendo, a modo de ejemplo, la anonimización de dos variables.
3. Conexión con base de datos en Redshift
4. Carga de datos en tabla “staging” en base de datos en Redshift
5. Envío de mail de notificación de la correcta carga de los datos

## Tablas en Redshift
Esta base de datos cuenta con las siguiente tablas:

**Calendario**

Tiene como objetivo agilizar los análisis para poder agrupar (en semanas, meses, trimestre y año) la información de variables principales del BCRA.
Este repositorio cuenta con el DDL de creación de la tabla y el scritp de Python el cual permite cargar la información.

**Stage_bcra_hash**

Tabla que se carga con información proveniente del proceso de ETL.
Este repositorio cuenta con el DDL de creación de la tabla.

**Bcra**

Tabla que se carga por medio de un procedimiento sql. El mismo trasforma información de la tabla ‘Stage_bcra_hash’ para asegurar el correcto formato de los campos y disponibilizar en producción los campos que fueron anonimizados. 
Este repositorio cuenta con el DDL de creación de la tabla y el código del procedimiento ‘MoveDataToProd’.


