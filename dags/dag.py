from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
from  etl_bcra import extraer_data, transformar_data, conexion_redshift, cargar_data , enviar_email



# argumentos por defecto para el DAG
default_args = {
    'owner': 'marcos',
    'start_date': datetime(2024,7,13),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='etl_bcra',
    default_args=default_args,
    description='Agrega data de forma diaria de BCRA',
    schedule_interval="@daily",
    catchup=False
)


### Tareas ###
#1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# 3. Envio de data 
# 3.1 Conexion a base de datos
task_31= PythonOperator(
    task_id="conexion_redshift",
    python_callable=conexion_redshift,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag
)

# 3.2 Envio final
task_32 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

task_4 = PythonOperator(
    task_id='send_mail',
    python_callable=enviar_email,
    dag=BC_dag,
)

# Definicion orden de tareas
task_1 >> task_2 >> task_31 >> task_32 >> task_4  