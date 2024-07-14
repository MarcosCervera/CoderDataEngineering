from datetime import timedelta,datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import os
import logging
from psycopg2.extras import execute_values

load_dotenv() 

dag_path = os.getcwd() 

### levanta tabla calendario ###
df = pd.read_csv(os.path.join(dag_path, 'raw_data' ,'calendario.csv') , sep = ';' , encoding= 'unicode_escape')


### Conexión Redshift ###

url = os.getenv("REDSHIFT_URL")
user = os.getenv("REDSHIFT_USER")
pwd = os.getenv("REDSHIFT_PWD")
data_base = os.getenv("REDSHIFT_DB")

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
try:
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')
    print(conn)
    print("Conexión exitosa a Redshift!")
except Exception as e:
    print("Falló conexión a Redshift.")
    print(e)


### Cargar datos ###

url=os.getenv("REDSHIFT_URL")
conn = psycopg2.connect(
    host=url,
    dbname=redshift_conn["database"],
    user=redshift_conn["username"],
    password=redshift_conn["pwd"],
    port='5439')

# Definir columnas
columns= ['fecha','anio','mes','dia','nombre_mes','nombre_dia','trimestre','es_dia_habil','ultimo_dia_mes']

cur = conn.cursor()
table_name = 'calendario'
columns = columns
# Generate 
values = [tuple(x) for x in df.to_numpy()]
insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

cur.execute("BEGIN")
execute_values(cur, insert_sql, values)
cur.execute("COMMIT")      