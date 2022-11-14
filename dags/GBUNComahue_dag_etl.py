"""
Story 6
COMO: Analista de datos
QUIERO: Crear una función Python con Pandas para cada universidad
PARA: poder normalizar los datos de las mismas

Criterios de aceptación: 
Una funcion que devuelva un txt para cada una de las siguientes 
universidades con los datos normalizados:
- Univ. Nacional Del Comahue
- Universidad Del Salvador

Datos Finales:
- university: str minúsculas, sin espacios extras, ni guiones
- career: str minúsculas, sin espacios extras, ni guiones
- inscription_date: str %Y-%m-%d format
- first_name: str minúscula y sin espacios, ni guiones
- last_name: str minúscula y sin espacios, ni guiones
- gender: str choice(male, female)
- age: int
- postal_code: str
- location: str minúscula sin espacios extras, ni guiones
- email: str minúsculas, sin espacios extras, ni guiones

Aclaraciones:
Para calcular codigo postal o locación se va a utilizar el .csv que se encuentra en el repo.
La edad se debe calcular en todos los casos, resolver con criterio propio las fechas de nacimiento
 que no considere lógicas para inscribirse en la universidad. 
Resolver en caso de que suceda la transformación de dos dígitos del año.
En el caso de no contar con los datos de first_name y last_name por separado, colocar todo en last_name.

# Dev: Aldo Agunin
# Fecha: 11/11/2022
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import logging
import logging.config
import pandas as pd
from plugins.GBcallables import csv_a_txt

# ------- DECLARACIONES -----------
universidad_corto = 'UNComahue'
universidad_largo = 'Universidad Nacional del Comahue'

# ------- LOGGER ------------------
log_cfg = ('GB' + universidad_corto + '_log.cfg')
configfile = Path(__file__).parent.parent / 'plugins' / log_cfg
logging.config.fileConfig(configfile, disable_existing_loggers=False)
logger = logging.getLogger(__name__)

#---------------  extraccion  --------------
def datos_a_csv():
    logger.info('*** Comenzando Extracción ***')
    ## Ubicacion del .sql
    sql_path = Path(__file__).parent.parent / 'include' / ('GB' + universidad_corto + '.sql')
    ## Leo el .sql
    sql_consulta = open(sql_path, 'r').read()
    ## Conexion a la base
    hook = PostgresHook(postgres_conn_id='alkemy_db')
    conexion = hook.get_conn()
    df = pd.read_sql(sql_consulta, conexion)
    ## Guardo .csv
    csv_path = Path.cwd() / 'files' / ('GB' + universidad_corto + '_select.csv')
    df.to_csv(csv_path, index=False)
    logger.info('*** Fin Extraccion ***')
    return
#------------------------------------------

dag = DAG(
    dag_id=f'GB{universidad_corto}_dag_etl',   # dag_id='GBUNComahue_dag_etl',
    description=f'DAG para hacer ETL de la {universidad_largo}',
    tags=['Aldo', 'ETL'],
    start_date=datetime(2022, 11, 1),
    schedule=timedelta(hours=1),  # cada hora
    catchup=False,
    default_args={
        'retries': 5, # If a task fails, it will retry 5 times.
        'retry_delay': timedelta(minutes=5),
        },
    )

# primera tarea: correr script .SQL y la salida a .CSV
# se usara un PythonOperator que ejecute la consulta .SQL de la Story1 y genere salida a .CSV
# extract = EmptyOperator(
#     task_id="extraction_task",
#     dag=dag
#     )
extract = PythonOperator(
        task_id='extraction_task',
        python_callable=datos_a_csv,
        dag=dag,
    )

# segunda tarea: procesar datos en pandas
# se usara un PythonOperator que llame a un modulo externo
# transform = EmptyOperator(
#     task_id="transformation_task",
#     dag=dag
#     )
transform = PythonOperator(
        task_id='transformation_task',
        python_callable=csv_a_txt,
        dag=dag,
    )

# tercera tarea: subir resultados a amazon s3
# se usara un operador desarrollado por la comunidad
load = EmptyOperator(
    task_id="load_task",
    dag=dag
    )

extract >> transform >> load
