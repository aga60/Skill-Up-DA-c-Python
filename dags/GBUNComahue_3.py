"""
## Grupo de Universidades B
## UNComahue

COMO: Analista de datos
QUIERO: Configurar los log
PARA: Mostrarlos en consola

Criterios de aceptacion: 

- Configurar logs para Univ. Nacional Del Comahue
- Configurar logs para Universidad del Salvador
- Utilizar la librería de Loggin de python: https://docs.python.org/3/howto/logging.html
- Realizar un log al empezar cada DAG con el nombre del logger
- Formato del log: %Y-%m-%d - nombre_logger - mensaje
Aclaracion:
Deben dejar la configuracion lista para que se pueda incluir dentro de las funciones futuras. 
No es necesario empezar a escribir logs.

# Dev: Aldo Agunin
# Fecha: 05/11/2022
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging

# ------- DECLARACIONES -----------
universidad_corto = "UNComahue"
universidad_largo = "Universidad Nacional del Comahue"

# ------- LOGGER ------------------
# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)     # DEBUG, INFO, WARNING, ERROR, CRITICAL
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s", "%Y-%m-%d')
# add formatter to console_handler
console_handler.setFormatter(formatter)
# add console_handler to logger
logger.addHandler(console_handler)


# def function_task_1():
#     print('Hello World!')

# forma alternativa al WITH DAG
# PENDIENTE Hacerlo con Taskflow (decoradores)
# https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
dag = DAG(
    dag_id=f"GB{universidad_corto}_3d",   # dag_id="GBUNComahue",
    description=f"DAG para hacer ETL de la {universidad_largo}",
    tags=['Aldo', 'ETL'],
    start_date=datetime(2022, 11, 1),
    schedule=timedelta(hours=1),  # cada hora
    catchup=False,
    default_args={
        "retries": 5, # If a task fails, it will retry 5 times.
        "retry_delay": timedelta(minutes=5),
        },
    )


# task1 = EmptyOperator(
#     task_id="task_1", # debe ser único dentro del dag
#     dag=dag # dag al que pertenece esta tarea
#     )

# task1 = PythonOperator(
#     task_id="task_1", # debe ser único dentro del dag
#     python_callable=function_task_1, # función que se ejecutará
#     dag=dag # dag al que pertenece esta tarea
#     )

# primera tarea: correr script .SQL y la salida a .CSV
# se usara un PythonOperator que ejecute la consulta .SQL de la Story1 y genere salida a .CSV
extract = EmptyOperator(
    task_id="extraction_task",
    dag=dag
    )

# segunda tarea: procesar datos en pandas
# se usara un PythonOperator
transform = EmptyOperator(
    task_id="transformation_task",
    dag=dag
    )

# tercera tarea: subir resultados a amazon s3
# se usara un PythonOperator
load = EmptyOperator(
    task_id="load_task",
    dag=dag
    )

extract >> transform >> load

