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
from datetime import datetime, timedelta
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
import logging

# ------- DECLARACIONES -----------
universidad_corto = 'UNComahue'
universidad_largo = 'Universidad Nacional del Comahue'

# ------- LOGGER ------------------
# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)     # DEBUG, INFO, WARNING, ERROR, CRITICAL
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s", "%Y-%m-%d')


@dag(
    dag_id=f'GB{universidad_corto}',  # dag_id='GBUNComahue',
    description=f'DAG para hacer ETL de la {universidad_largo}',
    tags=['Aldo', 'ETL'],
    start_date=datetime(2022, 11, 1),
    schedule=timedelta(hours=1),  # cada hora
    catchup=False,
    default_args={
        'retries': 5,  # If a task fails, it will retry 5 times.
        'retry_delay': timedelta(minutes=5),
        },
)
def GBUNComahue():
    """
    ### DAG ETL para la UNComahue
    For more information on Airflow's TaskFlow API, reference documentation here:
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
    """
    logging.info(f"Prueba de logger {universidad_largo}.")
    @task()
    def extract():
        """
        #### Extract task
        Los datos se obtendran con un PostgresHook que ejecute
        una consulta sql y guarde los resultados en un archivo csv.
        """
        return archivo_csv

    @task()
    def transform(archivo_csv):
        """
        #### Transform task
        El archivo csv obtenido en la Extraccion se procesara
        usando pandas con un PythonOperator que genere una salida txt.
        """
        return archivo_txt

    @task()
    def load(archivo_txt):
        """
        #### Load task
        El archivo txt obtenido en la Transformacion será subido a un
        repositorio S3 por medio de un operador desarrollado por la comunidad.
        """
        return


    archivo_csv = extract()
    archivo_txt = transform(archivo_csv)
    load(archivo_txt)

GBUNComahue = GBUNComahue()

