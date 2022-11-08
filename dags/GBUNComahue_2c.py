"""
## Grupo de Universidades B
## UNComahue

COMO: Analista de datos
QUIERO: Configurar un DAG, sin consultas, ni procesamiento
PARA: Hacer un ETL para 2 universidades distintas.

Criterios de aceptacion:
Configurar el DAG para procese las siguientes universidades:
- Univ. Nacional Del Comahue
- Universidad Del Salvador
Documentar los operators que se deberian utilizar a futuro,
teniendo en cuenta que se va a hacer dos consultas SQL (una para cada universidad),
 se van a procesar los datos con pandas y se van a cargar los datos en S3.  
El DAG se debe ejecutar cada 1 hora, todos los dias y cada tarea se debe ejecutar
5 veces antes de fallar.

# Dev: Aldo Agunin
# Fecha: 05/11/2022
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API

# ------- DECLARACIONES -----------
universidad_corto = 'UNComahue'
universidad_largo = 'Universidad Nacional del Comahue'

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
        return

    archivo_csv = extract()
    archivo_txt = transform(archivo_csv)
    load(archivo_txt)

GBUNComahue = GBUNComahue()

