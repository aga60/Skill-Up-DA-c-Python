"""
## Grupo de Universidades B
## USalvador

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
from pathlib import Path
import logging
import logging.config

# ------- DECLARACIONES ----------------------
universidad_corto = "USalvador"
universidad_largo = "Universidad del Salvador"

# ------- LOGGER -----------------------------
log_cfg = ('GB' + universidad_corto + '_log.cfg')
configfile = Path(__file__).parent.parent / 'plugins' / log_cfg
logging.config.fileConfig(configfile, disable_existing_loggers=False)
logger = logging.getLogger(__name__)

#---------------------------------------------
def prueba_log():
    logger.info('*** Comienzo de la Prueba de Logger ***')
    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warn message")
    logger.error("error message")
    logger.critical("critical message")
    logger.info('*** Fin de la Prueba de Logger ***')
#---------------------------------------------

# forma alternativa al WITH DAG
# PENDIENTE Hacerlo con Taskflow (decoradores)
# https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
dag = DAG(
    dag_id=f'GB{universidad_corto}_dag_etl',   # dag_id='GBUSalvador_dag_etl',
    description=f'DAG para hacer ETL de la {universidad_largo}',
    tags=['Aldo', 'ETL'],
    start_date=datetime(2022, 11, 1),
    schedule=timedelta(hours=1),  # cada hora
    catchup=False,
    default_args={
        'retries': 5, # If a task fails, it will retry 5 times.
        'retry_delay': timedelta(minutes=1),
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

# Provisoriamente agrego la tarea "begin" para probar el logger
begin = PythonOperator(
        task_id='begin',
        python_callable=prueba_log,
        dag=dag,
    )

# primera tarea: correr script .SQL y la salida a .CSV
# se usara un PythonOperator que ejecute la consulta .SQL de la Story1 y genere salida a .CSV
extract = EmptyOperator(
    task_id="extraction_task",
    dag=dag
    )

# segunda tarea: procesar datos en pandas
# se usara un PythonOperator que llame a un modulo externo
transform = EmptyOperator(
    task_id="transformation_task",
    dag=dag
    )

# tercera tarea: subir resultados a amazon s3
# se usara un operador desarrollado por la comunidad
load = EmptyOperator(
    task_id="load_task",
    dag=dag
    )

begin >> extract >> transform >> load
