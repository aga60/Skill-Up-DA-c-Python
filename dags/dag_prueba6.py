from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ahora tiene la definición de un DAG
# y una función
# y la definición de dos tareas, operadores, se agrega los import correspondientes
# y corre, y realiza bien la tarea python, empty no hace nada
# tags=['example'] para que lo muestre en Airflow, en la configuración de Airflow hay un switch para mostrar o no los 'example'
# schedule para repetir la corrida

def function_task_1():
    print('Hello World!')

def function_task_2():
    print('Hola Mundo!')

# forma alternativa al WITH DAG
dag = DAG(
	tags=['example'],
	dag_id="dag_prueba6",
	start_date=datetime.now(),
	schedule=timedelta(hours=2) # cada dos horas
	)

task1 = PythonOperator(
    task_id="task_1", # debe ser único dentro del dag
    python_callable=function_task_1, # función que se ejecutará
    dag=dag # se identifica el dag al cual pertenece esta tarea
    )

task2 = PythonOperator(
    task_id="task_2", # debe ser único dentro del dag
    python_callable=function_task_2, # función que se ejecutará
    dag=dag # se identifica el dag al cual pertenece esta tarea
    )

task3 = EmptyOperator(
    task_id="task_3",
    dag=dag # se identifica el dag al cual pertenece esta tarea
    )

# with DAG(
#     dag_id="mi_primer_dag",
#     start_date=datetime.now(),
#     schedule=None
#     ) as dag:

# # forma alternativa al WITH DAG
# dag = DAG(
# dag_id="mi_primer_dag",
# start_date=datetime.now(),
# schedule=None
# )

# # schedule
# # en casos muy generales
# "@daily" # todos los días a las 00:00
# "@hourly" # cada hora a los 00 minutos

# # con necesidades más específicas de minutos, horas y/o días
# "0 15 * * *" # todos los días a las 3
# "0 15,17 * * *" # todos los días a las 3 y a las 5
# "0 15-17 * * *" # todos los días, cada hora desde las 3 hasta las 5
# "0 15 * * SAT" # todos los sábados a las 3
# "0 15 * * SAT,MON" # todos los sábados y lunes a las 3
# "0 15 1 * *" # todos los primeros de cada mes a las 3

# # para casos menos comunes y más particulares
# import datetime as dt
# dt.timedelta(days=2) # cada dos días
# dt.timedelta(hours=5) # cada cinco horas

# dentro del DAG van Tareas, de 2 tipos operadores o sensores
# operadores:  PythonOperator, BashOperator, BranchPythonOperator y DummyOperator.

# Ejemplo de PythonOperator
# from airflow.operators.python_operator import PythonOperator
# def function_task_1():
#     ...
# task1 = PythonOperator(
#     task_id="task_1", # debe ser único dentro del dag
#     python_callable=function_task_1, # función que se ejecutará
#     dag=dag # se identifica el dag al cual pertenece esta tarea
# )

# Ejemplo de BashOperator
# Con este operador es posible ejecutar comandos en la terminal
# from airflow.operators.bash import BashOperator
# task2 = BashOperator(
#     task_id="task_2", # debe ser único dentro del dag
#     bash_command=(
#         "mkdir -p /data && " # se crea el directorio data
#         "curl -o /data/events.json " # se descarga en el archivo json,
#         "https:/ /localhost:5000/events" # el resultado de la petición
#     )
#     dag=dag # se identifica el dag al cual pertenece esta tarea
# )

# Ejemplo de DummyOperator (no hace nada, sirve de conector)
# from airflow.operators.dummy import DummyOperator
# task3 = DummyOperator(
#     task_id="task_3",
#     dag=dag # se identifica el dag al cual pertenece esta tarea
# )

# Ejemplo de BranchPythonOperator (dependencias, bifurcaciones)
# from airflow.operators.python import BranchPythonOperator
# def function_task_1():
#     if condition_1:
#         ...
#         return "task_2"
#     elif condition_2:
#         ...
#         return "task_3"
#     else:
#         ...
#         return ["task_2", "task_3"]

# task1 = PythonOperator(
#     task_id="task_1", # debe ser único dentro del dag
#     python_callable=function_task_1, # función que se ejecutará
#     dag=dag # se identifica el dag al cual pertenece esta tarea
# )

# task1 >> [task2, task3]

