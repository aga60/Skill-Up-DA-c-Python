from airflow import DAG
from datetime import datetime

# ahora tiene una funcion
# y la definición de un DAG
# y corre, pero realiza tareas?

def print_hello():
    print('Hello World!')

# forma alternativa al WITH DAG
dag = DAG(
dag_id="otro_dag_de_prueba1",
start_date=datetime.now(),
schedule=None
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

