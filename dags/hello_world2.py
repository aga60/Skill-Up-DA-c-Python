from datetime import datetime
from airflow import DAG
# from airflow.operators.python_operator import PythonOperator # deprecated
from airflow.operators.python import PythonOperator

def print_hello():
    print('Hello World!')

dag = DAG('hello_world2', description='Hola Mundo DAG',
        schedule='* * * * *',
        # schedule_interval='* * * * *',   # deprecated
        start_date=datetime(2021, 10, 20),
        catchup=False,)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

# orden en que se llama a los operadores si hay más de uno
hello_operator
