from airflow import DAG
from datetime import datetime

def print_hello():
    print('Hello World!')

# forma alternativa al WITH DAG
dag = DAG(
dag_id="mi_primer_dag",
start_date=datetime.now(),
schedule=None
)
