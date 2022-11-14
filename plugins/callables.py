"""
Funciones que pueden usarse dentro de los dags
"""
#from airflow import DAG
#from datetime import datetime, timedelta
#from airflow.operators.empty import EmptyOperator
#from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import logging
import logging.config
import pandas as pd

# ------- DECLARACIONES -----------
universidad_corto = 'UNComahue'
universidad_largo = 'Universidad Nacional del Comahue'

# ------- LOGGER ------------------
log_cfg = ('GB' + universidad_corto + '_log.cfg')
configfile = Path(__file__).parent.parent / 'plugins' / log_cfg
logging.config.fileConfig(configfile, disable_existing_loggers=False)
logger = logging.getLogger(__name__)

def datos_a_csv():
    logger.info('*** Comenzando Extraccion ***')
    # Ubicacion del .sql
    sql_path = Path(__file__).parent.parent / 'include' / ('GB' + universidad_corto + '.sql')
    # Leo el .sql
    sql_consulta = open(sql_path, 'r').read()
    # Conexion a la base
    hook = PostgresHook(postgres_conn_id='alkemy_db')
    conexion = hook.get_conn()
    df = pd.read_sql(sql_consulta, conexion)
    # Guardo .csv
    csv_path = Path.cwd() / 'files' / ('GB' + universidad_corto + '_select.csv')
    df.to_csv(csv_path, index=False)
    logger.info('*** Fin Extraccion ***')
    return

#---------------  transformacion  --------------
def csv_a_txt():
    return
    logger.info('*** Comenzando Transformacion ***')
    # Ubicacion del .csv
    csv_path = Path.cwd() / 'files' / ('GB' + universidad_corto + '_select.csv')
    # Leo el .csv
    df = pd.read_csv(csv_path)
    print(df)
    # 
    return
#------------------------------------------

if __name__ == '__main__':
    univ_list = ['GBUNComahue', 'USalvador']
    # ----- datos a csv --------
    #datos_a_csv()
    csv_a_txt()
    
    #transform_dataset(input_path = 'files/GGFLCienciasSociales_select.csv',
    #                    output_path = 'datasets/GGFLCienciasSociales_process.txt',
    #                    date_format='%d-%m-%Y')

    #transform_dataset(input_path = 'files/GGUKennedy_select.csv',
    #                    output_path = 'datasets/GGUKennedy_process.txt',
    #                    date_format='%y-%b-%d')
