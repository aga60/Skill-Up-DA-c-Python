"""
Funciones que pueden usarse dentro de los dags
"""
#from airflow import DAG
#from airflow.operators.empty import EmptyOperator
#from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import logging
import logging.config
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
# ------- DECLARACIONES -----------
universidad_corto = 'UNComahue'
universidad_largo = 'Universidad Nacional del Comahue'

# ------- LOGGER ------------------
log_cfg = ('GB' + universidad_corto + '_log.cfg')
configfile = Path(__file__).parent.parent / 'plugins' / log_cfg
logging.config.fileConfig(configfile, disable_existing_loggers=False)
logger = logging.getLogger(__name__)

# --- extraccion, solo está para probar, la que se usa está en el dag -------
def datos_a_csv_2():
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
def csv_a_txt(universidad_corto):
    logger.info('*** Comenzando Transformacion ***')
    # Ubicacion del .csv
    csv_path = Path.cwd() / 'files' / ('GB' + universidad_corto + '_select.csv')
    print(csv_path)

    # Leo el .csv
    # df = pd.read_csv(csv_path) # necesito forzar el tipo de cada columna
    df = pd.read_csv(
        csv_path,
        dtype={
            'university': str,
            'career': str,
            'inscription_date': str,
            'last_name': str,
            'first_name': str,
            'gender': str,
            'fecha_nacimiento': str,
            'age': pd.Int64Dtype(),
            'postal_code': str,
            'location': str,
            'email': str,
            },
        index_col=False,
        )
    print(df)

    # Normalizacion
    # - university: str minúsculas, sin espacios extras, ni guiones
    df['university'] = df['university'].str.lower().str.replace('_', ' ').str.strip(' -_')

    # - career: str minúsculas, sin espacios extras, ni guiones
    df['career'] = df['career'].str.lower().str.replace('_', ' ').str.strip(' -_')

    # - inscription_date: str %Y-%m-%d format
    # para UNComahue no es necesaria, ya viene en este formato
    df['inscription_date'] = pd.to_datetime(df['inscription_date'])
    
    # - last_name: elimino abreviaturas
    abrevs = ['MISS', 'III','PHD', 'DDS', 'DVM', 'MRS', 'II', 'IV',  'MR',  'JR', 'DR', 'MD', '.']
    for abrev in abrevs:
        df['last_name'] = df['last_name'].str.replace(abrev, '')

    # - last_name: str minúscula y sin espacios, ni guiones
    df['last_name'] = df['last_name'].str.lower().str.replace('_', ' ').str.strip(' -_')

    # separo first_name y last_name
    df[['first_name', 'last_name']] = df['last_name'].str.split(' ', 1, expand=True)

    # - first_name: str minúscula y sin espacios, ni guiones
    # ya quedó
    
    # - gender: str choice(male, female)
    df['gender'] = df['gender'].str.replace('F', 'female')
    df['gender'] = df['gender'].str.replace('M', 'male')
    
    # - email: str minúsculas, sin espacios extras, ni guiones
    df['email'] = df['email'].str.lower().str.replace('_', ' ').str.strip(' -_')
    
    # - postal_code: str
    # ya la tengo
    
    # - location: str minúscula sin espacios extras, ni guiones
    loc_path =  Path.cwd() / 'assets' / ('codigos_postales.csv')
    df_location = pd.read_csv(
        loc_path, 
        dtype={
            'codigo_postal': str, 
            'localidad': str
        },
        index_col=False
    )
    df_location.rename(
        columns={
            'codigo_postal': 'postal_code',
            'localidad': 'location'
        },
        inplace=True
    )
    df['postal_code']
    # hago un LEFT JOIN de df y df_location
    df = pd.merge(df, df_location, on='postal_code', how='left')
    df.rename(columns={'location_y': 'location'}, inplace=True)
    df['location'] = df['location'].str.lower().str.replace('_', ' ').str.strip(' -_')
    df = df.reindex(columns=[
        'university',
        'career',
        'inscription_date',
        'last_name',
        'first_name',
        'gender',
        'fecha_nacimiento',
        'age',
        'postal_code',
        'location',
        'email',
        ]
    )
        
    # - age: int
    #now = pd.Timestamp('now')
    df['_dob'] = pd.to_datetime(df['fecha_nacimiento'])
    df['_doi'] = pd.to_datetime(df['inscription_date'])
    df['_hoy'] = pd.to_datetime(datetime.today())
    df['_age_hoy'] = (df['_hoy'] - df['_dob']) / np.timedelta64(1, 'Y')
    df['_age_ins'] = (df['_doi'] - df['_dob']) / np.timedelta64(1, 'Y')
    df['_diff_hoy_ins'] = df['_age_hoy']-df['_age_ins']
    _age_hoy_mean = df['_age_hoy'].mean()
    _age_ins_mean = df['_age_ins'].mean()
    print('**0**_age_hoy_mean', _age_hoy_mean)
    print('**0**_age_ins_mean', _age_ins_mean)
    s = df['_age_hoy'].describe()
    print('**0**_age_hoy_describe', s)
    s = df['_age_ins'].describe()
    print('**0**_age_ins_describe', s)

    # verifico que (la edad al momento de la inscripcion sea 15 años o más)*.
    # (de acuerdo a datos del INDEC para el censo 2010 no hay población 
    # < 15 años de edad cursando estudios universitarios)
    # alternativa 1: en caso de que no (*) la reemplazo por NaN y recalculo la media.
    df['_age_ins_1'] = df['_age_ins']
    df.loc[df._age_ins_1 < 15, '_age_ins_1'] = np.NaN
    _age_ins_mean_1 = df['_age_ins_1'].mean()
    print('**1**_age_ins_mean_1', _age_ins_mean_1)
    s = df['_age_ins_1'].describe()
    print('**1**_age_ins_1_describe', s)
    # alternativa 2: en caso de que no (*) la reemplazo por la media y recalculo la media.
    df['_age_ins_2'] = df['_age_ins']
    df.loc[df._age_ins_2 < 15, '_age_ins_2'] = _age_ins_mean
    _age_ins_mean_2 = df['_age_ins_2'].mean()
    print('**2**_age_ins_mean_2', _age_ins_mean_2)
    s = df['_age_ins_2'].describe()
    print('**2**_age_ins_2_describe', s)
    # me quedo con la 2 aunque no hay una diferencia significativa
    # y convierto a int
    df['age'] = df['_age_ins_2'].astype(int)

    
    # Ubicacion del .txt PROVISORIO
    txt_path = Path.cwd() / 'datasets' / ('GB' + universidad_corto + '_PROVISORIO.txt')
    print(txt_path)
    
    # Escribo el .txt PROVISORIO
    df.to_csv(txt_path)
    
    # Elimino columnas no requeridas
    df.drop('fecha_nacimiento', axis=1, inplace=True)
    df.drop('_dob', axis=1, inplace=True)
    df.drop('_doi', axis=1, inplace=True)
    df.drop('_hoy', axis=1, inplace=True)
    df.drop('_age_hoy', axis=1, inplace=True)
    df.drop('_age_ins', axis=1, inplace=True)
    df.drop('_diff_hoy_ins', axis=1, inplace=True)
    df.drop('_age_ins_1', axis=1, inplace=True)
    df.drop('_age_ins_2', axis=1, inplace=True)
    df.drop('location_x', axis=1, inplace=True)

    # Ubicacion del .txt FINAL
    txt_path = Path.cwd() / 'datasets' / ('GB' + universidad_corto + '_process.txt')
    print(txt_path)
    
    # Escribo el .txt FINAL
    try:
        df.to_csv(txt_path, index=False)
        logger.info('*** Fin Transformacion ***')
    except:
        logger.error('*** NO SE PUDE GUARDAR TXT ***')
    # 
    return
#------------------------------------------

if __name__ == '__main__':
    # univ_list = ['GBUNComahue', 'USalvador']
    universidad_corto = 'UNComahue'
    # ----- datos a csv --------
    #datos_a_csv_2()
    csv_a_txt(universidad_corto)

