import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from datetime import datetime

def cria_tabela_fonte_recursos():
    return """
CREATE TABLE IF NOT EXISTS fonte_recursos (
    id_fonte_recurso NUMERIC,
    nome_fonte_recurso VARCHAR(255),
    total_liquidado NUMERIC,
    total_arrecadado NUMERIC,
    dt_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def processa_csv(**kwargs):
    despesas_caminho = '/opt/airflow/dags/gdvDespesasExcel.csv'
    receitas_caminho = '/opt/airflow/dags/gdvReceitasExcel.csv'
    
    despesas = pd.read_csv(despesas_caminho, sep=None, engine='python', encoding='ISO-8859-1')
    despesas = despesas.iloc[:-1]
    despesas[['ID Fonte Recurso', 'Nome Fonte Recurso']] = despesas['Fonte de Recursos'].str.split(' - ', n=1, expand=True)
    despesas[['ID', 'Description']] = despesas['Despesa'].str.split(' - ', n=1, expand=True)
    despesas = despesas.rename(columns={'Liquidado': 'Total Liquidado'})
    despesas = despesas.drop(columns=['Fonte de Recursos', 'Despesa'])
    despesas['Total Liquidado'] = despesas['Total Liquidado'].str.replace('.', '').str.replace(',', '.').astype(float)
    
    despesas_agrupadas = despesas.groupby(['ID Fonte Recurso', 'Nome Fonte Recurso']).agg({'Total Liquidado': 'sum'}).reset_index()

    receitas = pd.read_csv(receitas_caminho, sep=None, engine='python', encoding='ISO-8859-1')
    receitas = receitas.iloc[:-1]
    receitas[['ID Fonte Recurso', 'Nome Fonte Recurso']] = receitas['Fonte de Recursos'].str.split(' - ', n=1, expand=True)
    receitas[['ID', 'Receita']] = receitas['Receita'].str.split(' - ', n=1, expand=True)
    receitas = receitas.drop(columns=['Fonte de Recursos'])
    receitas['Arrecadado'] = receitas['Arrecadado'].str.replace('.', '').str.replace(',', '.').astype(float)
    
    receitas_grouped = receitas.groupby(['ID Fonte Recurso', 'Nome Fonte Recurso']).agg({'Arrecadado': 'sum'}).reset_index()

    data_frame_combinado = pd.merge(despesas_agrupadas, receitas_grouped, on=['ID Fonte Recurso', 'Nome Fonte Recurso'], how='outer')
    data_frame_combinado = data_frame_combinado.fillna(0)

    kwargs['ti'].xcom_push(key='fonte_recursos_data', value=data_frame_combinado.to_dict('records'))

def alimenta_banco_de_dados(**kwargs):
    data = kwargs['ti'].xcom_pull(key='fonte_recursos_data', task_ids='processa_csv')
    df = pd.DataFrame(data)
    
    df.rename(columns={
        'ID Fonte Recurso': 'id_fonte_recurso',
        'Nome Fonte Recurso': 'nome_fonte_recurso',
        'Total Liquidado': 'total_liquidado',
        'Arrecadado': 'total_arrecadado'
    }, inplace=True)

    df['dt_insert'] = datetime.now()

    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    
    df.to_sql('fonte_recursos', engine, if_exists='append', index=False)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='alimenta_banco_recursos_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_table = PostgresOperator(
        task_id='cria_tabela_fonte_recursos',
        postgres_conn_id='postgres_default',
        sql=cria_tabela_fonte_recursos()
    )

    process_csv = PythonOperator(
        task_id='processa_csv',
        python_callable=processa_csv,
        provide_context=True
    )

    insert_data = PythonOperator(
        task_id='alimenta_banco_de_dados',
        python_callable=alimenta_banco_de_dados,
        provide_context=True
    )

    create_table >> process_csv >> insert_data
