import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

def get_api_awesome(**kwargs):
    api_url = "https://economia.awesomeapi.com.br/json/daily/USD-BRL/?start_date=20220622&end_date=20220622"
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        kwargs['ti'].xcom_push(key='api_data', value=data)
    else:
        raise Exception(f"Erro ao obter dados da API: {response.status_code}")

def alimenta_api_no_banco(**kwargs):
    data = kwargs['ti'].xcom_pull(key='api_data', task_ids='get_api_awesome')
    df = pd.DataFrame(data)
    df.rename(columns={
        'code': 'codigo',
        'codein': 'codigo_in',
        'name': 'nome',
        'high': 'alto',
        'low': 'baixo',
        'varBid': 'variacao_lance',
        'pctChange': 'mudanca_pct',
        'bid': 'lance',
        'ask': 'pedido',
        'timestamp': 'timestamp',
        'create_date': 'data_criacao'
    }, inplace=True)
    df['alto'] = df['alto'].astype(float)
    df['baixo'] = df['baixo'].astype(float)
    df['variacao_lance'] = df['variacao_lance'].astype(float)
    df['mudanca_pct'] = df['mudanca_pct'].astype(float)
    df['lance'] = df['lance'].astype(float)
    df['pedido'] = df['pedido'].astype(float)
    df['timestamp'] = df['timestamp'].astype(int)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    df.to_sql('cambio_do_dia', engine, if_exists='append', index=False)

def cria_tabela_de_cambio():
    return """
    CREATE TABLE IF NOT EXISTS cambio_do_dia (
    codigo VARCHAR(3),
    codigo_in VARCHAR(3),
    nome VARCHAR(50),
    alto NUMERIC,
    baixo NUMERIC,
    variacao_lance NUMERIC,
    mudanca_pct NUMERIC,
    lance NUMERIC,
    pedido NUMERIC,
    timestamp BIGINT,
    data_criacao TIMESTAMP
    );
    """

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='api_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_table = PostgresOperator(
        task_id='cria_tabela_de_cambio',
        postgres_conn_id='postgres_default',
        sql=cria_tabela_de_cambio()
    )

    get_data = PythonOperator(
        task_id='get_api_awesome',
        python_callable=get_api_awesome,
        provide_context=True
    )

    insert_data = PythonOperator(
        task_id='alimenta_api_no_banco',
        python_callable=alimenta_api_no_banco,
        provide_context=True
    )

    create_table >> get_data >> insert_data
