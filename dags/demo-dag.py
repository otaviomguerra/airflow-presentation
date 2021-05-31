from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor

from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable

default_args = {
            "owner": "airflow",
            "start_date": datetime(2021, 1, 1),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "youremail@host.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }


def save_api_data(df):
    engine = create_engine(Variable.get('DATABASE_CONN'), echo=False)
    df.to_sql(
        name='Forex',
        con=engine,
        schema='public',
        if_exists='append',
        index=False,
        method='multi'
    )
    print('Dados salvos com sucesso!')


def get_api_data():
    r = requests.get(Variable.get('API_ENDPOINT'))
    json_response = r.json()
    rates_dict, base, date = json_response['rates'], json_response['base'], json_response['date']
    rates_dict.update({'base': base, 'date': date})
    rates_df = pd.DataFrame([rates_dict])
    print(rates_df.head())
    
    save_api_data(rates_df)


with DAG(dag_id="demo_dag", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    
    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available",
        method="GET",
        http_conn_id="forex_api",
        endpoint="/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
    )
    
    get_api_data_task = PythonOperator(
        task_id="get_api_data",
        python_callable=get_api_data
    )

    send_message_telegram_task = TelegramOperator(
    task_id='send_message_telegram',
    telegram_conn_id='telegram_conn_id',
    text='Dados disponiveis no banco!',
    dag=dag,
    )

    is_forex_rates_available >> get_api_data_task >> send_message_telegram_task

