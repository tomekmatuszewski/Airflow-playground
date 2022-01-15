import json
import logging
from sqlite3 import Connection, Cursor
from typing import Tuple
import os
import pandas as pd
from requests import adapters, Session
import urllib3
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from dotenv import load_dotenv
from pathlib import Path
import sqlite3
import time

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
logger = logging.getLogger(__name__)

args = {
    'owner': 'tm',
    'start_date': days_ago(1)
}

# env vars
DATABASE = os.getenv('DATABASE')
CSV_PATH = os.getenv('CSV_PATH')
JSON_PATH = os.getenv("JSON_PATH")
URL = os.getenv("URL")
TOKEN = os.getenv("TOKEN")


dag = DAG(
    dag_id='spotify_job',
    default_args=args,
    description='spotify data downloader',
    schedule_interval=timedelta(days=1)
)


def retryable_session(session: Session, retries: int = 8) -> Session:
    retry = urllib3.util.retry.Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
    )

    adapter = adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_data() -> dict:
    session = retryable_session(Session())
    header = {"Authorization": f"Bearer {TOKEN}"}
    params = {"limit": 50, "after": 1484811043508}
    result = session.request('GET', url=URL, headers=header, params=params).json()
    return result


def parse_json(data: dict) -> pd.DataFrame:
    mapping_names = json.loads(open(BASE_DIR/'dags/mapping_col_names.json').read())
    df = pd.json_normalize(data)[mapping_names.keys()].rename(columns=mapping_names)
    df['url'] = df['artists_name'].apply(lambda col: col[0]['external_urls']['spotify'])
    df['artist_name'] = df['artists_name'].apply(lambda col: col[0]['name'])
    df['artist_id'] = df['artists_name'].apply(lambda col: col[0]['id'])
    df['time'] = int(time.time())
    df.drop(columns=["artists_name"], inplace=True)
    return df


def get_sqllite_session(db_name: str) -> Tuple[Connection, Cursor]:
    with sqlite3.connect(BASE_DIR / db_name) as conn:
        cur = conn.cursor()
        return conn, cur


def spotify_data_loader(**context) -> None:
    try:
        data = get_data()['items']
        tracks_df = parse_json(data)
        conn, cur = get_sqllite_session(context['db_name'])
        tracks_df.to_sql(context['table'], conn, if_exists='append', index=False)
        logger.info("Data loaded to sqlite database")
        context['ti'].xcom_push(key='table', value='tracks')

    except Exception as e:
        logger.error(f"Exception occured: {e}")


def csv_loader(db_name: str, table: str, destination_path: str) -> None:
    conn, cur = get_sqllite_session(db_name)
    df = pd.read_sql_query(f"SELECT * from {table}", conn)
    df.to_csv(path_or_buf=BASE_DIR / destination_path)


def create_flat_csv_job(**context) -> None:
    try:
        table = context.get("ti").xcom_pull(key='table')
        csv_loader(context['db_name'], table, context['destination_path'])
        logger.info("Data loaded to csv file")
    except Exception as e:
        logger.error(f"Exception occured: {e}")


def json_loader(db_name: str, table: str, destination_path: str) -> None:
    conn, cur = get_sqllite_session(db_name)
    df = pd.read_sql_query(f"SELECT * from {table}", conn)
    df.to_json(path_or_buf=BASE_DIR / destination_path, orient='records')


def create_flat_json_job(**context) -> None:
    try:
        table = context.get("ti").xcom_pull(key='table')
        json_loader(context['db_name'], table, context['destination_path'])
        logger.info("Data loaded to json file")
    except Exception as e:
        logger.error(f"Exception occured: {e}")


with dag:
    load_data_sqllite = PythonOperator(
        task_id='load_data_to_sqllite',
        python_callable=spotify_data_loader,
        retries=3,
        op_kwargs={'db_name': DATABASE, 'table': 'tracks'},
        provide_context=True

    )

    load_data_to_csv = PythonOperator(
        task_id='load_data_to_csv',
        python_callable=create_flat_csv_job,
        op_kwargs={'db_name': DATABASE, 'destination_path': CSV_PATH},
        provide_context=True
    )

    load_data_to_json = PythonOperator(
        task_id='load_data_to_json_job',
        python_callable=create_flat_json_job,
        op_kwargs={'db_name': DATABASE, 'destination_path': JSON_PATH},
        provide_context=True
    )

    load_data_sqllite >> [load_data_to_csv, load_data_to_json]

