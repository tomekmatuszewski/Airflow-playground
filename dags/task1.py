import json
import logging
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

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
URL = os.getenv("URL")
TOKEN = os.getenv("TOKEN")
logger = logging.getLogger(__name__)

args = {
    'owner': 'tm',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='spotify_job',
    default_args=args,
    description="spotify data downloader",
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


def get_data():
    session = retryable_session(Session())
    header = {"Authorization": f"Bearer {TOKEN}"}
    params = {"limit": 50, "after": 1484811043508}
    result = session.request('GET', url=URL, headers=header, params=params).json()
    return result


def parse_json(data: dict) -> pd.DataFrame:
    mapping_names = {'played_at': 'played_at',
            'track.album.artists': 'artists_name',
            'track.album.id': 'album_id',
            'track.album.name': 'album_name',
            'track.album.release_date': 'release_date',
            'track.name': 'name',
            'track.popularity': 'popularity'}
    df = pd.json_normalize(data)[mapping_names.keys()].rename(columns=mapping_names)
    df['url'] = df['artists_name'].apply(lambda col: col[0]['external_urls']['spotify'])
    df['artist_name'] = df['artists_name'].apply(lambda col: col[0]['name'])
    df['artist_id'] = df['artists_name'].apply(lambda col: col[0]['id'])
    df.drop(columns=["artists_name"], inplace=True)
    return df


def spotify_data_loader() -> None:
    data = get_data()['items']
    tracks_df = parse_json(data)
    print(tracks_df)
    with sqlite3.connect(BASE_DIR / "spotify.db") as conn:
        cur = conn.cursor()
        tracks_df.to_sql("tracks", conn, if_exists='append', index=False)
        cur.execute("SELECT * from tracks")
        logger.info(cur.fetchall())


def create_flat_csv_job():
    logger.info("2ND TASK FINISHED")


with dag:
    run_this_task = PythonOperator(
        task_id='load_data_to_sqllite',
        python_callable=spotify_data_loader
    )

    run_this_task_too = PythonOperator(
        task_id='print_final_statement',
        python_callable=create_flat_csv_job
    )

    run_this_task >> run_this_task_too
