import logging
from requests import adapters, Session
import urllib3
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

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

URL = 'https://www.carboninterface.com/api/v1/estimates'


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


def get_data(url):
    session = retryable_session(Session())
    header = {"Authorization": "Bearer FvI0uUQAuqsAMprrwqTfmg"}
    body = {
        "type": "flight",
        "passengers": 2,
        "legs": [
            {"departure_airport": "KRK", "destination_airport": "WAW"},
            {"departure_airport": "JFK", "destination_airport": "WAW"}
        ]
    }
    result = session.request('POST', url=url, headers=header, json=body).text
    print(result)


def spotify_data_loader():
    get_data(URL)


def create_flat_csv_job():
    print('END OF TASKS')


with dag:
    run_this_task = PythonOperator(
        task_id='run_this_first',
        python_callable=spotify_data_loader
    )

    run_this_task_too = PythonOperator(
        task_id='run_this_last',
        python_callable=create_flat_csv_job
    )

    run_this_task >> run_this_task_too
