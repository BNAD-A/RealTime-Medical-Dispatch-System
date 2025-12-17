from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from prepare_structured_dispatch import main as prepare_dispatch  

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bigdata_prepare_dispatch_dag",
    default_args=default_args,
    description="Nettoyage des données brutes de dispatch",
    schedule_interval="*/2 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bigdata", "dispatch"],
) as dag:

    prepare_dispatch_task = PythonOperator(
        task_id="prepare_structured_dispatch",
        python_callable=prepare_dispatch,
    )
