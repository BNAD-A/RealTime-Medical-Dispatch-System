from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]  # .../Urgences/bigdata

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from prepare_structured_hopitaux import main as prepare_hopitaux  # noqa: E402


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bigdata_prepare_hopitaux_dag",
    default_args=default_args,
    description="Nettoyage des données brutes des hôpitaux",
    schedule_interval=("*/2 * * * *"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bigdata", "hopitaux"],
) as dag:

    prepare_hopitaux_task = PythonOperator(
        task_id="prepare_structured_hopitaux",
        python_callable=prepare_hopitaux,
    )
