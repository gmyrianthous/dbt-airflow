from datetime import datetime
from pathlib import Path

from airflow import DAG

from dbt_airflow.dag_builder import build_dag


with DAG(
    dag_id='test_dag',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    build_dag(
        dbt_manifest_path=Path('/opt/airflow/example_dbt_project/target/manifest.json'),
        dbt_target='dev',
        dbt_project_path=Path('/opt/airflow/example_dbt_project/'),
        dbt_profile_path=Path('/opt/airflow/example_dbt_project/profiles'),
    )
