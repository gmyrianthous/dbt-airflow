from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from dbt_airflow.dag_builder import DbtTaskGroup
from dbt_airflow.domain.model import ExtraAirflowTask


with DAG(
    dag_id='test_dag',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    extra_tasks = [
        ExtraAirflowTask(
            task_id='test_task',
            operator=PythonOperator,
            operator_args={
                'python_callable': lambda: print('Hello world'),
            },
        )
    ]

    t1 = DummyOperator(task_id='dummy_1')
    t2 = DummyOperator(task_id='dummy_2')

    tg = DbtTaskGroup(
        group_id='dbt-company',
        dbt_manifest_path=Path('/opt/airflow/example_dbt_project/target/manifest.json'),
        dbt_target='dev',
        dbt_project_path=Path('/opt/airflow/example_dbt_project/'),
        dbt_profile_path=Path('/opt/airflow/example_dbt_project/profiles'),
        extra_tasks=extra_tasks,
        create_task_groups=True,
    )

    t1 >> tg >> t2
