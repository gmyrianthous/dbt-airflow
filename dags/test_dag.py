from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from dbt_airflow.core.task_group import DbtTaskGroup
from dbt_airflow.core.task import ExtraTask


with DAG(
    dag_id='test_dag',
    start_date=datetime(2023, 5, 16),
    catchup=False,
    tags=['example'],
    schedule_interval='0 6 * * *',
) as dag:
    t1 = DummyOperator(task_id='dummy_1')

    # extra_tasks = [
    #     ExtraTask(
    #         task_id='test_task',
    #         operator=PythonOperator,
    #         operator_args={
    #             'python_callable': lambda: print('Hello world'),
    #         },
    #         upstream_task_ids={
    #             'model.example_dbt_project.int_customers_per_store',
    #             'model.example_dbt_project.int_revenue_by_date'
    #         }
    #     ),
    #     ExtraTask(
    #         task_id='another_test_task',
    #         operator=PythonOperator,
    #         operator_args={
    #             'python_callable': lambda: print('Hello world 2!'),
    #         },
    #         upstream_task_ids={
    #             'test.example_dbt_project.int_customers_per_store',
    #         },
    #         downstream_task_ids={
    #             'snapshot.example_dbt_project.int_customers_per_store_snapshot',
    #         }
    #     ),
    #     ExtraTask(
    #         task_id='test_task_3',
    #         operator=PythonOperator,
    #         operator_args={
    #             'python_callable': lambda: print('Hello world 3!'),
    #         },
    #         downstream_task_ids={
    #             'snapshot.example_dbt_project.int_customers_per_store_snapshot',
    #         },
    #         upstream_task_ids={
    #             'model.example_dbt_project.int_revenue_by_date',
    #         },
    #     )
    # ]
    #
    # t1 = DummyOperator(task_id='dummy_1')
    # t2 = DummyOperator(task_id='dummy_2')
    #
    # tg = DbtTaskGroup(
    #     group_id='dbt-company',
    #     dbt_manifest_path=Path('/opt/airflow/example_dbt_project/target/manifest.json'),
    #     dbt_target='dev',
    #     dbt_project_path=Path('/opt/airflow/example_dbt_project/'),
    #     dbt_profile_path=Path('/opt/airflow/example_dbt_project/profiles'),
    #     extra_tasks=extra_tasks,
    #     create_sub_task_groups=True,
    # )
    #
    # t1 >> tg >> t2
