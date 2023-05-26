from pathlib import Path

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dbt_airflow.core.task_group import DbtTaskGroup
from dbt_airflow.core.task import ExtraTask


@dag(schedule=None, tags=['example'], start_date=days_ago(1))
def tutorial_dbt_airflow_extra_tasks():
    """
    This is an example Airflow DAG that represents a fundamental ELT pipeline where the
    transformation part is performed with `dbt-airflow`. Furthermore, additional Airflow tasks are
    also added in between specific dbt tasks.

    The creation of sub task-groups is disabled such that all Airflow Tasks (each of which
    corresponds to a single dbt resource type), will be under the main DbtTaskGroup named
    `my-dbt-project`.
    """
    extract = DummyOperator(task_id='extract')
    load = DummyOperator(task_id='load')

    extra_tasks = [
        ExtraTask(
            task_id='test_task',
            operator=PythonOperator,
            operator_args={
                'python_callable': lambda: print('Hello world'),
            },
            upstream_task_ids={
                'model.example_dbt_project.int_customers_per_store',
                'model.example_dbt_project.int_revenue_by_date'
            }
        ),
        ExtraTask(
            task_id='another_test_task',
            operator=PythonOperator,
            operator_args={
                'python_callable': lambda: print('Hello world 2!'),
            },
            upstream_task_ids={
                'test.example_dbt_project.int_customers_per_store',
            },
            downstream_task_ids={
                'snapshot.example_dbt_project.int_customers_per_store_snapshot',
            }
        ),
        ExtraTask(
            task_id='test_task_3',
            operator=PythonOperator,
            operator_args={
                'python_callable': lambda: print('Hello world 3!'),
            },
            downstream_task_ids={
                'snapshot.example_dbt_project.int_customers_per_store_snapshot',
            },
            upstream_task_ids={
                'model.example_dbt_project.int_revenue_by_date',
            },
        )
    ]

    transform = DbtTaskGroup(
        group_id='my-dbt-project',
        dbt_manifest_path=Path('/path/to/dbt/project/target/manifest.json'),
        dbt_target='dev',
        dbt_project_path=Path('/path/to/dbt/project/'),
        dbt_profile_path=Path('/path/to/dbt/project/profiles/dir'),
        create_sub_task_groups=False,
        extra_tasks=extra_tasks,
    )

    extract >> load >> transform


dag = tutorial_dbt_airflow_extra_tasks()
