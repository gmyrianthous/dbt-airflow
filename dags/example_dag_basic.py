from pathlib import Path

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from dbt_airflow.core.task_group import DbtTaskGroup


@dag(schedule=None, tags=['example'], start_date=days_ago(1))
def tutorial_dbt_airflow_basic():
    """
    This is an example Airflow DAG that represents a fundamental ELT pipeline where the
    transformation part is performed with `dbt-airflow`.

    The creation of sub task-groups is disabled such that all Airflow Tasks (each of which
    corresponds to a single dbt resource type), will be under the main DbtTaskGroup named
    `my-dbt-project`.
    """
    extract = DummyOperator(task_id='extract')
    load = DummyOperator(task_id='load')
    transform = DbtTaskGroup(
        group_id='my-dbt-project',
        dbt_manifest_path=Path('/path/to/dbt/project/target/manifest.json'),
        dbt_target='dev',
        dbt_project_path=Path('/path/to/dbt/project/'),
        dbt_profile_path=Path('/path/to/dbt/project/profiles/dir'),
        create_sub_task_groups=False,
    )

    extract >> load >> transform


dag = tutorial_dbt_airflow_basic()
