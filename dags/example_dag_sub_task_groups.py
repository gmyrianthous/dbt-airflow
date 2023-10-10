from pathlib import Path

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from dbt_airflow.core.task_group import DbtTaskGroup


@dag(schedule=None, tags=['example'])
def tutorial_dbt_airflow_basic():
    """
    This is an example Airflow DAG that represents a fundamental ELT pipeline where the
    transformation part is performed with `dbt-airflow`
    """
    extract = DummyOperator(task_id='extract')
    load = DummyOperator(task_id='load')
    transform = DbtTaskGroup(
        group_id='my-dbt-project',
        dbt_manifest_path=Path('example_dbt_project/target/manifest.json'),
        dbt_target='dev',
        dbt_project_path=Path('example_dbt_project/'),
        dbt_profile_path=Path('example_dbt_project/profiles'),
        create_sub_task_groups=True,
    )

    extract >> load >> transform


dag = tutorial_dbt_airflow_basic()