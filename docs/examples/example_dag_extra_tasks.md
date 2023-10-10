# Example DAGs: Extra Tasks

This is an example DAG that will create two (dummy) tasks namely `extract` and `load`, 
followed by a `DbtTaskGroup` that renders a dbt project that: 
- Is located in `/path/to/dbt/project/` 
- Whose compiled manifest JSON file is found under `/path/to/dbt/project/target/manifest.json`
- Uses the profile specified (in `dbt_project.yml`) in `profiles.yml` file, that is located in `/path/to/dbt/project/profiles/dir`. 
- With `dev` target (that must be present in profile definition)

The `DbtTaskGroup` has disabled `create_sub_task_groups`, meaning that all individual dbt resources will be rendered
at the same level, under the `DbtTaskGroup` Airflow Task Group. 

## Extra Tasks
Additionally, we specify extra tasks by taking advantage of the `ExtraTask` class that can be used to insert
tasks in the dbt task dependency graph. 

More specifically, we specify three extra tasks:
- `test_task` will run a `PythonOperator` and the task itself will have two upstream dbt tasks, 
namely `model.example_dbt_project.int_customers_per_store` and `model.example_dbt_project.int_revenue_by_date`. This
the extra task will run once these two upstream dependencies are completed
- `another_test_task` is another `PythonOperator` that will be placed in between tasks
`test.example_dbt_project.int_customers_per_store` and `snapshot.example_dbt_project.int_customers_per_store_snapshot`
- `test_task_3` is also placed in between two tasks, namely `model.example_dbt_project.int_revenue_by_date`
and `snapshot.example_dbt_project.int_customers_per_store_snapshot`. 


## Code
```python
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
```

## How the DAG is rendered on Airflow

This is how the DAG will initially look like on Apache Airflow:

<img style="display: block; margin: 0 auto" src="../blob/examples/example_dag_extra_tasks/generic_view.png" alt="test">

When you click on the `my-dbt-project` TaskGroup, you will then be able to see all the dbt resources rendered as 
individual Airflow Tasks. 

<img style="display: block; margin: 0 auto" src="../blob/examples/example_dag_extra_tasks/detailed_view.png" alt="test">

