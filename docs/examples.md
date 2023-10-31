# Examples

## Populating a dbt project on Airflow, as a DAG
TODO: Write description

```python
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from dbt_airflow.core.config import DbtProjectConfig, DbtProfileConfig
from dbt_airflow.core.task_group import DbtTaskGroup


with DAG(
    dag_id='test_dag',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
  
    t1 = DummyOperator(task_id='dummy_1')
    t2 = DummyOperator(task_id='dummy_2')
    tg = DbtTaskGroup(
        group_id='dbt-company',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/path/to/dbt/project/dir'),
            manifest_path=Path('/path/to/target/manifest.json'),
        ),
        dbt_profile_config=DbtProfileConfig(
            profiles_path=Path('/path/to/dbt/project/profiles/dir'),
            target='dev',
        )
    )

    t1 >> tg >> t2
```

## Airflow DAG with dbt project and additional dependencies
TODO
