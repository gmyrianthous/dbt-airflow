# Running dbt-airflow using different operators
`dbt-airflow` currently supports two Airflow Operators:
- `BashOperator` (default)
- `KubernetesPodOperator`

The package will automatically render your project as an Airflow DAG consisting of Airflow Tasks
for each dbt resource. The constructed Airflow Tasks will use the corresponding operator based on 
your selection for `execution_operator` that can be provided as part of the `DbtAirflowConfig` 
object. Furthermore, you can also optionally provide further operator-specific arguments through the 
`operator_kwargs` argument. 

## Running dbt projects with `BashOperator`
This is the default execution operator and every dbt resource will be rendered on the Airflow DAG
as a `BashOperator`.

When using `ExecutionOperator.BASH` it is assumed that `dbt-core` (and the corresponding dbt 
adapter) are installed already on Airflow. Sometimes this is not possible given that there's a 
high chance of hitting the wall of conflicting package versions. If this is the case, then you need
to consider building your own image with your dbt project and the relevant dependencies, and run
`dbt-airflow` using `ExecutionOperator.KUBERNETES`. 

### Example DAG with `BashOperator`

```python3
from datetime import datetime
from pathlib import Path

from airflow import DAG

from dbt_airflow.core.config import DbtAirflowConfig, DbtProjectConfig, DbtProfileConfig
from dbt_airflow.core.task_group import DbtTaskGroup
from dbt_airflow.operators.execution import ExecutionOperator


with DAG(
    dag_id='test_dag',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    tg = DbtTaskGroup(
        group_id='dbt-company',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/opt/airflow/example_dbt_project/'),
            manifest_path=Path('/opt/airflow/example_dbt_project/target/manifest.json'),
        ),
        dbt_profile_config=DbtProfileConfig(
            profiles_path=Path('/opt/airflow/example_dbt_project/profiles'),
            target='dev',
        ),
        dbt_airflow_config=DbtAirflowConfig(
            execution_operator=ExecutionOperator.BASH,
        ),
    )
```

## Running dbt projects with `KubernetesPodOperator`
An alternative way to render dbt projects on Airflow via `dbt-airflow` is the use of 
`KubernetesPodOperator`. This execution operator, assumes that you have previously created a Docker
Image consisting of your dbt project as well as the relevant dependencies (including `dbt-core` and
dbt adapter). 

In order to run your project in this mode you will have to specify 
`execution_operator=ExecutionOperator.BASH` in `DbtAirflowConfig` and specify required and optional
arguments for `KubernetesPodOperator` via `operator_kwargs` argument. 


### Example DAG with `KubernetesPodOperator`

```python3
from datetime import datetime
from pathlib import Path

from airflow import DAG

from dbt_airflow.core.config import DbtAirflowConfig, DbtProjectConfig, DbtProfileConfig
from dbt_airflow.core.task_group import DbtTaskGroup
from dbt_airflow.operators.execution import ExecutionOperator


with DAG(
    dag_id='test_dag',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    tg = DbtTaskGroup(
        group_id='dbt-company',
        dbt_project_config=DbtProjectConfig(
            project_path=Path('/opt/airflow/example_dbt_project/'),
            manifest_path=Path('/opt/airflow/example_dbt_project/target/manifest.json'),
        ),
        dbt_profile_config=DbtProfileConfig(
            profiles_path=Path('/opt/airflow/example_dbt_project/profiles'),
            target='dev',
        ),
        dbt_airflow_config=DbtAirflowConfig(
            execution_operator=ExecutionOperator.KUBERNETES,
            operator_kwargs={
                'name': f'dbt-company',
                'namespace': 'composer-user-workloads',
                'image': 'ghcr.io/dbt-labs/dbt-bigquery:1.7.2',
                'kubernetes_conn_id': 'kubernetes_default',
                'config_file': '/home/airflow/composer_kube_config',
                'image_pull_policy': 'Always',
            },
        ),
    )
```
