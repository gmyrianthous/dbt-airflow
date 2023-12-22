# Configuration

We are working hard to make `dbt-airflow` as generalised and customizable as possible, in order to 
enable different users and teams serve different use-cases. The following sections, provide all
the details you need in order to properly set the configuration up. 

Configuration is split into three categories and can be provided when creating a Task Group
using `DbtTaskGroup`: 
- dbt project configuration (`DbtProjectConfig`)
- dbt profile configuration (`DbtProfileConfig`)
- `dbt-airflow`-related configuration (`DbtAirflowConfig`)

## dbt project configuration
This type of configuration will help `dbt-airflow` determine the location of your dbt project and 
the manifest file. It needs to be constructed using `dbt_airflow.core.config.DbtProjectConfig`
and needs to be supplied via `dbt_project_config` argument in `DbtTaskGroup`. 

### Parameters

| argument        | required | type           | description                                                          |
|-----------------|----------|----------------|----------------------------------------------------------------------|
| `project_path`  | yes      | `pathlib.Path` | The path to the dbt project (equivalent to dbt `--project-dir` flag) |
| `manifest_path` | yes      | `pathlib.Path` | The path to the `manifest.json` file                                 |


### Example

```python
from pathlib import Path 

from dbt_airflow.core.config import DbtProjectConfig
from dbt_airflow.core.task_group import DbtTaskGroup


tg = DbtTaskGroup(
    ...,
    dbt_project_config=DbtProjectConfig(
        project_path=Path('/opt/airflow/example_dbt_project/'),
        manifest_path=Path('/opt/airflow/example_dbt_project/target/manifest.json'),
    ),
)
```

## dbt profile configuration
This type of configuration will help `dbt-airflow` determine the dbt profile details it requires
in order to run the dbt tasks on Airflow. It needs to be constructed using 
`dbt_airflow.core.config.DbtProfileConfig` and must be supplied via `dbt_profile_config` 
argument in `DbtTaskGroup`. 

### Parameters

| argument         | required | type           | description                                                             |
|------------------|----------|----------------|-------------------------------------------------------------------------|
| `profiles_path`  | yes      | `pathlib.Path` | The path to the profiles path (equivalent to dbt `--profiles-dir` flag) |
| `target`         | yes      | `str`          | The name of the target, as specified in `profiles.yml` file             |



### Example 

```python
from pathlib import Path 

from dbt_airflow.core.config import DbtProfileConfig
from dbt_airflow.core.task_group import DbtTaskGroup


tg = DbtTaskGroup(
    ...,
    dbt_profile_config=DbtProfileConfig(
        profiles_path=Path('/opt/airflow/example_dbt_project/profiles'),
        target='dev',
    ),
)
```

## `dbt-airflow` configuration
This is an optional type of configuration and consists of numerous different settings that can help
users customize the way they execute dbt projects on Airflow. It can be constructed using 
`dbt_airflow.core.config.DbtAirflowConfig` and should be supplied via `dbt_airflow_config` 
argument in `DbtTaskGroup`. 

### Parameters

| argument                         | type                | required | default                  | description                                                                                                                                                           |
|----------------------------------|---------------------|----------|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `create_sub_task_groups`         | `bool`              | no       | `True`                   | Boolean flag indicating whether sub TaskGroups will be created when rendering Airflow DAG. If `True`, the folder name of the dbt entity will be used as a `TaskGroup` | 
| `extra_tasks`                    | `list`              | no       | `None`                   | A list of `ExtraTask` objects, that can be used to introduce extra Airflow tasks within the rendered DAG.                                                             |
| `execution_operator`             | `ExecutionOperator` | no       | `ExecutionOperator.BASH` | The execution operator for the dbt Airflow Tasks.                                                                                                                     |
| `operator_kwargs`                | `dict`              | no       | `None`                   | A dictionary with additional params/vals to be passed to the `execution_operator`                                                                                     |
| `select`                         | `list`              | no       | `None`                   | Equivalent to `--select` flag of dbt CLI                                                                                                                              |
| `exclude`                        | `list`              | no       | `None`                   | Equivalent to `--exclude` flag of dbt CLI                                                                                                                             |
| `full_refresh`                   | `bool`              | no       | `False`                  | Equivalent to `--full-refresh` flag of dbt CLI                                                                                                                        |
| `no_write_json`                  | `bool`              | no       | `True`                   | Equivalent to `--no-write-json` flag of dbt CLI                                                                                                                       |
| `variables`                      | `str`               | no       | `None`                   | Equivalent to `--vars` flag of dbt CLI                                                                                                                                |
| `no_partial_parse`               | `bool`              | no       | `False`                  | Equivalent to `--no-partial-parse` flag of dbt CLI                                                                                                                    |
| `warn_error`                     | `bool`              | no       | `False`                  | Equivalent to `--warn-error` flag of dbt CLI                                                                                                                          |
| `warn_error_options`             | `str`               | no       | `None`                   | Equivalent to `--warn-error-options` flag of dbt CLI                                                                                                                  |
| `include_tags`                   | `list`              | no       | `None`                   | When specified, only dbt resources with these tags will be rendered on the Airflow DAG                                                                                |
| `exclude_tags`                   | `list`              | no       | `None`                   | When specified, dbt resources with these tags will not be rendered on the Airflow DAG                                                                                 |
| `model_tasks_operator_kwargs`    | `dict`              | no       | `None`                   | Operator Keyword arguments that will be supplied only for dbt model run tasks                                                                                         |
| `test_tasks_operator_kwargs`     | `dict`              | no       | `None`                   | Operator Keyword arguments that will be supplied only for dbt test tasks                                                                                              |
| `seed_tasks_operator_kwargs`     | `dict`              | no       | `None`                   | Operator Keyword arguments that will be supplied only for dbt seed tasks                                                                                              |
| `snapshot_tasks_operator_kwargs` | `dict`              | no       | `None`                   | Operator Keyword arguments that will be supplied only for dbt snapshot tasks                                                                                          |
