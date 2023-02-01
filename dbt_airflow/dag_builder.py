from pathlib import Path
from typing import Dict, Optional

from airflow.utils.task_group import TaskGroup

from dbt_airflow.domain.model import TaskList
from dbt_airflow.domain.task_builder import DbtAirflowTaskBuilder


def build_airflow_tasks(
    tasks: TaskList,
    dbt_target: str,
    dbt_profile_path: Path,
    dbt_project_path: Path,
    task_groups: Dict[str, TaskGroup],
) -> None:
    dbt_tasks = {
        task.task_id: task.dbt_operator(
            task_id=task.task_id,
            dbt_target_profile=dbt_target,
            dbt_profile_path=dbt_profile_path,
            dbt_project_path=dbt_project_path,
            resource_name=task.model_name,
            task_group=task_groups.get(task.task_group),
        )
        for task in tasks
    }

    for task in tasks:
        for upstream_task in task.upstream_task_ids:
            dbt_tasks[task.task_id] << dbt_tasks[upstream_task]


def build_airflow_task_groups(tasks: TaskList) -> Dict[str, TaskGroup]:
    """

    """
    task_groups = {}
    for task in tasks:
        if task.task_group and task.task_group not in task_groups:
            task_groups[task.task_group] = TaskGroup(task.task_group)
    return task_groups


def build_dag(
    dbt_manifest_path: Path,
    dbt_target: str,
    dbt_profile_path: Path,
    dbt_project_path: Path,
    create_task_groups: Optional[bool] = True,
) -> None:
    task_builder = DbtAirflowTaskBuilder(dbt_manifest_path.as_posix())
    tasks = task_builder.build_tasks()

    airflow_task_groups = {}
    if create_task_groups:
        airflow_task_groups = build_airflow_task_groups(tasks)

    build_airflow_tasks(
        tasks=tasks,
        dbt_target=dbt_target,
        dbt_profile_path=dbt_profile_path,
        dbt_project_path=dbt_project_path,
        task_groups=airflow_task_groups,
    )
