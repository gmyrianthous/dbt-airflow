from pathlib import Path
from typing import Dict

from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup

from dbt_airflow.tasks import TaskList


def build_airflow_tasks(
    tasks: TaskList,
    dbt_target: str,
    dbt_profile_path: Path,
    dbt_project_path: Path,
    task_groups: Dict[str, TaskGroup],
) -> None:
    dbt_tasks = {
        task.name: BashOperator(
            task_id=task.name,
            bash_command=construct_dbt_command(
                dbt_command=task.dbt_command,
                dbt_target=dbt_target,
                dbt_profile_path=dbt_profile_path,
                dbt_project_path=dbt_project_path,
                model_name=task.model_name,
            ),
            task_group=task_groups.get(task.task_group),
        )
        for task in tasks
    }

    for task in tasks:
        for upstream_task in task.upstream_tasks:
            dbt_tasks[task.name] << dbt_tasks[upstream_task]


def build_airflow_task_groups(tasks: TaskList) -> Dict[str, TaskGroup]:
    """
    :param tasks:
    :return:
    """
    task_groups = {}
    for task in tasks:
        if task.task_group and task.task_group not in task_groups:
            task_groups[task.task_group] = TaskGroup(task.task_group)
    return task_groups


def build_dag_dependencies(
    tasks: TaskList,
    dbt_target: str,
    dbt_profile_path: Path,
    dbt_project_path: Path,
) -> None:
    airflow_task_groups = build_airflow_task_groups(tasks)
    build_airflow_tasks(
        tasks=tasks,
        dbt_target=dbt_target,
        dbt_profile_path=dbt_profile_path,
        dbt_project_path=dbt_project_path,
        task_groups=airflow_task_groups
    )


def construct_dbt_command(
    dbt_command: str,
    dbt_target: str,
    dbt_profile_path: Path,
    dbt_project_path: Path,
    model_name: str,
):
    profile = f'--profiles-dir {dbt_profile_path.as_posix()}'
    target = f'--target {dbt_target}'
    model = f'--select {model_name}'

    cmd = f'cd {dbt_project_path.as_posix()} && ' \
          f'dbt --no-write-json {dbt_command} {profile} {target} {model}'

    return cmd
