from pathlib import Path
from typing import Dict, List, Optional

from airflow.utils.task_group import TaskGroup

from dbt_airflow.domain.model import TaskList, ExtraAirflowTask, DbtAirflowTask
from dbt_airflow.domain.task_builder import DbtAirflowTaskBuilder


def build_airflow_tasks(
    tasks: TaskList,
    dbt_target: str,
    dbt_profile_path: Path,
    dbt_project_path: Path,
    task_groups: Dict[str, TaskGroup],
) -> None:
    airflow_tasks = {
        task.task_id: task.dbt_operator(
            task_id=task.task_id,
            dbt_target_profile=dbt_target,
            dbt_profile_path=dbt_profile_path,
            dbt_project_path=dbt_project_path,
            resource_name=task.model_name,
            task_group=task_groups.get(task.task_group),
        )
        for task in tasks if isinstance(task, DbtAirflowTask)
    }
    airflow_tasks.update({
        task.task_id: task.operator(task_id=task.task_id, **task.operator_args)
        for task in tasks if isinstance(task, ExtraAirflowTask)
    })

    # Handle dependencies
    for task in tasks:
        for upstream_task in task.upstream_task_ids:
            airflow_tasks[task.task_id] << airflow_tasks[upstream_task]


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
    extra_tasks: Optional[List[ExtraAirflowTask]] = None,
) -> None:
    task_builder = DbtAirflowTaskBuilder(
        manifest_path=dbt_manifest_path.as_posix(),
        extra_tasks=extra_tasks,
    )
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


from airflow.utils.task_group import TaskGroup

class DbtTaskGroup(TaskGroup):

    def __init__(
        self,
        dbt_manifest_path: Path,
        dbt_target: str,
        dbt_profile_path: Path,
        dbt_project_path: Path,
        create_task_groups: Optional[bool] = True,
        extra_tasks: Optional[List[ExtraAirflowTask]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        with self as _self:
            task_builder = DbtAirflowTaskBuilder(
                manifest_path=dbt_manifest_path.as_posix(),
                extra_tasks=extra_tasks,
            )
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

