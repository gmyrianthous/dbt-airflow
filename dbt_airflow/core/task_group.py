from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow.utils.task_group import TaskGroup

from dbt_airflow.core.task import ExtraTask, DbtAirflowTask
from dbt_airflow.core.task_builder import DbtAirflowTaskBuilder


class DbtTaskGroup(TaskGroup):

    def __init__(
        self,
        dbt_manifest_path: Path,
        dbt_target: str,
        dbt_profile_path: Path,
        dbt_project_path: Path,
        operator_class: Optional[str] = 'BashOperator',
        operator_kwargs: Optional[Dict[Any, Any]] = None,
        create_sub_task_groups: Optional[bool] = True,
        extra_tasks: Optional[List[ExtraTask]] = None,
        *args,
        **kwargs
    ):
        self.dbt_manifest_path = dbt_manifest_path
        self.dbt_target = dbt_target
        self.dbt_profile_path = dbt_profile_path
        self.dbt_project_path = dbt_project_path
        self.operator_class = operator_class
        self.create_sub_task_groups = create_sub_task_groups
        self.extra_tasks = extra_tasks

        if operator_kwargs:
            self.operator_kwargs = operator_kwargs
        else:
            self.operator_kwargs = {}

        super().__init__(*args, **kwargs)

        with self as _self:
            task_builder = DbtAirflowTaskBuilder(
                manifest_path=dbt_manifest_path.as_posix(),
                extra_tasks=extra_tasks,
                operator_class=operator_class,
            )
            self.tasks = task_builder.build_tasks()
            self.nested_task_groups = self._build_nested_task_groups()
            self._build_airflow_tasks()

    def _build_nested_task_groups(self) -> Dict[str, TaskGroup]:
        """
        """
        if not self.create_sub_task_groups:
            return {}

        task_groups = {}
        for task in self.tasks:
            if task.task_group and task.task_group not in task_groups:
                task_groups[task.task_group] = TaskGroup(task.task_group)
        return task_groups

    def _build_airflow_tasks(self):
        airflow_tasks = {
            task.task_id: task.dbt_operator(
                task_id=task.task_id,
                dbt_target_profile=self.dbt_target,
                dbt_profile_path=self.dbt_profile_path,
                dbt_project_path=self.dbt_project_path,
                resource_name=task.model_name,
                task_group=self.nested_task_groups.get(task.task_group),
                **self.operator_kwargs,
            )
            for task in self.tasks if isinstance(task, DbtAirflowTask)
        }
        airflow_tasks.update({
            task.task_id: task.operator(task_id=task.task_id, **task.operator_args)
            for task in self.tasks if isinstance(task, ExtraTask)
        })

        # Handle dependencies
        for task in self.tasks:
            for upstream_task in task.upstream_task_ids:
                airflow_tasks[task.task_id] << airflow_tasks[upstream_task]
