from typing import Dict, Optional

from airflow.utils.task_group import TaskGroup

from dbt_airflow.core.config import DbtAirflowConfig, DbtProfileConfig, DbtProjectConfig
from dbt_airflow.core.task import ExtraTask, DbtAirflowTask
from dbt_airflow.core.task_builder import DbtAirflowTaskBuilder


class DbtTaskGroup(TaskGroup):

    def __init__(
        self,
        dbt_project_config: DbtProjectConfig,
        dbt_profile_config: DbtProfileConfig,
        dbt_airflow_config: Optional[DbtAirflowConfig] = DbtAirflowConfig(),
        *args,
        **kwargs
    ) -> None:
        self.dbt_project_config = dbt_project_config
        self.dbt_profile_config = dbt_profile_config
        self.dbt_airflow_config = dbt_airflow_config
        super().__init__(*args, **kwargs)

        with self as _self:
            task_builder = DbtAirflowTaskBuilder(
                manifest_path=self.dbt_project_config.manifest_path.as_posix(),
                extra_tasks=self.dbt_airflow_config.extra_tasks,
                execution_operator=self.dbt_airflow_config.execution_operator,
            )
            self.tasks = task_builder.build_tasks()
            self.nested_task_groups = self._build_nested_task_groups()
            self._build_airflow_tasks()

    def _build_nested_task_groups(self) -> Dict[str, TaskGroup]:
        """
        """
        if not self.dbt_airflow_config.create_sub_task_groups:
            return {}

        task_groups = {}
        for task in self.tasks:
            if task.task_group and task.task_group not in task_groups:
                task_groups[task.task_group] = TaskGroup(task.task_group)
        return task_groups

    def _build_airflow_tasks(self) -> None:
        """
        Creates the Airflow Tasks and handles all of their dependencies
        """
        airflow_tasks = {
            task.task_id: task.dbt_operator(
                task_id=task.task_id,
                dbt_target_profile=self.dbt_profile_config.target,
                dbt_profile_path=self.dbt_profile_config.profiles_path,
                dbt_project_path=self.dbt_project_config.project_path,
                selectors=[task.model_name] + self.dbt_airflow_config.selectors,
                exclude=self.dbt_airflow_config.exclude,
                full_refresh=self.dbt_airflow_config.full_refresh,
                no_write_json=self.dbt_airflow_config.no_write_json,
                variables=self.dbt_airflow_config.variables,
                no_partial_parse=self.dbt_airflow_config.no_partial_parse,
                warn_error=self.dbt_airflow_config.warn_error,
                warn_error_options=self.dbt_airflow_config.warn_error_options,
                task_group=self.nested_task_groups.get(task.task_group),
                **self.dbt_airflow_config.operator_kwargs,
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
