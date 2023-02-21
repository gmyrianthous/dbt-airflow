from typing import Dict

from dbt_airflow.core.task import AirflowTask, DbtAirflowTask, ExtraTask
from dbt_airflow.parser.dbt import DbtResourceType


class TaskList(list):
    """
    A collection of AirflowTasks
    """

    def append(self, item) -> None:
        """
        Appends a new task into the `TaskList` assuming that every element is of type `AirflowTask`
        and no other task with the same `item.task_id` already exists in the TaskList.
        """
        if not isinstance(item, AirflowTask):
            raise ValueError(f'Element of type {type(item)} cannot be added in TaskList.')

        if item.task_id in [t.task_id for t in self]:
            raise ValueError(f'A task with id {item.task_id} already exists')

        super(TaskList, self).append(item)

    def find_task_by_id(self, task_id: str) -> AirflowTask:
        """
        Returns the task with the matching `task_id`. If no task is found a ValueError is raised
        """
        for task in self:
            if task.task_id == task_id:
                return task

        raise ValueError(f'Task with id `{task_id}` was not found.')

    def get_statistics(self) -> Dict[str, int]:
        """
        Returns counts per node type in the resulting TaskList
        """
        resource_types = [task.resource_type for task in self if isinstance(task, DbtAirflowTask)]
        return {
            'models': resource_types.count(DbtResourceType.model),
            'tests': resource_types.count(DbtResourceType.test),
            'snapshots': resource_types.count(DbtResourceType.snapshot),
            'seeds': resource_types.count(DbtResourceType.seed),
            'extra_tasks': sum(isinstance(task, ExtraTask) for task in self)
        }
