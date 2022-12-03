import json
import os
from enum import Enum
from typing import Any, Dict, List, Optional

from dbt_airflow.exceptions import TaskGroupExtractionError


class DbtNodeType(Enum):
    """
    Each dbt node could be of one of the specified types within this Enum class.
    """
    MODEL = 'model'
    TEST = 'test'
    SNAPSHOT = 'snapshot'
    SEED = 'seed'


class DbtNode:
    pass


class Task:
    """
    Corresponds to a single Task as this is understood within the context of
    dbt-airflow (not to be confused with Airflow Tasks).

    This is an internal convention that is used to represent a single task
    that is extracted from manifest.json file.
    """

    def __init__(
        self,
        model_name: str,
        dbt_node_name: str,
        dbt_command: str,
        upstream_tasks: set,
        task_group: Optional[str],
    ):
        self.model_name = model_name
        self.dbt_command = dbt_command
        self.dbt_node_name = dbt_node_name
        self.name = f'{self.dbt_command}_{self.model_name}'
        self.upstream_tasks = upstream_tasks
        self.task_group = task_group

    def __str__(self) -> str:
        return self.name

    def __eq__(self, other) -> bool:
        return self.name == other.name

    def to_dict(self):
        """
        Returns the task info as a dictionary.
        """
        return {
            'task_name': self.name,
            'model_name': self.model_name,
            'dbt_node_name': self.dbt_node_name,
            'dbt_command': self.dbt_command,
            'upstream_tasks': list(self.upstream_tasks),
            'task_group': self.task_group,
        }

    @classmethod
    def create_task_from_manifest_node(
        cls,
        node_name: str,
        node_details: dict[str, Any],
        create_task_group: bool,
    ):
        """
        Given a dbt node as specified in manifest.json file, construct a Task instance
        """
        return Task(
            model_name=cls.get_model_name(node_name),
            dbt_node_name=node_name,
            dbt_command=cls.get_dbt_command(node_name),
            upstream_tasks=set(
                Task.create_task_name_from_node_name(node_name)
                for node_name in node_details['depends_on']['nodes']
                if Task.get_node_type(node_name) in [
                    DbtNodeType.MODEL.value,
                    DbtNodeType.SEED.value,
                    DbtNodeType.SNAPSHOT.value,
                ]
            ),
            task_group=cls.get_task_group(node_details) if create_task_group else None,
        )

    @staticmethod
    def create_task_name_from_node_name(node_name: str) -> str:
        """
        Constructs and returns a task name from the input node name
        in the form `<dbt-command>_<model_name>` (e.g. `run_my_model`)
        """
        return f'{Task.get_dbt_command(node_name)}_{Task.get_model_name(node_name)}'

    @staticmethod
    def get_model_name(node_name: str) -> str:
        """
        From `model.<dbt-profile-name>.<model_name>` extracts `model_name`
        """
        return node_name.split('.')[-1]

    @staticmethod
    def get_node_type(node_name: str) -> str:
        """
        From `<node_type>.<dbt-profile-name>.<model_name>` extracts `node_type`
        """
        return node_name.split('.')[0]

    @staticmethod
    def get_dbt_command(node_name: str) -> str:
        """
        Returns the corresponding dbt command based on the dbt node type.
        --------------------
        type        command
        --------------------
        model    -> run
        test     -> test
        seed     -> seed
        snapshot -> snapshot
        """
        node_type = Task.get_node_type(node_name)
        if node_type == DbtNodeType.MODEL.value:
            return 'run'
        return node_type

    @staticmethod
    def get_task_group(node_details: Dict[str, Any], idx: Optional[int] = -2) -> str:
        """
        The task group logic is based on the structure of a dbt project. This structure is
        specified in the `fqn` key that each of the nodes has in manifest.json file.

        TODO: Consider moving this option to argparse
        """
        try:
            return node_details['fqn'][-2]
        except IndexError:
            raise TaskGroupExtractionError(
                f"Task Group cannot be extracted from fqn "
                f"{node_details['fqn']} with index index {idx}."
            )


class TaskList(List):
    """
    A collection of dbt-airflow tasks
    """
    # TODO: Prevent adding tasks of the same name i.e. which are __eq__(ual)
    # def append(self, __object: _T) -> None:
    #     if __object in self:
    #         raise ValueError('test')

    def build_airflow_dependencies(self):
        # for task in self:
        #     pass
        pass

    def find_task_by_name(self, name: str) -> Optional[Task]:
        """
        Returns the task within the TaskList whose name is equal to the input `name`.
        If no task is found with the given name, then `None` is returned.
        """
        for task in self:
            if task.name == name:
                return task

    def write_to_file(self, path: str) -> None:
        """
        Dumps tasks in list as json into the specified path
        """
        # os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            tasks_dict = [task.to_dict() for task in self]
            json.dump(tasks_dict, f, indent=4, default=str)

    def get_statistics(self) -> Dict[str, int]:
        """
        Returns counts per node type in the resulting TaskList
        """
        node_types = [task.dbt_command for task in self]
        return {
            'models': node_types.count('run'),
            'tests': node_types.count('test'),
            'snapshots': node_types.count('snapshot'),
            'seeds': node_types.count('seed'),
        }
