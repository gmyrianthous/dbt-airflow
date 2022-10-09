from enum import Enum
from typing import Any, Optional


class DbtNodeType(Enum):
    """
    TODO
    """
    MODEL = 'model'
    TEST = 'test'
    SNAPSHOT = 'snapshot'
    SEED = 'seed'


class Task:

    def __init__(
        self,
        model_name: str,
        dbt_node_name: str,
        dbt_command: str,
        upstream_tasks: set,
        task_group: Optional[str],
    ):
        self.model_name = model_name
        self.dbt_node_name = dbt_node_name
        self.dbt_command = dbt_command
        self.upstream_tasks = upstream_tasks
        self.task_group = task_group
        self.name = f'{self.dbt_command}_{self.model_name}'

    def __str__(self):
        return f'{self.name}, Deps: {self.upstream_tasks}'

    def to_dict(self):
        return {
            'task_name': self.name,
            'model_name': self.model_name,
            'dbt_node_name': self.dbt_node_name,
            'dbt_command': self.dbt_command,
            'upstream_tasks': list(self.upstream_tasks),
            'task_group': self.task_group,
        }

    @classmethod
    def create_task_from_manifest_node(cls, node_name: str, node_details: dict[str, Any]):
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
            task_group=cls.get_task_group(node_details),
        )

    @staticmethod
    def create_task_name_from_node_name(node_name):
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
    def get_task_group(node_details: dict[str, Any]) -> str:
        """
        TODO: Consider moving this option to argparse
        """
        return node_details['fqn'][1]


class TaskList(list):
    """

    """
    def find_task_by_name(self, name: str) -> Task:
        for task in self:
            if task.name == name:
                return task

    @classmethod
    def find_tasks_by_model_name(cls, model_name: str):
        pass

    def find_task_by_model_name_and_dbt_command(self, model_name: str, dbt_command: str):
        pass
