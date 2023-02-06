from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional, Set

from airflow.models.baseoperator import BaseOperator
from pydantic import BaseModel, validator


from dbt_airflow.domain.operators import (
    DbtRunOperator,
    DbtTestOperator,
    DbtSeedOperator,
    DbtSnapshotOperator,
)


class DbtResourceType(str, Enum):
    model = 'model'
    test = 'test'
    snapshot = 'snapshot'
    seed = 'seed'


class NodeDeps(BaseModel):
    nodes: List[str]

    def __getitem__(self, item):
        return getattr(self, item)

    @validator('nodes')
    def filter(cls, val):
        """
        Filters out dbt tests since these will be constructed by subsequent steps
        """
        return [
            n for n in val
            if n.split('.')[0] in [
                DbtResourceType.model, DbtResourceType.seed, DbtResourceType.snapshot
            ]
        ]


class Node(BaseModel):
    resource_type: DbtResourceType
    depends_on: Optional[NodeDeps]
    fqn: Optional[List[str]]
    package_name: str
    name: str
    task_group: str = None

    @validator('task_group', always=True)
    def create_task_group(cls, v, values) -> str:
        tg_idx = -2
        if values['resource_type'] == DbtResourceType.snapshot:
            tg_idx = -3

        if len(values['fqn']) >= abs(tg_idx):
            return values['fqn'][tg_idx]
        return values['fqn'][0]


class Manifest(BaseModel):
    nodes: Dict[str, Node]

    @validator('nodes')
    def filter(cls, val):
        return {
            k: v for k, v in val.items()
            if v.resource_type.value in ('model', 'seed', 'snapshot', 'test')
        }

    def get_statistics(self):
        """
        Returns a dictionary containing some statistics of the input manifest data.
        """
        node_types = [n.resource_type for n in self.nodes.values()]
        return {
            'models': node_types.count(DbtResourceType.model),
            'tests': node_types.count(DbtResourceType.test),
            'snapshots': node_types.count(DbtResourceType.snapshot),
            'seeds': node_types.count(DbtResourceType.seed),
        }


@dataclass
class AirflowTask:
    task_id: str

    def __eq__(self, other):
        return self.task_id == other.task_id


@dataclass(eq=False)
class ExtraTask(AirflowTask):
    operator: BaseOperator
    operator_args: field(default_factory=dict)
    upstream_task_ids: Optional[Set[str]] = field(default_factory=set)
    downstream_task_ids: Optional[Set[str]] = field(default_factory=set)
    task_group: Optional[str] = None


@dataclass(eq=False)
class DbtAirflowTask(AirflowTask):
    dbt_operator: Callable = field(init=False)
    manifest_node_name: str  # This is the node name as found on manifest.json file
    model_name: str = field(init=False)
    package_name: str
    resource_type: DbtResourceType
    task_group: Optional[str]
    upstream_task_ids: Set[str]

    def __post_init__(self):
        self.dbt_operator = self.get_dbt_operator()
        self.model_name = self.get_model_name()

    @classmethod
    def from_manifest_node(cls, manifest_node_name: str, node: Node):
        """
        Creates an instance of this dataclass from a Manifest Node
        """
        return DbtAirflowTask(
            task_id=manifest_node_name,
            manifest_node_name=manifest_node_name,
            resource_type=node.resource_type,
            upstream_task_ids=set(node.depends_on.nodes),
            task_group=node.task_group,
            package_name=node.package_name,
        )

    @classmethod
    def test_task_from_manifest_node(cls, parent_manifest_node_name: str, parent_node: Node):
        """
        Creates an instance of this dataclass that corresponds to a dbt task, from the input
        parent node.
        """
        model_name = parent_manifest_node_name.split('.')[-1]
        test_node_name = f'test.{parent_node.package_name}.{model_name}'
        return DbtAirflowTask(
            task_id=test_node_name,
            manifest_node_name='',
            resource_type=DbtResourceType.test,
            upstream_task_ids={parent_manifest_node_name},
            task_group=parent_node.task_group,
            package_name=parent_node.package_name,
        )

    def get_dbt_operator(self) -> Callable:
        if self.resource_type == DbtResourceType.model:
            return DbtRunOperator
        if self.resource_type == DbtResourceType.test:
            return DbtTestOperator
        if self.resource_type == DbtResourceType.seed:
            return DbtSeedOperator
        return DbtSnapshotOperator

    def get_model_name(self) -> str:
        """
        Extracts the model name from the manifest node name (in the form
        `<resource-type>.<package-name>.<model-name>`) of the dbt resource.
        The test tasks get created by dbt-airflow and they all have an empty string as
        `manifest_node_name`. Therefore, the model name is extracted by the upstream dependencies
        of the test task, that always contains a single model/seed/snapshot task.
        """
        if self.resource_type != DbtResourceType.test:
            return self.manifest_node_name.split('.')[-1]
        return list(self.upstream_task_ids)[0].split('.')[-1]


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
