from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, validator


class DbtResourceType(str, Enum):
    model = 'model'
    test = 'test'
    snapshot = 'snapshot'
    seed = 'seed'

    # dbt resource types we are not interested in, but we still need them in order for
    # dbt-airflow to be able to parse the manifest file
    operation = 'operation'


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
    task_group: Optional[str] = None

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
