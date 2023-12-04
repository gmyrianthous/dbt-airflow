from enum import Enum
from typing import Dict, List, Optional, Any
import logging

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
    # Note: The following initialisation of a mutable object such as a list,
    # would normally suggest a bug, but `pydantic` handles this properly,
    # meaning that a deep copy will be created and each model instance will
    # get its own empty list.
    nodes: Optional[List[str]] = []

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
    tags: Optional[List[str]]
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

    @classmethod
    def load(cls, data: Dict[str, Any], **kwargs) -> "Manifest":
        """
        This method serves as a factory for creating instances of the Manifest class. It can initialize an instance
        either with the raw data provided or with a subset of the data filtered based on tags, if provided.
        """
        tags = kwargs.get('tags', [])
        if tags:
            logging.info(f'Filtering nodes by tags {tags}')
            return cls.filter_by_tags(data, tags)
        else:
            return cls(**data)


    @classmethod
    def filter_by_tags(cls, data,tags: List[str]) -> "Manifest":
        """
        Filters a dataset of nodes based on specified tags and dependencies.

        This method processes a given dataset to select nodes that match one or more of the provided tags.
        It also includes 'test' type nodes if they have dependencies on any of the selected nodes.
        Post filtering, it updates the dependencies of the nodes to ensure they are consistent
        within the context of the filtered dataset.
        """

        # Step 1: Filter nodes based on tags
        filtered_nodes = {
            node_name: node for node_name, node in data["nodes"].items()
            if any(tag in node["tags"] for tag in tags)
        }

        # Step 2: Filter for 'test' nodes with dependencies in filtered_nodes
        filtered_nodes_with_test = {
            node_name: node for node_name, node in data["nodes"].items()
            if node["resource_type"] == "test" and any(dep in filtered_nodes for dep in node["depends_on"]["nodes"])
        }

        final_filtered_nodes = {**filtered_nodes, **filtered_nodes_with_test}

        # Step 3: Update dependencies
        for node in final_filtered_nodes.values():
            updated_dependent_nodes=[]
            for dependent_node in node["depends_on"]["nodes"]:
                if dependent_node in final_filtered_nodes.keys():
                    updated_dependent_nodes.append(dependent_node)
            node["depends_on"]["nodes"] = updated_dependent_nodes

        filtered_data = {'nodes': final_filtered_nodes}
        return cls(**filtered_data)
