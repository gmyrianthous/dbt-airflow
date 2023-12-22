import logging

from enum import Enum
from typing import Any, Dict, List, Optional

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
            n
            for n in val
            if n.split('.')[0]
            in [DbtResourceType.model, DbtResourceType.seed, DbtResourceType.snapshot]
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
            k: v
            for k, v in val.items()
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
    def load(
        cls,
        data: Dict[str, Any],
        include_tags: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
    ) -> 'Manifest':
        """
        Factory method to create a Manifest instance. It can initialize an instance with the raw
        data, applying filters based on provided tags or excluding certain tags.
        """
        if include_tags:
            logging.info(f'Filtering nodes by tags {include_tags}')
            data = cls.include_by_tags(data, include_tags)

        if exclude_tags:
            logging.info(f'Excluding nodes by tags {exclude_tags}')
            data = cls.exclude_by_tags(data, exclude_tags)

        return cls(**data)

    @staticmethod
    def include_by_tags(data: Dict[str, Any], tags: List[str]) -> Dict[str, Any]:
        """
        Filters the dataset of nodes based on specified tags and then includes related 'test' nodes
        """
        filtered_nodes_with_no_tests = {
            node_name: node
            for node_name, node in data['nodes'].items()
            if any(tag in node['tags'] for tag in tags)
        }

        filtered_nodes_with_tests = Manifest.filter_tests_with_dependencies(
            data['nodes'],
            filtered_nodes_with_no_tests,
        )

        filtered_nodes = Manifest.update_dependencies(filtered_nodes_with_tests)

        return {'nodes': filtered_nodes}

    @staticmethod
    def exclude_by_tags(data: Dict[str, Any], exclude_tags: List[str]) -> Dict[str, Any]:
        """
        Filters out any nodes that have tags matching any in the exclude_tags list.
        """

        filtered_nodes_with_no_tests = {
            node_name: node
            for node_name, node in data['nodes'].items()
            if not any(tag in node['tags'] for tag in exclude_tags)
        }

        filtered_nodes_and_tests = Manifest.filter_tests_with_dependencies(
            data['nodes'],
            filtered_nodes_with_no_tests,
        )
        filtered_nodes = Manifest.update_dependencies(filtered_nodes_and_tests)

        return {'nodes': filtered_nodes}

    @staticmethod
    def filter_tests_with_dependencies(
        all_nodes: Dict[str, Any],
        filtered_nodes: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Includes 'test' nodes that have dependencies on any of the nodes in the filtered list.
        """

        filtered_tests_nodes = {
            node_name: node
            for node_name, node in all_nodes.items()
            if node['resource_type'] == DbtResourceType.test
            and any(dep in filtered_nodes for dep in node['depends_on']['nodes'])
        }

        return {**filtered_nodes, **filtered_tests_nodes}

    @staticmethod
    def update_dependencies(filtered_nodes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates the dependencies of each node to ensure they only reference nodes present in
        the filtered list.
        """
        for node in filtered_nodes.values():
            if node['depends_on'].get('nodes') is not None:
                node['depends_on']['nodes'] = [
                    dep for dep in node['depends_on']['nodes'] if dep in filtered_nodes
                ]
        return filtered_nodes
