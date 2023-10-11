from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Set

from airflow.models.baseoperator import BaseOperator

from dbt_airflow.parser.dbt import DbtResourceType, Node
from dbt_airflow.operators.bash import (
    DbtRunBashOperator,
    DbtTestBashOperator,
    DbtSeedBashOperator,
    DbtSnapshotBashOperator,
)
from dbt_airflow.operators.kubernetes import (
    DbtRunKubernetesPodOperator,
    DbtTestKubernetesPodOperator,
    DbtSeedKubernetesPodOperator,
    DbtSnapshotKubernetesPodOperator,
)


@dataclass
class AirflowTask:
    task_id: str

    def __eq__(self, other):
        return self.task_id == other.task_id


@dataclass(eq=False)
class ExtraTask(AirflowTask):
    """
    This is a dataclass representing an extra task that is supposed to be inserted in a specific
    place within the populated dbt project, based on the task names specified in
    `upstream_task_ids` and/or `downstream_task_ids`.
    """
    operator: BaseOperator
    operator_args: Dict[Any, Any] = field(default_factory=dict)
    upstream_task_ids: Set[str] = field(default_factory=set)
    downstream_task_ids: Set[str] = field(default_factory=set)
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
    operator_class: str

    def __post_init__(self):
        self.dbt_operator = self.get_dbt_operator()
        self.model_name = self.get_model_name()

    @classmethod
    def from_manifest_node(cls, manifest_node_name: str, node: Node, operator_class: str):
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
            operator_class=operator_class,
        )

    @classmethod
    def test_task_from_manifest_node(cls, parent_manifest_node_name: str, parent_node: Node, operator_class: str):
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
            operator_class=operator_class,
        )

    def get_dbt_operator(self) -> Callable:
        """
        Returns a callable that corresponds to the dbt Airflow Operator based on the instance's
        resource type.
        """
        if self.operator_class == 'BashOperator':
            if self.resource_type == DbtResourceType.model:
                return DbtRunBashOperator
            if self.resource_type == DbtResourceType.test:
                return DbtTestBashOperator
            if self.resource_type == DbtResourceType.seed:
                return DbtSeedBashOperator
            return DbtSnapshotBashOperator

        if self.operator_class == 'KubernetesPodOperator':
            if self.resource_type == DbtResourceType.model:
                return DbtRunKubernetesPodOperator
            if self.resource_type == DbtResourceType.test:
                return DbtTestKubernetesPodOperator
            if self.resource_type == DbtResourceType.seed:
                return DbtSeedKubernetesPodOperator
            return DbtSnapshotKubernetesPodOperator

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
