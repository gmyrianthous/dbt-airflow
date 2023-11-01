import pytest

from dbt_airflow.core.task import AirflowTask, DbtAirflowTask
from dbt_airflow.parser.dbt import DbtResourceType
from dbt_airflow.operators.execution import ExecutionOperator


def test_airflow_task_equality():
    first_task = AirflowTask('first_task')
    same_task = AirflowTask('first_task')

    assert first_task == same_task


def test_dbt_airflow_task_dataclass_initialisation():
    """
    GIVEN the inputs for a dbt Airflow Task
    WHEN the instance of the dataclass undergoes post initialisation
    THEN fields not included in the init (model_name) are initialised as expected
    """
    # GIVEN/WHEN
    task = DbtAirflowTask(
        task_id='model.mypackage.my_model',
        manifest_node_name='model.mypackage.my_model',
        package_name='mypackage',
        resource_type=DbtResourceType.model,
        task_group='b',
        upstream_task_ids={'seed.mypackage.my_seed', 'model.mypackage.another_model'},
        execution_operator=ExecutionOperator.BASH,
    )

    # THEN
    assert task.model_name == 'my_model'


def test_dbt_airflow_task_from_manifest_node(mock_node):
    """
    GIVEN a manifest node (and a node name as appears in manifest.json)
    WHEN we would like to create a new instance of DbtAirflowTask from manifest node data
    THEN a new instance is created using the class method `from_manifest_node`
    """
    # GIVEN
    manifest_node_name = 'model.mypackage.my_model'

    # WHEN
    actual = DbtAirflowTask.from_manifest_node(
        manifest_node_name, mock_node, ExecutionOperator.BASH
    )

    # THEN
    assert actual.task_id == 'model.mypackage.my_model'
    assert actual.manifest_node_name == manifest_node_name
    assert actual.resource_type == DbtResourceType.model
    assert actual.upstream_task_ids == {'seed.mypackage.my_seed', 'model.mypackage.another_model'}
    assert actual.model_name == 'my_model'
    assert actual.execution_operator == ExecutionOperator.BASH


def test_dbt_airflow_task_test_task_from_manifest_node(mock_node):
    """
    GIVEN a parent manifest node
    WHEN we the parent node was identified that it has also dbt tests associated with it
    THEN a new DbtAirflowTask is created that corresponds to the test task of the corresponding
        parent manifest node
    """
    # GIVEN
    parent_manifest_name = 'model.mypackage.my_model'

    # WHEN
    actual = DbtAirflowTask.test_task_from_manifest_node(
        parent_manifest_name, mock_node, ExecutionOperator.BASH
    )

    # THEN
    assert actual.task_id == 'test.mypackage.my_model'
    assert actual.manifest_node_name == ''
    assert actual.resource_type == DbtResourceType.test
    assert actual.model_name == 'my_model'
    assert actual.upstream_task_ids == {parent_manifest_name}
    assert actual.task_group == mock_node.task_group
    assert actual.package_name == mock_node.package_name
    assert actual.execution_operator == ExecutionOperator.BASH


@pytest.mark.parametrize(
    'resource_type, manifest_node_name, upstream_task_ids, expected', [
        pytest.param(
            DbtResourceType.test,
            '',  # Test tasks are created with an empty `manifest_node_name`
            {'model.mypackage.another_model'},
            'another_model',
            id='Extract model name for a test resource'
        ),
        pytest.param(
            DbtResourceType.model, 'model.mypackage.my_model', None, 'my_model',
            id='Extract model name for a model resource'
        ),
        pytest.param(
            DbtResourceType.seed, 'seed.mypackage.my_seed', None, 'my_seed',
            id='Extract model name for a seed resource'
        ),
        pytest.param(
            DbtResourceType.snapshot, 'snapshot.mypackage.my_snapshot', None, 'my_snapshot',
            id='Extract model name for a snapshot resource'
        ),
    ]
)
def test_dbt_airflow_task_get_model_name(
    mock_dbt_airflow_task,
    resource_type,
    manifest_node_name,
    upstream_task_ids,
    expected,
):
    """
    GIVEN a particular DbtAirflowTask instance
    WHEN extracting the model name
    THEN the model name is extracted based on the manifest node name or upstream
        dependencies, depending on the resource type
    """
    # GIVEN
    mock_task = mock_dbt_airflow_task(
        resource_type=resource_type,
        manifest_node_name=manifest_node_name,
        upstream_task_ids=upstream_task_ids
    )

    # WHEN
    actual = mock_task.get_model_name()

    # THEN
    assert actual == expected


