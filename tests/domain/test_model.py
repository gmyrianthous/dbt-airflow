import pytest

from dbt_airflow.domain.model import (
    AirflowTask,
    DbtAirflowTask,
    TaskList,
)
from dbt_airflow.parser.dbt import DbtResourceType


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
    actual = DbtAirflowTask.from_manifest_node(manifest_node_name, mock_node)

    # THEN
    assert actual.task_id == 'model.mypackage.my_model'
    assert actual.manifest_node_name == manifest_node_name
    assert actual.resource_type == DbtResourceType.model
    assert actual.upstream_task_ids == {'seed.mypackage.my_seed', 'model.mypackage.another_model'}
    assert actual.model_name == 'my_model'


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
    actual = DbtAirflowTask.test_task_from_manifest_node(parent_manifest_name, mock_node)

    # THEN
    assert actual.task_id == 'test.mypackage.my_model'
    assert actual.manifest_node_name == ''
    assert actual.resource_type == DbtResourceType.test
    assert actual.model_name == 'my_model'
    assert actual.upstream_task_ids == {parent_manifest_name}
    assert actual.task_group == mock_node.task_group
    assert actual.package_name == mock_node.package_name


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


def test_task_list_appends_task_elements_correctly(mock_dbt_airflow_task):
    """
    GIVEN an instance of TaskList
    WHEN calling the append method in order to add elements to the TaskList
    THEN elements are added as expected
    """
    # GIVEN
    task_list = TaskList()

    # WHEN
    task_list.append(mock_dbt_airflow_task(task_id='model.mypackage.my_model'))
    task_list.append(mock_dbt_airflow_task(task_id='seed.mypackage.my_seed'))

    # THEN
    assert len(task_list) == 2


def test_task_list_raises_error_when_element_with_not_acceptable_type_is_added(
    mock_dbt_airflow_task
):
    """
    GIVEN an instance of TaskList
    WHEN attempting to add element which is not of type `AirflowTask`
    THEN an error is raised informing the user that this is an invalid action
    """
    expected_error = "Element of type <class 'int'> cannot be added in TaskList"

    # GIVEN
    task_list = TaskList([mock_dbt_airflow_task()])

    # THEN
    with pytest.raises(ValueError, match=expected_error):
        # WHEN
        task_list.append(10)


def test_task_list_raises_error_when_task_id_already_exists(mock_dbt_airflow_task):
    """
    GIVEN an instance of a TaskList
    WHEN attempting to append a task whose task ID already exists in the TaskList
    THEN an error is reported to inform the user this is an invalid operation
    """
    # GIVEN
    task_list = TaskList([mock_dbt_airflow_task(task_id='model.mypackage.my_model')])

    # THEN
    with pytest.raises(ValueError, match='A task with id model.mypackage.my_model already exists'):
        # WHEN
        task_list.append(mock_dbt_airflow_task(task_id='model.mypackage.my_model'))


def test_task_list_find_task_by_id_finds_the_task(mock_dbt_airflow_task):
    """
    GIVEN a TaskList containing some instances of `AirflowTask`
    WHEN searching for a particular Task by specifying a task ID
    THEN the matching task is returned
    """
    # GIVEN
    task_list = TaskList([
        mock_dbt_airflow_task(task_id='model.mypackage.my_model'),
        mock_dbt_airflow_task(task_id='test.mypackage.my_model'),
        mock_dbt_airflow_task(task_id='seed.mypackage.my_seed'),
    ])

    # WHEN
    actual = task_list.find_task_by_id('model.mypackage.my_model')

    # THEN
    assert actual == mock_dbt_airflow_task(task_id='model.mypackage.my_model')


def test_task_list_find_task_by_id_raises_error_when_task_is_not_found(mock_dbt_airflow_task):
    """
    GIVEN a TaskList containing some instances of `AirflowTask`
    WHEN searching for a particular Task by specifying a task ID
    THEN a ValueError is raised if the task id is not found
    """
    # GIVEN
    task_list = TaskList([
        mock_dbt_airflow_task(task_id='model.mypackage.my_model'),
        mock_dbt_airflow_task(task_id='test.mypackage.my_model'),
        mock_dbt_airflow_task(task_id='seed.mypackage.my_seed'),
    ])

    # THEN
    with pytest.raises(ValueError, match="Task with id `does_not_exist` was not found"):
        # WHEN
        task_list.find_task_by_id('does_not_exist')


def test_task_list_get_statistics_reports_correct_counts_per_task_resource_type(
    mock_dbt_airflow_task
):
    """
    GIVEN a task list containing tasks
    WHEN computing the statistics of the TaskList
    THEN the correct metrics/counts are reported
    """
    # GIVEN
    task_list = TaskList([
        mock_dbt_airflow_task(resource_type=DbtResourceType.model),
        mock_dbt_airflow_task(resource_type=DbtResourceType.seed),
        mock_dbt_airflow_task(resource_type=DbtResourceType.model),
        mock_dbt_airflow_task(resource_type=DbtResourceType.test),
        mock_dbt_airflow_task(resource_type=DbtResourceType.snapshot),
    ])

    # WHEN
    actual = task_list.get_statistics()

    # THEN
    expected = {'models': 2, 'tests': 1, 'snapshots': 1, 'seeds': 1, 'extra_tasks': 0}
    assert actual == expected
