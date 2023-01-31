import pytest

from dbt_airflow.domain.model import (
    AirflowTask,
    DbtAirflowTask,
    DbtCommand,
    DbtResourceType,
    Manifest,
    NodeDeps,
    TaskList,
)


@pytest.fixture
def mock_manifest_data():
    return {
        'metadata': {},
        'nodes': {
            'model.mypackage.my_model': {
                'name': 'my_model',
                'resource_type': 'model',
                'compiled': True,
                'depends_on': {
                    'macros': [],
                    'nodes': [
                        'seed.mypackage.my_seed',
                        'model.mypackage.another_model'
                    ]
                },
                'package_name': 'mypackage',
                'fqn': [
                    'a',
                    'b',
                    'c'
                ]
            },
            'seed.mypackage.my_seed': {
                'name': 'my_seed',
                'resource_type': 'seed',
                'compiled': True,
                'depends_on': {
                    'macros': [],
                    'nodes': []
                },
                'package_name': 'mypackage',
                'fqn': [
                    'a'
                ]
            },
            'model.mypackage.another_model': {
                'name': 'another_model',
                'resource_type': 'model',
                'compiled': True,
                'depends_on': {
                    'macros': [],
                    'nodes': [
                        'seed.mypackage.my_seed'
                    ]
                },
                'package_name': 'mypackage',
                'fqn': ['d', 'e', 'f']
            },
            'snapshot.mypackage.my_snapshot': {
                'name': 'my_snapshot',
                'resource_type': 'snapshot',
                'compiled': True,
                'depends_on': {
                    'macros': [],
                    'nodes': [
                        'model.mypackage.my_model'
                    ]
                },
                'package_name': 'mypackage',
                'fqn': ['a', 'b', 'c']
            },
            'test.mypackage.not_null_another_model_field_A.c9c3c572df': {
                'name': 'not_null_another_model_field_A',
                'resource_type': 'test',
                'compiled': True,
                'depends_on': {
                    'macros': [],
                    'nodes': [
                        'model.mypackage.another_model'
                    ]
                },
                'package_name': 'mypackage',
                'fqn': ['w', 'x', 'y']
            }
        }
    }


@pytest.fixture
def mock_manifest(mock_manifest_data):
    return Manifest(**mock_manifest_data)


@pytest.fixture
def mock_node(mock_manifest):
    return mock_manifest.nodes['model.mypackage.my_model']


@pytest.fixture
def mock_node_deps(mock_node):
    return mock_node.depends_on


@pytest.fixture
def mock_dbt_airflow_task():

    def create_task(
        task_id='model.mypackage.my_model',
        manifest_node_name='model.mypackage.my_model',
        resource_type=DbtResourceType.model,
        upstream_task_ids=None,
        task_group='b',
        package_name='mypackage',
    ):
        if not upstream_task_ids:
            upstream_task_ids = {'seed.mypackage.my_seed', 'model.mypackage.another_model'}

        return DbtAirflowTask(
            task_id=task_id,
            manifest_node_name=manifest_node_name,
            resource_type=resource_type,
            upstream_task_ids=upstream_task_ids,
            task_group=task_group,
            package_name=package_name,
        )

    return create_task


def test_node_deps_model_filters_keeps_only_model_seed_and_snapshot_resources():
    """
    GIVEN a list of nodes representing NodeDeps
    WHEN validating `nodes` field of NodeDeps dataclass
    THEN the validator will filter out any dependency which does not correspond to a dbt model,
        seed or snapshot
    """
    # GIVEN
    resources = [
        'model.profile_name.my_model',
        'model.profile_name.another_model',
        'seed.profile_name.my_seed',
        'snapshot.profile_name.my_snapshot',
        'test.profile_name.not_null_test_my_model',
    ]

    # WHEN
    node_deps = NodeDeps(nodes=resources)

    # THEN
    assert node_deps.nodes == [
        'model.profile_name.my_model',
        'model.profile_name.another_model',
        'seed.profile_name.my_seed',
        'snapshot.profile_name.my_snapshot',
    ]


def test_node_model_parsing(mock_node):
    assert mock_node.name == 'my_model'
    assert mock_node.resource_type == 'model'
    assert mock_node.package_name == 'mypackage'
    assert mock_node.task_group == 'b'
    assert len(mock_node.depends_on.nodes) == 2


def test_manifest_model_parsing(mock_manifest_data):
    manifest = Manifest(**mock_manifest_data)
    assert len(manifest.nodes) == 5


def test_manifest_model_get_statistics(mock_manifest):
    """
    GIVEN manifest json data
    WHEN parsing the Manifest model
    THEN the number of parsed resource types (models, seeds, snapshot, tests) is computed correctly
    """
    # GIVEN/WHEN
    actual = mock_manifest.get_statistics()

    # THEN
    expected = {
        'models': 2,
        'tests': 1,
        'snapshots': 1,
        'seeds': 1,
    }
    assert actual == expected


def test_node_deps_model_parsing(mock_node_deps):
    assert len(mock_node_deps.nodes) == 2
    assert mock_node_deps.nodes == ['seed.mypackage.my_seed', 'model.mypackage.another_model']


def test_airflow_task_equality():
    first_task = AirflowTask('first_task')
    same_task = AirflowTask('first_task')

    assert first_task == same_task


def test_dbt_airflow_task_dataclass_initialisation():
    """
    GIVEN the inputs for a dbt Airflow Task
    WHEN the instance of the dataclass undergoes post initialisation
    THEN fields not included in the init (dbt_command and model_name) are initialised as expected
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
    assert task.dbt_command == DbtCommand.run
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
    assert actual.dbt_command == DbtCommand.run
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
    assert actual.dbt_command == DbtCommand.test
    assert actual.model_name == 'my_model'
    assert actual.upstream_task_ids == {parent_manifest_name}
    assert actual.task_group == mock_node.task_group
    assert actual.package_name == mock_node.package_name


@pytest.mark.parametrize(
    'resource_type, expected', [
        pytest.param(DbtResourceType.test, DbtCommand.test, id='Test `test` resource'),
        pytest.param(DbtResourceType.model, DbtCommand.run, id='Test `model` resource'),
        pytest.param(DbtResourceType.seed, DbtCommand.seed, id='Test `seed` resource'),
        pytest.param(DbtResourceType.snapshot, DbtCommand.snapshot, id='Test `snapshot` resource'),
    ],
)
def test_dbt_airflow_task_get_dbt_command(mock_dbt_airflow_task, resource_type, expected):
    """
    GIVEN a particular DbtAirflowTask instance
    WHEN generating the dbt command
    THEN the correct dbt command is returned based on the resource type of DbtAirflowTask instance
    """
    # GIVEN
    mock_task = mock_dbt_airflow_task(resource_type=resource_type)

    # WHEN
    actual = mock_task.get_dbt_command()

    # THEN
    assert actual == expected


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


def test_task_list_find_task_by_id_finds_the_task():
    assert False


def test_task_list_find_task_by_id_raises_error_when_task_is_not_found():
    assert False

def test_task_list_get_statistics_reports_correct_counts_per_task_resource_type():
    assert False