import pytest

from dbt_airflow.domain.tasks import Task, TaskList


def test_task_init_constructs_correcxt_task_name(mock_run_task):
    expected_task_name = 'run_my_model'
    assert mock_run_task.name == expected_task_name


def test_task_to_string(mock_run_task):
    expected = 'run_my_model'
    assert str(mock_run_task) == expected


def test_task_equality_is_based_on_task_name(mock_run_task):
    """
    Task equality is computed based on task name, which in turn gets constructed from
    `dbt_command` and `model_name`.
    """
    other = Task(
        model_name='my_model',  # same as mock
        dbt_node_name='model.test_profile.another_model',
        dbt_command='run',  # same as mock
        upstream_tasks=set(),
        task_group='different_task_group_from_the_mocked_one',
    )

    assert other == mock_run_task


def test_task_to_dict(mock_test_task, mock_run_task):
    expected = {
        'task_name': 'test_my_model',
        'model_name': 'my_model',
        'dbt_node_name': '',
        'dbt_command': 'test',
        'upstream_tasks': [mock_run_task.name],
        'task_group': 'my_task_group',
    }
    actual = mock_test_task.to_dict()
    assert expected == actual


def test_create_task_from_manifest_node_model():
    manifest_node_name = 'model.my_profile.my_model'
    manifest_node_details = {
        'raw_sql': 'SELECT * FROM one_table;',
        'compiled': True,
        'resource_type': 'model',
        'depends_on': {
            'macros': [],
            'nodes': [
                'model.my_profile.another_model',
                'model.my_profile.third_model',
            ]
        },
        'database': 'my_database',
        'schema': 'my_schema',
        'fqn': [
            'my_profile',
            'marts',
            'finance',
            'my_model'
        ]
    }

    actual = Task.create_task_from_manifest_node(manifest_node_name, manifest_node_details, True)
    expected = Task(
        model_name='my_model',
        dbt_node_name=manifest_node_name,
        dbt_command='run',
        upstream_tasks={'model.my_profile.another_model', 'model.my_profile.third_model'},
        task_group='finance',
    )

    assert actual == expected


@pytest.mark.parametrize(
    'node_name, expected', [
        pytest.param('model.my_profile.my_model', 'run_my_model', id='model'),
        pytest.param('snapshot.my_profile.my_model', 'snapshot_my_model', id='snapshot'),
        pytest.param('seed.my_profile.users_csv', 'seed_users_csv', id='seed'),
    ]
)
def test_create_task_name_from_node_node(node_name, expected):
    actual = Task.create_task_name_from_node_name(node_name)
    assert actual == expected


def test_get_model_name():
    node_name = 'model.my_profile.my_model'
    expected = 'my_model'
    actual = Task.get_model_name(node_name)
    assert actual == expected


def test_get_node_type():
    node_name = 'model.my_profile.my_model'
    expected = 'model'
    actual = Task.get_node_type(node_name)
    assert actual == expected


@pytest.mark.parametrize(
    'node_name, expected', [
        pytest.param('model.my_profile.my_model', 'run', id='model run'),
        pytest.param('snapshot.my_profile.my_model', 'snapshot', id='snapshot remains snapshot'),
        pytest.param('seed.my_profile.users_csv', 'seed', id='seed remains seed'),
    ]
)
def test_get_dbt_command(node_name, expected):
    actual = Task.get_dbt_command(node_name)
    assert actual == expected


@pytest.mark.parametrize(
    'node_details, idx, expected', [
        pytest.param(
            {'name': 'my_model', 'fqn': ['my_profile', 'marts', 'finance', 'my_model']},
            -2,
            'finance',
            id='extract from the last but one index'
        ),
        pytest.param(
            {'name': 'my_model', 'fqn': ['my_profile', 'marts', 'finance', 'my_model']},
            1,
            'marts',
            id='extract from the first index'
        ),
    ]
)
def test_get_task_group(node_details, idx, expected):
    actual = Task.get_task_group(node_details, idx)
    assert actual == expected


@pytest.mark.parametrize(
    'node_details, expected', [
        pytest.param(
            {
                'name': 'my_model',
                'depends_on': {
                    'macros': [],
                    'nodes': [
                        'model.my_profile.second_model',
                        'model.my_profile.third_model',
                        'macro.dbt.get_where_subquery',
                    ]
                }
            },
            {'run_second_model', 'run_third_model'},
        ),
        pytest.param(
            {
                'name': 'my_model',
                'depends_on': {
                    'macros': [],
                    'nodes': [
                        'model.my_profile.second_model',
                        'model.my_profile.third_model',
                        'seed.my_profile.customers_csv',
                        'macro.dbt.get_where_subquery',
                    ]
                }
            },
            {'run_second_model', 'run_third_model', 'seed_customers_csv'},
        ),
    ]
)
def test_get_upstream_dependencies(node_details, expected):
    actual = Task.get_upstream_dependencies(node_details)
    assert actual == expected


def test_task_list_find_task_by_name(mock_task_list):
    expected_task = Task(
        model_name='my_model',
        dbt_node_name='',
        dbt_command='test',
        upstream_tasks={'run_my_model'},
        task_group='my_task_group',
    )
    actual = mock_task_list.find_task_by_name('test_my_model')

    assert actual == expected_task


def test_task_list_find_task_by_name_raises(mock_task_list):
    with pytest.raises(ValueError):
        _ = mock_task_list.find_task_by_name('task_does_not_exist')


def test_task_list_get_statistics(mock_task_list):
    expected = {
        'models': 1,
        'tests': 1,
        'snapshots': 1,
        'seeds': 1,
    }
    actual = mock_task_list.get_statistics()

    assert actual == expected
