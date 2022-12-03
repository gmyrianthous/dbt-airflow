from dbt_airflow.domain.tasks import Task


def test_task_init_constructs_correct_task_name(mock_run_task):
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
