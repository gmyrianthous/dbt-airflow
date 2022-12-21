import pytest

from dbt_airflow.domain.tasks import Task, TaskList


@pytest.fixture
def mock_run_task():
    """Test task for running a model"""
    return Task(
        model_name='my_model',
        dbt_node_name='model.test_profile.my_model',
        dbt_command='run',
        upstream_tasks=set(),
        task_group='my_task_group',
    )


@pytest.fixture
def mock_test_task():
    """Test task for testing a model"""
    return Task(
        model_name='my_model',
        dbt_node_name='',
        dbt_command='test',
        upstream_tasks={'run_my_model'},
        task_group='my_task_group',
    )


@pytest.fixture
def mock_task_list(mock_test_task, mock_run_task):
    return TaskList([
        Task(
            model_name='my_model',
            dbt_node_name='',
            dbt_command='test',
            upstream_tasks={'run_my_model'},
            task_group='my_task_group',
        ),
        Task(
            model_name='my_model',
            dbt_node_name='model.test_profile.my_model',
            dbt_command='run',
            upstream_tasks=set(),
            task_group='my_task_group',
        ),
        Task(
            model_name='my_model',
            dbt_node_name='snapshot.test_profile.my_model',
            dbt_command='snapshot',
            upstream_tasks=set(),
            task_group='my_task_group',
        ),
        Task(
            model_name='my_model',
            dbt_node_name='seed.test_profile.a_csv_file',
            dbt_command='seed',
            upstream_tasks=set(),
            task_group='another_task_group',
        ),
    ])
