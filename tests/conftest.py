import pytest

from dbt_airflow.domain.tasks import Task


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
