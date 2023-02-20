import pytest

from dbt_airflow.core.task_list import TaskList
from dbt_airflow.parser.dbt import DbtResourceType


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
