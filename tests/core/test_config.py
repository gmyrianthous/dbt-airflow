import pytest

from dbt_airflow.core.config import (
    DbtAirflowConfig,
    DbtProfileConfig,
    DbtProjectConfig,
)
from dbt_airflow.exceptions import OperatorClassNotSupported


def test_dbt_airflow_config_initialisation():
    """
    GIVEN no input for a dbt-airflow configuration
    WHEN the instance of the dataclass undergoes initialisation
    THEN fields not included in the init are initialised as expected
    """
    # GIVEN/WHEN
    config = DbtAirflowConfig()

    # THEN
    assert config.operator_class == 'BashOperator'
    assert config.extra_tasks == []
    assert config.operator_kwargs == {}
    assert config.create_sub_task_groups is True


def test_dbt_airflow_config_with_user_defined_arguments(mock_extra_task):
    """
    GIVEN user-specified arguments for dbt-airflow configuration
    WHEN the dataclass is initialised
    THEN the object is constructed with the expected fields
    """
    # GIVEN/WHEN
    config = DbtAirflowConfig(
        create_sub_task_groups=False,
        operator_class='KubernetesPodOperator',
        operator_kwargs={'namespace': 'default'},
        extra_tasks=[mock_extra_task],
    )

    # THEN
    assert config.operator_class == 'KubernetesPodOperator'
    assert config.operator_kwargs == {'namespace': 'default'}
    assert config.create_sub_task_groups is False
    assert len(config.extra_tasks) == 1


def test_dbt_airflow_config_post_init():
    """
    GIVEN an invalid/not supported operator
    WHEN instantiation a dbt-airflow config object
    THEN an exception is raised
    """
    # THEN
    with pytest.raises(OperatorClassNotSupported, match='PythonOperator is not supported'):
        # GIVEN/WHEN
        _ = DbtAirflowConfig(operator_class='PythonOperator')
