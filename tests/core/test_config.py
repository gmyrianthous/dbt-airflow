from pathlib import Path

import pytest

from dbt_airflow.core.config import (
    DbtAirflowConfig,
    DbtProfileConfig,
    DbtProjectConfig,
)
from dbt_airflow.exceptions import ExecutionOperatorNotSupported
from dbt_airflow.operators.execution import ExecutionOperator


def test_dbt_airflow_config_initialisation():
    """
    GIVEN no input for a dbt-airflow configuration
    WHEN the instance of the dataclass undergoes initialisation
    THEN fields not included in the init are initialised as expected
    """
    # GIVEN/WHEN
    config = DbtAirflowConfig()

    # THEN
    assert config.execution_operator == ExecutionOperator.BASH
    assert config.extra_tasks == []
    assert config.operator_kwargs == {}
    assert config.create_sub_task_groups is True
    assert config.selectors == []
    assert config.full_refresh is False
    assert config.variables is None


def test_dbt_airflow_config_with_user_defined_arguments(mock_extra_task):
    """
    GIVEN user-specified arguments for dbt-airflow configuration
    WHEN the dataclass is initialised
    THEN the object is constructed with the expected fields
    """
    # GIVEN/WHEN
    config = DbtAirflowConfig(
        create_sub_task_groups=False,
        execution_operator=ExecutionOperator.KUBERNETES,
        operator_kwargs={'namespace': 'default'},
        extra_tasks=[mock_extra_task],
        selectors=['tag:daily'],
        full_refresh=True,
        variables='{key: value, date: 20190101}'
    )

    # THEN
    assert config.execution_operator == ExecutionOperator.KUBERNETES
    assert config.operator_kwargs == {'namespace': 'default'}
    assert config.create_sub_task_groups is False
    assert len(config.extra_tasks) == 1
    assert config.selectors == ['tag:daily']
    assert config.full_refresh is True
    assert config.variables == '{key: value, date: 20190101}'


def test_dbt_airflow_config_post_init():
    """
    GIVEN an invalid/not supported operator
    WHEN instantiation a dbt-airflow config object
    THEN an exception is raised
    """
    # THEN
    with pytest.raises(ExecutionOperatorNotSupported, match='PythonOperator is not supported'):
        # GIVEN/WHEN
        _ = DbtAirflowConfig(execution_operator='PythonOperator')


@pytest.mark.parametrize(
    'profiles_path, target', [
        pytest.param(Path('/home/dbt/project/profiles'), 'dev', id='Valid arguments'),
    ]
)
def test_dbt_profile_config_initialisation(profiles_path, target):
    """
    GIVEN valid arguments for DbtProfileConfig class
    WHEN creating an instance of DbtProfileConfig object
    THEN the object gets created with the expected field values
    """
    # GIVEN/WHEN
    config = DbtProfileConfig(profiles_path=profiles_path, target=target)

    # THEN
    assert config.profiles_path == profiles_path
    assert config.target == target


@pytest.mark.parametrize(
    'kwargs, missing_arg', [
        pytest.param(
            {'profiles_path': Path('/home/dbt/project/profiles')},
            'target',
            id='Missing `target` which is a required field',
        ),
        pytest.param(
            {'target': 'dev'},
            'profiles_path',
            id='Missing `profiles_path` which is a required field',
        ),
    ]
)
def test_dbt_profile_config_invalid_init(kwargs, missing_arg):
    """
    GIVEN missing arguments
    WHEN creating an instance of DbtProfileConfig
    THEN a TypeError is raised
    """
    with pytest.raises(TypeError, match=f"missing 1 required positional argument: '{missing_arg}'"):
        _ = DbtProfileConfig(**kwargs)


@pytest.mark.parametrize(
    'project_path, manifest_path', [
        pytest.param(
            Path('/home/dbt/project'),
            Path('/home/dbt/project/target/manifest.json'),
            id='Valid arguments'
        ),
    ]
)
def test_dbt_project_config_initialisation(project_path, manifest_path):
    """
    GIVEN valid arguments for DbtProjectConfig class
    WHEN creating an instance of DbtProjectConfig object
    THEN the object gets created with the expected field values
    """
    # GIVEN/WHEN
    config = DbtProjectConfig(project_path=project_path, manifest_path=manifest_path)

    # THEN
    assert config.project_path == project_path
    assert config.manifest_path == manifest_path


@pytest.mark.parametrize(
    'kwargs, missing_arg', [
        pytest.param(
            {'project_path': Path('/home/dbt/project')},
            'manifest_path',
            id='Missing `manifest_path` which is a required field',
        ),
        pytest.param(
            {'manifest_path': Path('/home/dbt/project/target/manifest.json')},
            'project_path',
            id='Missing `project_path` which is a required field',
        ),
    ]
)
def test_dbt_project_config_invalid_init(kwargs, missing_arg):
    """
    GIVEN missing arguments
    WHEN creating an instance of DbtProjectConfig
    THEN a TypeError is raised
    """
    with pytest.raises(TypeError, match=f"missing 1 required positional argument: '{missing_arg}'"):
        _ = DbtProjectConfig(**kwargs)

