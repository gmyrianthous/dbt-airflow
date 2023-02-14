import pytest

from dbt_airflow.domain.model import DbtAirflowTask
from dbt_airflow.parser.dbt import DbtResourceType, Manifest


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
