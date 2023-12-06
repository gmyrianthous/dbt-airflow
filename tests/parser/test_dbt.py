from dbt_airflow.parser.dbt import (
    Manifest,
    NodeDeps,
)


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


def test_load_method(mock_manifest_data):
    manifest = Manifest.load(data=mock_manifest_data)
    assert len(manifest.nodes) == 5


def test_load_method_with_include_by_tags(mock_manifest_data):
    manifest = Manifest.load(data=mock_manifest_data, include_tags=['hourly'])
    assert len(manifest.nodes) == 1
    assert 'model.mypackage.my_model' in manifest.nodes


def test_load_method_with_exclude_by_tags(mock_manifest_data):
    manifest = Manifest.load(data=mock_manifest_data, exclude_tags=['hourly'])
    assert len(manifest.nodes) == 4
    assert 'model.mypackage.my_model' not in manifest.nodes


def test_filter_tests_with_dependencies(mock_manifest_data):
    filtered_nodes = {
        'model.mypackage.another_model': {
            'name': 'another_model',
            'resource_type': 'model',
            'compiled': True,
            'depends_on': {'macros': [], 'nodes': ['seed.mypackage.my_seed']},
            'package_name': 'mypackage',
            'fqn': ['d', 'e', 'f'],
            'tags': [],
        }
    }

    all_nodes = mock_manifest_data['nodes']

    included_tests = Manifest.filter_tests_with_dependencies(all_nodes, filtered_nodes)

    assert list(included_tests.keys()) == [
        'model.mypackage.another_model',
        'test.mypackage.not_null_another_model_field_A.c9c3c572df',
    ]


def test_update_dependencies():
    filtered_nodes = {
        'node1': {'depends_on': {'nodes': ['node2', 'node3']}},  # 'node3' should be removed
        'node2': {'depends_on': {'nodes': ['node3']}},  # 'node3' should be removed
    }
    updated_nodes = Manifest.update_dependencies(filtered_nodes)

    assert updated_nodes['node1']['depends_on']['nodes'] == ['node2']
    assert updated_nodes['node2']['depends_on']['nodes'] == []


def test_include_by_tags(mock_manifest_data):
    included_nodes = Manifest.include_by_tags(mock_manifest_data, ['hourly'])
    assert 'model.mypackage.my_model' in included_nodes['nodes']


def test_exclude_by_tags(mock_manifest_data):
    included_nodes = Manifest.exclude_by_tags(mock_manifest_data, ['hourly'])
    assert 'model.mypackage.my_model' not in included_nodes['nodes']
