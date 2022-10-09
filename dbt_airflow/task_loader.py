"""
This module contains functionality that is used when it comes to processing and extracting
information from the `manifest.json` file that gets generated by several dbt commands.

Facts:
- Snapshots are never an upstream dependency of any task
- Snapshots on seeds (if possible?) are not handled
- Models may have tests
- Snapshots may have tests
- Seeds may have tests

Strategy:
    - Iterate all over tasks and create tasks for each model, test, seed and snapshot found
        in the manifest file
    - Extract the dependencies between the tasks created
"""
import json
import logging
import os

from typing import Any

from dbt_airflow.exceptions import (
    ManifestNotFound,
    ManifestDataNotFound,
    InvalidDbtCommand,
)
from dbt_airflow.tasks import DbtNodeType, Task, TaskList


class TaskLoader:

    def __init__(self):
        self.tasks = TaskList()
        self.path = os.path.abspath('target/manifest.json')
        self.data = self.load_manifest()
        self.test_deps = self.load_test_dependencies()

        # TODO: Coming from argparse through constructor
        # self.output_path = None
        # self.task_group_folder_depth = None
        # self.log_level = None
        # self.create_task_groups = None

    def load_test_dependencies(self) -> set:
        """
        Loads all the dependencies found in test nodes within manifest file.
        This set of dependencies is then used to determine whether a test task should be created
        for model/seed/snapshot tasks inferred.
        """
        test_deps = set()
        for node, node_details in self.data['nodes'].items():
            upstream_dependencies = node_details['depends_on']['nodes']
            test_deps.update(upstream_dependencies)
        return test_deps

    def load_manifest(self) -> dict:
        """
        Loads the manifest.json file created by dbt
        """
        logging.info(f'Loading {self.path} file')
        if not os.path.isfile(self.path):
            raise ManifestNotFound(
                f'manifest.json file was not found in {self.path}. '
                f'Make sure that you are running dbt-airflow from your dbt project directory '
                f'and that the project is compiled (hint: `dbt compile`).'
            )

        with open(self.path, 'r') as f:
            data = json.load(f)

        logging.info(f'{self.path} file was loaded successfully.')
        logging.info(f'Found {self.get_manifest_statistics(data)}')

        return data

    def write_to_file(self):
        # os.makedirs(os.path.dirname(path), exist_ok=True)
        with open('output.json', 'w') as f:
            json.dump([task.to_dict() for task in self.tasks], f, indent=4, default=str)

    def create_tasks(self):
        """
        TODO
        """
        logging.info('Creating tasks from manifest data')
        if not self.data:
            raise ManifestDataNotFound('No data was found.')

        for node_name, node_details in self.data['nodes'].items():
            if Task.get_node_type(node_name) in [
                DbtNodeType.MODEL.value,
                DbtNodeType.SNAPSHOT.value,
                DbtNodeType.SEED.value,
            ]:
                self._create_task(node_name, node_details)

        self._fix_dependencies()

    def _create_task(self, node_name, node_details):
        task = Task.create_task_from_manifest_node(node_name, node_details)
        self.tasks.append(task)

        if task.dbt_node_name in self.test_deps:
            self.tasks.append(
                Task(
                    model_name=task.model_name,
                    dbt_command=DbtNodeType.TEST.value,
                    dbt_node_name=None,
                    upstream_tasks={task.name},
                    task_group=task.task_group,
                )
            )

    def _fix_dependencies(self) -> None:
        """
        This method is supposed to fix the dependencies between the created tasks.

        For every qualifying model/seed/snapshot a test task was also created. Therefore, we need
        to adjust the dependencies of the models so that the newly create test tasks are placed
        in the correct position within the dependency graph.

        Note:
            - The upstream dependencies of test tasks are already correct, since we created them
        """
        for task in self.tasks:
            if task.dbt_command == DbtNodeType.TEST.value:
                self._update_upstream_dependencies(task)

    def _update_upstream_dependencies(self, test_task: Task) -> None:
        """
        TODO
        """
        if test_task.dbt_command != DbtNodeType.TEST.value:
            raise InvalidDbtCommand(
                f'Task {test_task.name} with dbt command {test_task.dbt_command} is invalid.'
            )

        # First we need to find the corresponding task that is responsible for running the model
        model_run_task = self.tasks.find_task_by_name(list(test_task.upstream_tasks)[0])

        for task in self.tasks:
            if task.dbt_command != DbtNodeType.TEST.value \
                    and model_run_task.name in task.upstream_tasks:
                # Remove model run dependency
                task.upstream_tasks.remove(model_run_task.name)

                # Add model test dependency
                task.upstream_tasks.add(test_task.name)

    @staticmethod
    def get_manifest_statistics(data: dict[str, Any]) -> dict[str, int]:
        node_types = [Task.get_node_type(n) for n in data['nodes']]
        return {
            'models': node_types.count('model'),
            'tests': node_types.count('test'),
            'snapshots': node_types.count('snapshot'),
            'seeds': node_types.count('seed'),
        }
