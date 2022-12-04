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

from typing import Any, Dict, Set

from ..exceptions import ManifestNotFound, ManifestDataNotFound, InvalidDbtCommand
from .tasks import DbtNodeType, Task, TaskList


class TaskLoader:

    def __init__(
        self,
        manifest_path: str,
        create_task_groups: bool = False,
        task_group_folder_depth: int = -2,
    ):
        self.tasks = TaskList()
        self.path = os.path.abspath(manifest_path)
        self.create_task_groups = create_task_groups
        self.task_group_folder_depth = task_group_folder_depth

        self.data = self.load_manifest()
        self.test_deps = self.load_test_dependencies()

    def load_test_dependencies(self) -> Set[str]:
        """
        Loads all the dependencies found in test nodes within manifest file.
        This set of dependencies is then used to determine whether a test task should be created
        for model/seed/snapshot tasks inferred.
        """
        test_deps = set()
        for node, node_details in self.data['nodes'].items():
            if node_details['resource_type'] == DbtNodeType.TEST.value:
                upstream_dependencies = node_details['depends_on']['nodes']
                test_deps.update(upstream_dependencies)
        return test_deps

    def load_manifest(self) -> Dict:
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

    def create_tasks(self) -> TaskList:
        """
        Returns a TaskList instance consisting of the tasks created out of the
        input dbt manifest JSON file.
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
                self._create_task(
                    node_name, node_details, self.create_task_groups, self.task_group_folder_depth
                )

        self._fix_dependencies()

        logging.info(f'Created a TaskList of: {self.tasks.get_statistics()}')
        logging.warning(
            'The number of tests created could less of the original number of tests reported in '
            'manifest file. This is due to the fact that the resulting Airflow DAG will only '
            'create a single test task, to execute all the tests for a particular model, '
            'snapshot or seed.'
        )
        return self.tasks

    def _create_task(
        self,
        node_name: str,
        node_details: Dict[str, Any],
        create_task_group: bool,
        task_group_folder_depth: int,
    ) -> None:
        """
        Create a task for a model(run), snapshot or seed. If the model also has tests, create
        an additional test node
        """
        task = Task.create_task_from_manifest_node(
            node_name=node_name,
            node_details=node_details,
            create_task_group=create_task_group,
            task_group_folder_depth=task_group_folder_depth,
        )
        self.tasks.append(task)

        if task.dbt_node_name in self.test_deps:
            test_task = Task(
                model_name=task.model_name,
                dbt_command=DbtNodeType.TEST.value,
                dbt_node_name='',
                upstream_tasks={task.name},
                task_group=task.task_group if create_task_group else None,
            )
            self.tasks.append(test_task)

    def _fix_dependencies(self) -> None:
        """
        This method is supposed to fix the dependencies between the created tasks.

        For every qualifying model/seed/snapshot a test task was also created. Therefore, we need
        to adjust the dependencies of the models so that the newly create test tasks are placed
        in the correct position within the dependency graph.

        Note:
            - The upstream dependencies of test tasks are already correct, since we created them
                and they were not read by the manifest file
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
    def get_manifest_statistics(data: Dict[str, Any]) -> Dict[str, int]:
        """
        Returns the counts per node type found in the original manifest.json file, as generated
        by dbt commands.
        """
        node_types = [Task.get_node_type(n) for n in data['nodes']]
        return {
            'models': node_types.count('model'),
            'tests': node_types.count('test'),
            'snapshots': node_types.count('snapshot'),
            'seeds': node_types.count('seed'),
        }
