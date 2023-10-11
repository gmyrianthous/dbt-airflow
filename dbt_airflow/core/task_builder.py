import json
import logging
import os
from typing import List, Optional

from ..exceptions import ManifestNotFound, ManifestDataNotFound
from dbt_airflow.core.task import DbtAirflowTask, ExtraTask
from dbt_airflow.core.task_list import TaskList
from dbt_airflow.parser.dbt import DbtResourceType, Manifest, Node


class DbtAirflowTaskBuilder:
    """
    This class implements functionality that is used to populate the dbt project as an Airflow DAG,
    based on the user input. It also handles dbt model and test dependencies as well as any extra
    tasks specified by user.
    """

    def __init__(
        self,
        manifest_path: str,
        operator_class: str,
        extra_tasks: Optional[List[ExtraTask]] = None,
    ) -> None:
        self.manifest_path = os.path.abspath(manifest_path)
        self.manifest = self._load_manifest()
        self.nodes_with_tests = set()
        self.task_list = TaskList()

        if extra_tasks is None:
            self.extra_tasks = []
        else:
            self.extra_tasks = extra_tasks

        self.operator_class = operator_class

    def _add_extra_tasks(self) -> None:
        """
        Adds extra tasks specified in DbtTaskGroup
        """
        if not self.extra_tasks:
            logging.info('No extra tasks were provided. Skipping..')
            return

        for task in self.extra_tasks:
            self.task_list.append(task)

            # Iterate through extra task downstream dependencies
            for downstream_task_id in task.downstream_task_ids:
                downstream_task = self.task_list.find_task_by_id(downstream_task_id)

                # We want to check if the downstream task of extra task, is defined as
                # upstream dependency
                for upstream_task_id in task.upstream_task_ids:
                    upstream_task = self.task_list.find_task_by_id(upstream_task_id)
                    if upstream_task.task_id in downstream_task.upstream_task_ids:
                        downstream_task.upstream_task_ids.remove(upstream_task_id)

                #
                downstream_task.upstream_task_ids.add(task.task_id)

    def _create_task(self, manifest_node_name: str, node: Node) -> None:
        """
        Creates a task from the input manifest node. If the node also has test, an additional
        test task also gets created.
        """
        self.task_list.append(
            DbtAirflowTask.from_manifest_node(
                manifest_node_name,
                node,
                self.operator_class,
            )
        )
        if manifest_node_name in self.nodes_with_tests:
            self.task_list.append(
                DbtAirflowTask.test_task_from_manifest_node(
                    manifest_node_name,
                    node,
                    self.operator_class,
                )
            )

    def _create_tasks_with_tests(self) -> None:
        """
        For every dbt model, seed and snapshot we call `_create_task()` method that is responsible
        for creating a task per resource as well as an additional test task in case at least
        one test was defined for that particular resource.
        """
        logging.info('Creating tasks from manifest.json file')
        for manifest_node_name, node in self.manifest.nodes.items():
            if node.resource_type in [
                DbtResourceType.model, DbtResourceType.seed, DbtResourceType.snapshot
            ]:
                self._create_task(manifest_node_name, node)

    def _get_nodes_with_tests(self) -> None:
        """
        Creates a set consisting of models, seeds and snapshots for which at least one test is
        found within the manifest file.
        """
        for node_name, node in self.manifest.nodes.items():
            if node.resource_type == DbtResourceType.test:
                self.nodes_with_tests.update(node.depends_on.nodes)
        logging.info(
            f'Found {len(self.nodes_with_tests)} models, snapshots or seeds with dbt tests.'
        )

    def _load_manifest(self) -> Manifest:
        """
        Loads the test_manifest.json file created by dbt
        """
        logging.info(f'Loading {self.manifest_path} file')
        if not os.path.isfile(self.manifest_path):
            raise ManifestNotFound(
                f'Manifest file was not found in {self.manifest_path}. '
                f'Make sure that you are running dbt-airflow from your dbt project directory '
                f'and that the project is compiled (hint: `dbt compile`).'
            )

        with open(self.manifest_path, 'r') as f:
            data = json.load(f)
            if not data:
                raise ManifestDataNotFound('No data was found.')

        manifest = Manifest(**data)
        logging.info(f'{self.manifest_path} file was loaded successfully.')
        logging.info(f'Found {manifest.get_statistics()}')

        return manifest

    def _update_dependencies(self) -> None:
        """
        Goes through every test task in the existing `self.task_list` and re-arranges the
        dependencies of the nodes such that:

        For every task that references test's parent task its upstream tasks, it will remove the
        dependency to test's parent task and add a new dependency that references the test task.

        This is required given that test tasks are not part of dbt manifest file. `dbt-airflow`
        creates a test task for every model, seed and snapshot resource that have at least one
        test defined. Therefore, we then need to fix the dependencies between all these tasks
        such that whenever a model/snapshot/seed task that also has at least one test defined
        and is referenced in upstream dependencies of other tasks, it is replaced by the
        corresponding test task.
        """
        for task in self.task_list:
            if task.resource_type == DbtResourceType.test:
                parent_task = self.task_list.find_task_by_id(list(task.upstream_task_ids)[0])
                for task_other in self.task_list:
                    if task_other.resource_type != DbtResourceType.test \
                            and parent_task.task_id in task_other.upstream_task_ids:
                        task_other.upstream_task_ids.remove(parent_task.task_id)
                        task_other.upstream_task_ids.add(task.task_id)

    def build_tasks(self) -> TaskList:
        """
        Algorithm steps:
            1. Firstly, we extract the model, seed and snapshot nodes that have tests
            2. We then start building the tasks for every model, seed and snapshot node
                 i. Create a task for the mode, seed or snapshot
                ii. If the node has a test, create a test task as well
            3. Now that we have the full set of tasks, we need to go back and re-arrange
                the dependencies since we have created test tasks for the nodes.
                We couldn't have done this step in step 2, since if we were about to fixing
                the upstream dependencies of other nodes referencing the model/seed/snapshot task
                on the fly, we would've missed some given that at every iteration of creating
                tasks we don't have the full list of tasks. Therefore, this needs to be done
                at the very end
        """
        logging.info('Building tasks from manifest data')

        # Step 1: Extract all models, seeds and snapshots that have at least one test
        self._get_nodes_with_tests()

        # Step 2: Create tasks with tests
        self._create_tasks_with_tests()

        # Step 3: Update dependencies between models and tests created
        self._update_dependencies()

        # Step 4: Add extra tasks
        self._add_extra_tasks()

        logging.info(f'Created a TaskList of: {self.task_list.get_statistics()}')
        logging.warning(
            'The number of tests created could be less of the original number of tests reported '
            'in manifest file. This is due to the fact that the resulting Airflow DAG will only '
            'create a single test task, to execute all the tests for a particular model, '
            'snapshot or seed.'
        )

        return self.task_list
