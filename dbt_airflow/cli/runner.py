"""
TODO:
    - Create argparser
        - flags:
            1. --create-task-groups
            2. --manifest-path
            3. --task-group-folder-depth
            5. --write-output-to-file
            6. --output-path

To add:
    - Unit tests
    - How to include additional Airflow dependencies (external to dbt)

"""
import logging
import sys

from dbt_airflow.task_loader import TaskLoader


logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def main():
    task_loader = TaskLoader(
        manifest_path='example_targets/large/target/manifest.json',
        create_task_groups=True,
        task_group_folder_depth=-2,
    )
    task_list = task_loader.create_tasks()

    task_list.write_to_file('output.json')
    print(len(task_list))
    logger.info('Done')
