"""
TODO:
    - Create argparser
        - flags:
            1. --create-task-groups
            2. --task-group-folder-depth
            2. --log-level
            3. --output-path
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
    task_loader = TaskLoader()
    task_loader.create_tasks()
    task_loader.write_to_file()
    logger.info('Done')
