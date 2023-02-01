"""
To add:
    - Unit tests
    - How to include additional Airflow dependencies (external to dbt)

"""
import logging
import sys

from dbt_airflow.domain.task_builder import DbtAirflowTaskBuilder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def main():
    task_builder = DbtAirflowTaskBuilder('../example_targets/large/target/manifest.json')
    task_list = task_builder.build_tasks()
    task_list.write_to_file('output2.json')
