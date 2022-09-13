"""
TODO:
    - Create argparser
"""

from dbt_airflow.manifest_processor import ManifestProcessor


def main():
    man_processor = ManifestProcessor()
    man_processor.load()
