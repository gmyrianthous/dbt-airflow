from pathlib import Path
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class DbtBaseOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self, 
        dbt_target_profile: str,
        dbt_profile_path: Path,
        dbt_project_path: Path,
        resource_name: str,
        dbt_base_command: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_target_profile = dbt_target_profile
        self.dbt_profile_path = dbt_profile_path
        self.dbt_project_path = dbt_project_path
        self.resource_name = resource_name
        self.dbt_base_command = dbt_base_command

    def get_dbt_command(self):
        dbt_command = ['dbt', '--no-write-json']

        if self.dbt_base_command:
            dbt_command.extend([self.dbt_base_command])
        
        dbt_command.extend(['--select', self.resource_name])
        dbt_command.extend(['--project-dir', self.dbt_project_path.as_posix()])
        dbt_command.extend(['--profiles-dir', self.dbt_profile_path.as_posix()])
        dbt_command.extend(['--target', self.dbt_target_profile])

        return dbt_command
