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
        full_refresh: bool,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_target_profile = dbt_target_profile
        self.dbt_profile_path = dbt_profile_path
        self.dbt_project_path = dbt_project_path
        self.resource_name = resource_name
        self.dbt_base_command = dbt_base_command
        self.full_refresh = full_refresh

    def get_dbt_command(self):
        dbt_command = ['dbt', '--no-write-json', self.dbt_base_command]
        dbt_command.extend(['--project-dir', self.dbt_project_path.as_posix()])
        dbt_command.extend(['--profiles-dir', self.dbt_profile_path.as_posix()])
        dbt_command.extend(['--target', self.dbt_target_profile])
        dbt_command.extend(['--select', self.resource_name])

        # full refreshes are supported only by `run` and `seed` commands
        # https://docs.getdbt.com/reference/resource-configs/full_refresh
        if self.full_refresh and self.dbt_base_command in ['run', 'seed']:
            dbt_command.append('--full-refresh')

        return dbt_command
