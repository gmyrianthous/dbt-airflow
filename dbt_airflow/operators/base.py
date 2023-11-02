from pathlib import Path
from typing import Any, List, Optional

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
        variables: Optional[str],
        **kwargs: Any,
    ) -> None:
        """
        Constructor for DbtBaseOperator

        :param dbt_target_profile: The name of the profile target, as specified in profiles.yml
        :param dbt_profile_path: The path to the profiles.yml file
        :param dbt_project_path: The path to the dbt project
        :param resource_name: The name of the dbt resource the operator will be executed for
        :param dbt_base_command: The base command that will be used when calling dbt CLI
        :param full_refresh: Whether a `--full-refresh` flag will be passed when running dbt
        :param variables: If not empty, will be passed as string and called with `--vars` flag
        :param kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.dbt_target_profile = dbt_target_profile
        self.dbt_profile_path = dbt_profile_path
        self.dbt_project_path = dbt_project_path
        self.resource_name = resource_name
        self.dbt_base_command = dbt_base_command
        self.full_refresh = full_refresh
        self.variables = variables

    def get_dbt_command(self) -> List[str]:
        """
        Constructs a list consisting of the building components of the dbt command
        that the operator will execute.
        """
        dbt_command = ['dbt', '--no-write-json', self.dbt_base_command]
        dbt_command.extend(['--project-dir', self.dbt_project_path.as_posix()])
        dbt_command.extend(['--profiles-dir', self.dbt_profile_path.as_posix()])
        dbt_command.extend(['--target', self.dbt_target_profile])
        dbt_command.extend(['--select', self.resource_name])

        # full refreshes are supported only by `run` and `seed` commands
        # https://docs.getdbt.com/reference/resource-configs/full_refresh
        if self.full_refresh and self.dbt_base_command in ['run', 'seed']:
            dbt_command.append('--full-refresh')

        if self.variables:
            dbt_command.extend(['--vars', f"'{self.variables}'"])

        return dbt_command
