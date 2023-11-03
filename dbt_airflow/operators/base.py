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
        dbt_base_command: str,
        selectors: Optional[List[str]],
        exclude: Optional[List[str]],
        full_refresh: bool,
        no_write_json: bool,
        variables: Optional[str],
        **kwargs: Any,
    ) -> None:
        """
        Constructor for DbtBaseOperator

        :param dbt_target_profile: The name of the profile target, as specified in profiles.yml
        :param dbt_profile_path: The path to the profiles.yml file
        :param dbt_project_path: The path to the dbt project
        :param dbt_base_command: The base command that will be used when calling dbt CLI
        :param selectors: The entities that will be passed in `--select` flag
        :param exclude: Entities specified will be included in the `--exclude` flag
        :param full_refresh: Whether a `--full-refresh` flag will be passed when running dbt
        :param no_write_json: Indicates whether `--no-write-json` will be included when running
            the command
        :param variables: If not empty, will be passed as string and called with `--vars` flag
        :param kwargs: Keyword arguments
        """
        super().__init__(**kwargs)
        self.dbt_target_profile = dbt_target_profile
        self.dbt_profile_path = dbt_profile_path
        self.dbt_project_path = dbt_project_path
        self.dbt_base_command = dbt_base_command
        self.selectors = selectors
        self.exclude = exclude
        self.full_refresh = full_refresh
        self.no_write_json = no_write_json
        self.variables = variables

    def get_dbt_command(self) -> List[str]:
        """
        Constructs a list consisting of the building components of the dbt command
        that the operator will execute.
        """
        dbt_command = ['dbt']

        if self.no_write_json:
            dbt_command.append('--no-write-json')

        dbt_command.append(self.dbt_base_command)
        dbt_command.extend(['--project-dir', self.dbt_project_path.as_posix()])
        dbt_command.extend(['--profiles-dir', self.dbt_profile_path.as_posix()])
        dbt_command.extend(['--target', self.dbt_target_profile])

        if self.selectors:
            dbt_command.append('--select')
            dbt_command.extend(self.selectors)

        if self.exclude:
            dbt_command.append('--exclude')
            dbt_command.extend(self.exclude)

        # full refreshes are supported only by `run` and `seed` commands
        # https://docs.getdbt.com/reference/resource-configs/full_refresh
        if self.full_refresh and self.dbt_base_command in ['run', 'seed']:
            dbt_command.append('--full-refresh')

        if self.variables:
            dbt_command.extend(['--vars', f"'{self.variables}'"])

        return dbt_command
