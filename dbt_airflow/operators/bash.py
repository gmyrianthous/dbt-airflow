from pathlib import Path
from typing import Any, Optional

from airflow.operators.bash import BashOperator
from airflow.utils.decorators import apply_defaults


class DbtBashOperator(BashOperator):
    """
    This child class inherits from BashOperator and enriches its functionality such that various
    dbt commands can be executed as Airflow Operators. Given that dbt is a command-line tool, it
    makes sense to run these commands from bash (and thus BashOperator is used in the context of
    Airflow).
    """
    @apply_defaults
    def __init__(
        self,
        dbt_target_profile: str,
        dbt_profile_path: Path,
        dbt_project_path: Path,
        resource_name: str,
        dbt_command: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.dbt_target_profile = dbt_target_profile
        self.dbt_profile_path = dbt_profile_path
        self.dbt_project_path = dbt_project_path
        self.resource_name = resource_name
        self.dbt_command = dbt_command

        super().__init__(bash_command=self._get_bash_command(), **kwargs)

    def _get_bash_command(self) -> str:
        """
        Returns the bash command to be executed from BashOperator parent class on Airflow.
        We specify the `--no-write-json` flag in order to avoid overwriting the `manifest.json`
        file that sits at the heart of this implementation. Therefore, the creation and versioning
        of that file remains responsibility of the user running the library.
        """
        return f'cd {self.dbt_project_path.as_posix()} && ' \
               f'dbt --no-write-json ' \
               f'{self.dbt_command} ' \
               f'--profiles-dir {self.dbt_profile_path.as_posix()} ' \
               f'--target {self.dbt_target_profile} ' \
               f'--select {self.resource_name}'


class DbtRunBashOperator(DbtBashOperator):
    """
    Class for an Airflow Operator that executes a dbt `run` operation.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='run', **kwargs)


class DbtSeedBashOperator(DbtBashOperator):
    """
    Class for an Airflow Operator that executes a dbt `seed` operation.
    """
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='seed', **kwargs)


class DbtTestBashOperator(DbtBashOperator):
    """
    Class for an Airflow Operator that executes a dbt `test` operation.
    """
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='test', **kwargs)


class DbtSnapshotBashOperator(DbtBashOperator):
    """
    Class for an Airflow Operator that executes a dbt `snapshot` operation.
    """
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='snapshot', **kwargs)
