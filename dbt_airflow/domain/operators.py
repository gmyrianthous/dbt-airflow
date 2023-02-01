from pathlib import Path
from typing import Any

from airflow.operators.bash import BashOperator
from airflow.utils.decorators import apply_defaults


class DbtOperator(BashOperator):
    @apply_defaults
    def __init__(
        self,
        dbt_target_profile: str,
        dbt_profile_path: Path,
        dbt_project_path: Path,
        resource_name: str,
        dbt_command: str = None,
        **kwargs: Any,
    ) -> None:
        self.dbt_target_profile = dbt_target_profile
        self.dbt_profile_path = dbt_profile_path
        self.dbt_project_path = dbt_project_path
        self.resource_name = resource_name
        self.dbt_command = dbt_command

        super().__init__(bash_command=self._get_bash_command(), **kwargs)

    def _get_bash_command(self) -> str:
        return f'cd {self.dbt_project_path.as_posix()} && ' \
               f'dbt --no-write-json ' \
               f'{self.dbt_command} ' \
               f'--profiles-dir {self.dbt_profile_path.as_posix()} ' \
               f'--target {self.dbt_target_profile} ' \
               f'--select {self.resource_name}'


class DbtRunOperator(DbtOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='run', **kwargs)


class DbtSeedOperator(DbtOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='seed', **kwargs)


class DbtTestOperator(DbtOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='test', **kwargs)


class DbtSnapshotOperator(DbtOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='snapshot', **kwargs)
