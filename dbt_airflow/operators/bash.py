from typing import Any

from airflow.operators.bash import BashOperator

from dbt_airflow.operators.base import DbtBaseOperator


class DbtBashOperator(DbtBaseOperator, BashOperator):
    """
    This child class inherits from BashOperator and enriches its functionality such that various
    dbt commands can be executed as Airflow Operators. Given that dbt is a command-line tool, it
    makes sense to run these commands from bash (and thus BashOperator is used in the context of
    Airflow).
    """

    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__(bash_command='placeholder', **kwargs)
        self.bash_command = ' '.join(self.get_dbt_command())


class DbtRunBashOperator(DbtBashOperator):
    """
    Class for an Airflow Operator that executes a dbt `run` operation.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_base_command='run', **kwargs)


class DbtSeedBashOperator(DbtBashOperator):
    """
    Class for an Airflow Operator that executes a dbt `seed` operation.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_base_command='seed', **kwargs)


class DbtTestBashOperator(DbtBashOperator):
    """
    Class for an Airflow Operator that executes a dbt `test` operation.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_base_command='test', **kwargs)


class DbtSnapshotBashOperator(DbtBashOperator):
    """
    Class for an Airflow Operator that executes a dbt `snapshot` operation.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_base_command='snapshot', **kwargs)
