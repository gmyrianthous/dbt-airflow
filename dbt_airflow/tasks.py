from enum import Enum, auto
from typing import List, Optional


class DbtCommand(Enum):
    """
    Constructs an enumeration class that corresponds to dbt commands that are relevant to
    dbt-airflow package
    """
    RUN = auto()
    TEST = auto()
    SNAPSHOT = auto()
    SEED = auto()


class DbtAirflowTask:
    """
    This class corresponds to a single internal task and every instance gets constructed based
    on information extracted from dbt manifest file.
    """

    def __init__(
        self,
        name: str,
        dbt_command: DbtCommand,
        downstream_tasks: Optional[List[str]] = [],
    ):
        self.name = name
        self.dbt_command = dbt_command
        self.downstream_tasks = downstream_tasks
