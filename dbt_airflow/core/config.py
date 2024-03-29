from dataclasses import dataclass, field

from pathlib import Path
from typing import Any, Dict, List, Optional

from dbt_airflow.core.task import ExtraTask
from dbt_airflow.exceptions import ExecutionOperatorNotSupported
from dbt_airflow.operators.execution import ExecutionOperator


@dataclass
class DbtAirflowConfig:
    """Configuration specific to the functionality offered by dbt-airflow package"""

    create_sub_task_groups: Optional[bool] = True
    extra_tasks: Optional[List[ExtraTask]] = field(default_factory=list)
    execution_operator: Optional[ExecutionOperator] = ExecutionOperator.BASH
    operator_kwargs: Optional[Dict[Any, Any]] = field(default_factory=dict)
    include_tags: Optional[List[str]] = field(default_factory=list)
    exclude_tags: Optional[List[str]] = field(default_factory=list)
    model_tasks_operator_kwargs: Optional[Dict[Any, Any]] = field(default_factory=dict)
    seed_tasks_operator_kwargs: Optional[Dict[Any, Any]] = field(default_factory=dict)
    snapshot_tasks_operator_kwargs: Optional[Dict[Any, Any]] = field(default_factory=dict)
    test_tasks_operator_kwargs: Optional[Dict[Any, Any]] = field(default_factory=dict)

    # dbt native commands
    select: Optional[List[str]] = field(default_factory=list)
    exclude: Optional[List[str]] = field(default_factory=list)
    full_refresh: Optional[bool] = False
    no_write_json: Optional[bool] = True
    variables: Optional[str] = None
    no_partial_parse: Optional[bool] = False
    warn_error: Optional[bool] = False
    warn_error_options: Optional[str] = None

    def __post_init__(self) -> None:
        self._validate_execution_operator()

    def _validate_execution_operator(self) -> None:
        # Validate whether the input `execution_operator` is among the supported ones.
        supported_operators = [op for op in ExecutionOperator]
        if self.execution_operator not in supported_operators:
            raise ExecutionOperatorNotSupported(
                f'{self.execution_operator} is not supported. '
                f'Please choose one of {supported_operators}'
            )


@dataclass
class DbtProfileConfig:
    """Configuration specific to the dbt profile used while running dbt-airflow operators"""

    profiles_path: Path
    target: str


@dataclass
class DbtProjectConfig:
    """Configuration specific to the dbt project the dbt-airflow package will unwrap tasks for"""

    project_path: Path
    manifest_path: Path
