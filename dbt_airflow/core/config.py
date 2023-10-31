from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from dbt_airflow.core.task import ExtraTask
from dbt_airflow.exceptions import OperatorClassNotSupported


@dataclass
class DbtAirflowConfig:
    """Configuration specific to the functionality offered by dbt-airflow package"""
    create_sub_task_groups: Optional[bool] = True
    extra_tasks: Optional[List[ExtraTask]] = field(default_factory=list)
    operator_class: Optional[str] = 'BashOperator'
    operator_kwargs: Optional[Dict[Any, Any]] = field(default_factory=dict)

    def __post_init__(self):
        # Validate whether the input `operator_class` is among the supported ones.
        valid_operators = ['BashOperator', 'KubernetesPodOperator']
        if self.operator_class not in valid_operators:
            raise OperatorClassNotSupported(
                f'{self.operator_class} is not supported. Please choose one of {valid_operators}'
            )


@dataclass
class DbtProfileConfig:
    """Configuration specific to the dbt profile used while running dbt-airflow operators"""
    profiles_path: Path
    target: str


@dataclass
class DbtProjectConfig:
    project_path: Path
    manifest_path: Path
