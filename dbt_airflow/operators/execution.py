from enum import Enum


class ExecutionOperator(Enum):
    """
    Specifies the operator type that will be used to create and execute Airflow Tasks
    """
    # BashOperator
    BASH = 'bash'

    # KubernetesPodOperator
    KUBERNETES = 'kubernetes'
