class DbtAirflowException(Exception):
    pass


class ManifestNotFound(DbtAirflowException):
    """Raised when manifest.json file is not found under the expected path"""
    pass


class ManifestDataNotFound(DbtAirflowException):
    """Raised when a ManifestProcessor instance has empty data"""
    pass


class DbtCommandNotSupported(DbtAirflowException):
    """Raised when the given dbt command is not supported by dbt-airflow"""
    pass


class InvalidDbtCommand(DbtAirflowException):
    """Raised when the given task has an unexpected dbt command"""
    pass


class DuplicateTaskName(DbtAirflowException):
    """Raised when a duplicate task is identified within a TaskList"""
    pass


class TaskGroupExtractionError(DbtAirflowException):
    """Raised when a Task Group cannot be extracted using the input index provided"""
    pass
