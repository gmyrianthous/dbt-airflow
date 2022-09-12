class DbtAirflowException(Exception):
    pass


class ManifestNotFound(DbtAirflowException):
    """Raised when manifest.json file is not found under the expected path"""
    pass
