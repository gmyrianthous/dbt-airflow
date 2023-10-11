import logging
from pathlib import Path
from typing import Any, Optional

from airflow.utils.decorators import apply_defaults

try:
    # apache-airflow-providers-cncf-kubernetes >= 7.4.0
    from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
        convert_env_vars,
    )
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:
    try:
        # apache-airflow-providers-cncf-kubernetes < 7.4.0
        from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
            KubernetesPodOperator,
        )
    except ImportError as error:
        logging.exception(error)
        raise ImportError(
            'Could not import KubernetesPodOperator. Make sure you have '
            'apache-airflow-providers-cncf-kubernetes installed.'
        )


class DbtKubernetesPodOperator(KubernetesPodOperator):

    @apply_defaults
    def __init__(
        self,
        name: str,
        namespace: str,
        image: str,
        dbt_target_profile: str,
        dbt_profile_path: Path,
        dbt_project_path: Path,
        resource_name: str,
        dbt_command: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.name = name
        self.namespace = namespace
        self.image = image
        self.dbt_target_profile = dbt_target_profile
        self.dbt_profile_path = dbt_profile_path
        self.dbt_project_path = dbt_project_path
        self.resource_name = resource_name
        self.dbt_command = dbt_command

        super().__init__(
            name=self.name,
            namespace=self.namespace,
            image=self.image,
            cmds=self._get_cmds(),
            **kwargs,
        )

    def _get_cmds(self):
        return [
            'dbt', self.dbt_command,
            '--select', self.resource_name,
            '--project-dir', self.dbt_project_path.as_posix(),
            '--profiles-dir', self.dbt_profile_path.as_posix(),
            '--target', self.dbt_target_profile
        ]


class DbtRunKubernetesPodOperator(DbtKubernetesPodOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='run', **kwargs)


class DbtTestKubernetesPodOperator(DbtKubernetesPodOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='test', **kwargs)


class DbtSeedKubernetesPodOperator(DbtKubernetesPodOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='seed', **kwargs)


class DbtSnapshotKubernetesPodOperator(DbtKubernetesPodOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_command='snapshot', **kwargs)
