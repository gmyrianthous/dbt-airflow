import logging
from typing import Any

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

from dbt_airflow.operators.base import DbtBaseOperator


class DbtKubernetesPodOperator(DbtBaseOperator, KubernetesPodOperator):

    @apply_defaults
    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.cmds = self.get_dbt_command()


class DbtRunKubernetesPodOperator(DbtKubernetesPodOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_base_command='run', **kwargs)


class DbtTestKubernetesPodOperator(DbtKubernetesPodOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_base_command='test', **kwargs)


class DbtSeedKubernetesPodOperator(DbtKubernetesPodOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_base_command='seed', **kwargs)


class DbtSnapshotKubernetesPodOperator(DbtKubernetesPodOperator):

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dbt_base_command='snapshot', **kwargs)
