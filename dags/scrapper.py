"""
Scram data from the UK government website.
"""

from textwrap import dedent
from datetime import datetime

from airflow.kubernetes.secret import Secret

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

key_id = Secret(
    # Expose the secret as environment variable.
    deploy_type='env',
    # The name of the environment variable, since deploy_type is `env` rather
    # than `volume`.
    deploy_target='AWS_ACCESS_KEY_ID',
    # Name of the Kubernetes Secret
    secret='cluster-50d1ce18',
    # Key of a secret stored in this Secret object
    key='sAWS_ACCESS_KEY_ID')

secret_key = Secret(
    # Expose the secret as environment variable.
    deploy_type='env',
    # The name of the environment variable, since deploy_type is `env` rather
    # than `volume`.
    deploy_target='AWS_SECRET_ACCESS_KEY',
    # Name of the Kubernetes Secret
    secret='cluster-50d1ce18',
    # Key of a secret stored in this Secret object
    key='AWS_SECRET_ACCESS_KEY')

# Kubernetes airflow operator
with DAG(
    "scrapper",
    default_args={
        "depends_on_past": False,
    },
    description="Get data from the uk government website",
    tags=["scrapper", "london-bridge"],
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
) as dag:
    # kubernetes_pod_operator
    k8s_pod_operator = KubernetesPodOperator(
        task_id="scrapper",
        name="scrapper",
        namespace="airflow-10ff5c97",
        image="roikana/london-bridge-scrapper:latest",
        cmds=["python"],
        arguments=["exec.py"],
        labels={"purpose": "scrapper"},
        startup_timeout_seconds=5,
        get_logs=True,
        in_cluster=True,
        secret=[key_id, secret_key],
        dag=dag,
    )
    k8s_pod_operator.doc_md = dedent(
        """\
        #### Task Documentation
        This task runs the scrapper.py script in the container. The script
        scrapes the data from the uk government website.
        """
    )
