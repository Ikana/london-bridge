from textwrap import dedent
from datetime import datetime

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

# Kubernetes airflow operator
with DAG(
    "k8s",
    default_args={
        "depends_on_past": False,
    },
    description="Tests of k8s operator",
    tags=["example"],
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
) as dag:
    # kubernetes_pod_operator
    k8s_pod_operator = KubernetesPodOperator(
        task_id="k8s_pod_operator",
        name="k8s_pod_operator",
        namespace="airflow-10ff5c97",
        image="roikana/london-bridge-base:latest",
        cmds=["python", "-c"],
        arguments=["print('hello world')"],
        labels={"test": "test"},
        startup_timeout_seconds=5,
        get_logs=True,
        in_cluster=True,
        dag=dag,
    )
    k8s_pod_operator.doc_md = dedent(
        """\
        #### Task Documentation
        You can document your task using the attributes `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
        """
    )
