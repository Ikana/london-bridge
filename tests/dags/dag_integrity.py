"""
Test dag integrity and best practices.
"""
from airflow.models import DagBag

def test_dagbag_import():
    """
    Test that the DAGs in the DAG bag have no import errors.
    """
    dag_bag = DagBag(include_examples=False) #Loads all DAGs in $AIRFLOW_HOME/dags
    #Import errors aren't raised but captured to ensure all DAGs are parsed
    assert not dag_bag.import_errors

def test_dagbag_tags():
    """
    Test that the DAGs in the DAG bag have tags.
    """
    dag_bag = DagBag(include_examples=False) #Loads all DAGs in $AIRFLOW_HOME/dags
    for _, dag in dag_bag.dags.items():
        assert dag.tags #Assert dag.tags is not empty
