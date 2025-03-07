import unittest
from airflow.models import DagBag

class TestPageRankDag(unittest.TestCase):
    dag_bag = DagBag()
    dag = dag_bag.get_dag('etl-pagerank')

    def test_dag_loading(self):
        assert len(self.dag_bag.import_errors) == 0, 'No DAG loading errors'

    def test_task_dependencies(self):
        tasks = self.dag.tasks
        dependencies = {
            'extract_pageranks': {'downstream': ['load_pageranks'], 'upstream': []},
            'load_pageranks': {'downstream': [], 'upstream': ['extract_pageranks']}
        }
        for task in tasks:
            assert task.downstream_task_ids == set(dependencies[task.task_id]['downstream'])
            assert task.upstream_task_ids == set(dependencies[task.task_id]['upstream'])