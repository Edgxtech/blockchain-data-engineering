import unittest
from airflow.models import DagBag

class TestPageRankDag(unittest.TestCase):
    dag_bag = DagBag()
    dag = dag_bag.get_dag('etl-vol')

    def test_dag_loading(self):
        assert len(self.dag_bag.import_errors) == 0, 'No DAG loading errors'

    def test_task_dependencies(self):
        tasks = self.dag.tasks
        dependencies = {
            'extract_vols': {'downstream': ['transform_vols_by_block'], 'upstream': []},
            'transform_vols_by_block': {'downstream': ['load_vols_by_block','transform_vols_all_time'], 'upstream': ['extract_vols']},
            'load_vols_by_block': {'downstream': [], 'upstream': ['transform_vols_by_block']},
            'transform_vols_all_time': {'downstream': ['load_vols_all_time'], 'upstream': ['transform_vols_by_block']},
            'load_vols_all_time': {'downstream': [], 'upstream': ['transform_vols_all_time']},
        }
        for task in tasks:
            assert task.downstream_task_ids == set(dependencies[task.task_id]['downstream'])
            assert task.upstream_task_ids == set(dependencies[task.task_id]['upstream'])