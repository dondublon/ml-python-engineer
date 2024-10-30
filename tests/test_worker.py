import json
import os
import io
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch, call, MagicMock

import pandas as pd
from pandas._testing import assert_frame_equal

from tests.dummy_server import DummyServer
from worker import Index, data_is_fresh, Worker, SERVICE_LAG

TEST_INDEX_FILE = 'test_index.json'

class TestIndex(TestCase):
    def setUp(self):
        with open(TEST_INDEX_FILE, 'w') as f:
            # noinspection PyTypeChecker
            json.dump({"got_data": {}}, f)
    def tearDown(self):
        os.remove(TEST_INDEX_FILE)

    @patch('worker.INDEX_FILE_NAME', TEST_INDEX_FILE)
    def test_index_present(self):
        index_obj = Index()
        index_obj.set_last_updated_date('companyA', '2024-10-25')
        got_date = index_obj.last_updated_date('companyA')
        self.assertEqual(got_date, '2024-10-25')
        with open(TEST_INDEX_FILE) as f:
            file_content = json.load(f)
            self.assertDictEqual({'got_data': {'companyA': {'last_updated': '2024-10-25'}}}, file_content)

    @patch('worker.INDEX_FILE_NAME', TEST_INDEX_FILE)
    def test_index_not_present(self):
        index_obj = Index()
        got_date = index_obj.last_updated_date('companyB')
        self.assertIsNone(got_date)


class TestDataIsFresh(TestCase):
    def test_data_is_fresh_none(self):
        result = data_is_fresh(None)
        self.assertFalse(result)

    def test_data_is_fresh_true(self):
        with patch('worker.now', return_value=datetime.fromisoformat('2024-10-25 12:00:00')):
            result = data_is_fresh('2024-10-25 11:00:00')
            self.assertTrue(result)

    def test_data_is_fresh_false(self):
        with patch('worker.now', return_value=datetime.fromisoformat('2024-10-25 12:00:00')):
            result = data_is_fresh('2024-10-24 11:00:00')
            self.assertFalse(result)


@patch('worker.get_db_server', return_value=DummyServer())
@patch('worker.INDEX_FILE_NAME', TEST_INDEX_FILE)
class TestMakeSnapshot(TestCase):
    def setUp(self):
        with open(TEST_INDEX_FILE, 'w') as f:
            # noinspection PyTypeChecker
            json.dump({"got_data": {}}, f)

    def tearDown(self):
        os.remove(TEST_INDEX_FILE)

    def test_pure_company(self, get_server):
        with patch('worker.Worker.make_snapshot_pure_company') as mock_make_snapshot_pure_company:
            obj = Worker()
            obj.make_snapshot('companyA')
            mock_make_snapshot_pure_company.assert_called_once_with('companyA')

    @patch('worker.data_is_fresh', return_value=False)
    def test_jointg_data_obsolette(self, get_server, data_is_fresh):
        with patch('worker.Worker.make_snapshot_pure_company') as mock_make_snapshot_pure_company:
            obj = Worker()
            obj.make_snapshot('jointg3')
            mock_make_snapshot_pure_company.call_count = 2
            self.assertListEqual(mock_make_snapshot_pure_company.call_args_list, [
                call('companyA', None),
                call('companyC', None),
            ])

    @patch('worker.data_is_fresh', return_value=True)
    def test_jointg_data_fresh(self, get_server, data_is_fresh):
        with patch('worker.Worker.make_snapshot_pure_company') as mock_make_snapshot_pure_company:
            obj = Worker()
            obj.make_snapshot('jointg3')
            mock_make_snapshot_pure_company.assert_not_called()


@patch('worker.get_db_server', return_value=DummyServer())
@patch('worker.INDEX_FILE_NAME', TEST_INDEX_FILE)
class TestMakeSnapshotPureCompany(TestCase):
    def setUp(self):
        with open(TEST_INDEX_FILE, 'w') as f:
            # noinspection PyTypeChecker
            json.dump({"got_data": {}}, f)

    def tearDown(self):
        os.remove(TEST_INDEX_FILE)

    def test_not_empty__old_exists(self, get_server):
        old_data = """
id,value,last_updated_date
1,  10,2024-01-01
2,  20,2024-01-02
                """
        old_data = pd.read_csv(io.StringIO(old_data), parse_dates=['last_updated_date'])
        new_data = """
id,value,last_updated_date
1,  10,2024-01-01
3,  30,2024-01-03
"""
        new_data = pd.read_csv(io.StringIO(new_data), parse_dates=['last_updated_date'])
        def mock_to_parquet(df, filename):
            expected = pd.DataFrame({
                'id': [2, 1, 3],
                'value': [20, 10, 30],
                'last_updated_date': pd.to_datetime(['2024-01-02', '2024-01-01', '2024-01-03'])
            }, index=[1, 0, 1])
            assert_frame_equal(df, expected)

        now = datetime(2024, 10, 30, tzinfo=timezone.utc) + SERVICE_LAG
        with patch('worker.Worker.get_snapshot_name', lambda self, company_name: f'test_{company_name}.prq'), \
             patch('pandas.read_parquet', return_value=old_data), \
             patch('pandas.DataFrame.to_parquet', mock_to_parquet), \
             patch('os.path.exists', return_value=True), \
             patch('worker.now', return_value=now), \
             patch('worker.Index.set_last_updated_date') as mock_set_last_updated_date:
            obj = Worker()
            obj.db_server.get_data = MagicMock(return_value=new_data)
            obj.make_snapshot_pure_company('companyA')
            mock_set_last_updated_date.assert_called_once_with('companyA', '2024-10-30 00:00:00.000000')

    def test_not_empty__old_not_exists(self, get_server):
        new_data = """
id,value,last_updated_date
1,  10,2024-01-01
3,  30,2024-01-03
"""
        new_data = pd.read_csv(io.StringIO(new_data), parse_dates=['last_updated_date'])
        def mock_to_parquet(df, filename):
            expected = pd.DataFrame({
                'id': [1, 3],
                'value': [10, 30],
                'last_updated_date': pd.to_datetime(['2024-01-01', '2024-01-03'])
            })
            assert_frame_equal(df, expected)

        now = datetime(2024, 10, 30, tzinfo=timezone.utc) + SERVICE_LAG
        with patch('worker.Worker.get_snapshot_name', lambda self, company_name: f'test_{company_name}.prq'), \
             patch('pandas.DataFrame.to_parquet', mock_to_parquet), \
             patch('os.path.exists', return_value=False), \
             patch('worker.now', return_value=now), \
             patch('worker.Index.set_last_updated_date') as mock_set_last_updated_date:
            obj = Worker()
            obj.db_server.get_data = MagicMock(return_value=new_data)
            obj.make_snapshot_pure_company('companyA')
            mock_set_last_updated_date.assert_called_once_with('companyA', '2024-10-30 00:00:00.000000')

    def test_empty(self, get_server):
        now = datetime(2024, 10, 30, tzinfo=timezone.utc) + SERVICE_LAG
        with patch('pandas.DataFrame.to_parquet') as mock_to_parquet, \
             patch('worker.now', return_value=now), \
             patch('worker.Index.set_last_updated_date') as mock_set_last_updated_date:
            obj = Worker()
            obj.db_server.get_data = MagicMock(return_value=pd.DataFrame())
            obj.make_snapshot('companyA')
            mock_to_parquet.assert_not_called()
            mock_set_last_updated_date.assert_called_once_with('companyA', '2024-10-30 00:00:00.000000')
