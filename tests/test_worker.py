import json
import os
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch

from tests.dummy_server import DummyServer
from worker import Index, data_is_fresh, Worker

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

    def test_pure_company(self, g):
        with patch('worker.Worker.make_snapshot_pure_company') as mock_make_snapshot_pure_company:
            obj = Worker()
            obj.make_snapshot('companyA')
            mock_make_snapshot_pure_company.assert_called_once_with('companyA')