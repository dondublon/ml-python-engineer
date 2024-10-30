import json
import os
from unittest import TestCase
from unittest.mock import patch

from worker import Index


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
