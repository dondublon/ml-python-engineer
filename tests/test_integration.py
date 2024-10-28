import json
import glob
import os
import random

from unittest import TestCase
from unittest.mock import patch

import worker


random.seed(123)

class Test1(TestCase):
    def setUp(self):
        self.worker = worker.Worker()

    def tearDown(self):
        # erase the index
        with open('snapshots/index.json', 'w') as f:
            # noinspection PyTypeChecker
            json.dump({'got_data': {}}, f)
        for file_path in glob.glob(os.path.join('snapshots', '*.prq')):
            os.remove(file_path)

    def test_1(self):
        companies = ['ktkzxrffnd', 'grnescfhiv', 'bsjhgjllri', 'jointgs7']
        with patch('worker.now', self.worker.db_server.get_current_date):
            for date_step in range(12):  # 12 - to cover steps from 2023-03 to 2024-05
                for company in companies:
                    self.worker.make_snapshot(company)
                self.worker.db_server.increase_date()
