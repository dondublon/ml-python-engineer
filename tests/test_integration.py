import json
import glob
import logging
import os
import random
from datetime import datetime

from unittest import TestCase
from unittest.mock import patch

import worker


random.seed(123)
class TestCommon(TestCase):
    def reset_index(self):
        with open('snapshots/index.json', 'w') as f:
            # noinspection PyTypeChecker
            json.dump({'got_data': {}}, f)
        for file_path in glob.glob(os.path.join('snapshots', '*.prq')):
            os.remove(file_path)

class TestTimeSteps(TestCommon):
    def setUp(self):
        self.worker = worker.Worker()
        self.reset_index()

    def tearDown(self):
        return


    def test_time_steps(self):
        companies = ['ktkzxrffnd', 'grnescfhiv', 'bsjhgjllri', 'jointgs7']
        with patch('worker.now', self.worker.db_server.get_current_date):
            for date_step in range(12):  # 12 - to cover steps from 2023-03 to 2024-05
                for company in companies:
                    self.worker.make_snapshot(company)
                self.worker.db_server.increase_date()
                logging.debug('--- New date step, nowt time is %s --- ', self.worker.db_server.get_current_date())


class TestReport(TestCommon):
    def setUp(self):
        self.worker = worker.Worker()
        self.reset_index()

    def test_report(self):
        companies = self.worker.companies.get_companies(include_jointgs=False)
        self.worker.db_server.current_date = datetime(2024, 6, 1)
        # week index: database.pickup_date.dt.year.astype(str) + '-' + database.pickup_date.dt.isocalendar().week.astype(str)
        for company in companies:
            self.worker.make_snapshot(company)