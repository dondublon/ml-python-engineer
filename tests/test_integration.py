import json
import glob
import logging
import os
import random
from datetime import datetime

from unittest import TestCase
from unittest.mock import patch

import pandas as pd

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
        self.reset_index()
        self.worker = worker.Worker()

    def tearDown(self):
        return


    def test_time_steps(self):
        companies_pure = ['ktkzxrffnd', 'grnescfhiv', 'bsjhgjllri']
        companies = companies_pure + ['jointgs7']
        with patch('worker.now', self.worker.db_server.get_current_date):
            for date_step in range(12):  # 12 - to cover steps from 2023-03 to 2024-05
                for company in companies:
                    self.worker.make_snapshot(company)
                self.worker.db_server.increase_date()
                logging.debug('--- New date step, nowt time is %s --- ', self.worker.db_server.get_current_date())
        # Assertions
        database0 = pd.read_parquet('data.prq')
        companies_for_jointg = self.worker.companies.get_jointg_companies('jointgs7')
        assert 'iwqnohooyt' in companies_for_jointg
        for company in companies_for_jointg:
            print('Checking company ', company)
            data_in_db = database0[database0.companyName==company]
            snapshot = pd.read_parquet(f'snapshots/{company}.prq')
            self.assertEqual(len(data_in_db), len(snapshot))


class TestReport(TestCommon):
    def setUp(self):
        self.reset_index()
        self.worker = worker.Worker()

    def test_report(self):
        companies = self.worker.companies.get_companies(include_jointgs=False)
        self.worker.db_server.current_date = datetime(2024, 6, 1)
        # week index: database.pickup_date.dt.year.astype(str) + '-' + database.pickup_date.dt.isocalendar().week.astype(str)
        for company in companies:
            self.worker.make_snapshot(company)