import json
import glob
import logging
import os
import random
from collections import defaultdict
from datetime import datetime, timezone

from unittest import TestCase
from unittest.mock import patch

import numpy as np
import pandas as pd
from pandas._testing import assert_frame_equal

from tests.call_logger import MethodCallLogger
import worker
import report



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
        self.worker.make_snapshot_pure_company = MethodCallLogger(self.worker.make_snapshot_pure_company)
        with patch('worker.now', self.worker.db_server.get_current_date):
            for date_step in range(12):  # 12 - to cover steps from 2023-03 to 2024-05
                for company in companies:
                    self.worker.make_snapshot(company)
                self.worker.db_server.increase_date()
                logging.debug('--- New date step, nowt time is %s --- ', self.worker.db_server.get_current_date())
        # Assertions
        database0 = pd.read_parquet('data.prq')
        database0['pickup_date'] = database0['pickup_date'].dt.tz_localize('UTC')
        database0['last_updated_date'] = database0['last_updated_date'].dt.tz_localize('UTC')
        database0['created_date'] = database0['created_date'].dt.tz_localize('UTC')
        companies_for_jointg = self.worker.companies.get_jointg_companies('jointgs7')
        assert 'iwqnohooyt' in companies_for_jointg
        for company in companies_for_jointg:
            print('Checking company ', company)
            data_in_db = database0[database0.companyName==company]
            snapshot = pd.read_parquet(f'snapshots/{company}.prq')
            self.assertEqual(len(data_in_db), len(snapshot))
            assert_frame_equal(data_in_db.sort_values(by='id').reset_index(drop=True),
                               snapshot.sort_values(by='id').reset_index(drop=True),
                               check_exact=True)
        # Left check self.worker.make_snapshot_pure_company
        method = self.worker.make_snapshot_pure_company
        self.assertEqual(len(method.call_args_list), 4*12)  # 4=companies for joint7
        calls_by_company = defaultdict(list)
        for call in method.call_args_list:
            calls_by_company[call[0][0]].append(call[0][1] if len(call[0])>1 else None)
        for company in companies_pure:
            self.assertEqual(calls_by_company[company], [None] * 12)
        self.assertEqual(calls_by_company['iwqnohooyt'], [None, '2023-08-01 12:00:00.000000', '2023-09-01 12:00:00.000000', '2023-10-01 12:00:00.000000', '2023-11-01 12:00:00.000000', '2023-12-01 12:00:00.000000', '2024-01-01 12:00:00.000000', '2024-02-01 12:00:00.000000', '2024-03-01 12:00:00.000000', '2024-04-01 12:00:00.000000', '2024-05-01 12:00:00.000000', '2024-06-01 12:00:00.000000'])


class TestReport(TestCommon):
    def setUp(self):
        self.reset_index()
        self.worker = worker.Worker()
        self.worker.db_server.database['year_week'] = self.worker.db_server.database['pickup_date'].apply(report.gregorian_year_week)
        pass

    def test_report(self):
        companies = self.worker.companies.get_companies(include_jointgs=False)
        database0 = self.worker.db_server.database
        self.worker.db_server.current_date = datetime(2024, 6, 1, tzinfo=timezone.utc)
        for company in companies:
            self.worker.make_snapshot(company)
        report_maker = report.ReportMaker(self.worker.companies)
        # Pure companies:
        companies = ['bawutedhpu', 'bwiwxhznom', 'dbijifujtu', 'gvdpftceua', 'hxgbojleqe',
                     'jointgs7', 'jointGS', 'jointgs14', 'jointgs10',
                     'kfpeljbvly', 'mekaofdvfc', 'nxnnpaland', 'rleyktmksb', 'ubhqnnewtx',
                     'vypsclbgdn', 'zmqigygwhu']
        def get_company_condition(company_name):
            if company_name.startswith('joint'):
                joingt_companies = self.worker.companies.get_jointg_companies(company_name)
                condition = database0.companyName.isin(joingt_companies)
            else:
                condition = database0.companyName==company
            return condition

        transport_types = ['R', 'N', 'D']
        for company in companies:
            report_company = report_maker.make_report(company)
            company_condition = get_company_condition(company)
            volume_columns = [col[1] for col in report_company.columns if col[0]=='volume']
            # We assume that median columns are the same
            for col in volume_columns:
                year, week_no = col.split('-')
                year = int(year)
                for tt in transport_types:
                    db_fragment = database0[company_condition &
                                            (database0.transport_type==tt) &
                                            (database0.year_week == col) &
                                            (database0.pickup_date.dt.year == year)
                                            ]
                    report_volume = report_company.loc[(company, tt), ('volume', col)]
                    if np.isnan(report_volume):
                        self.assertTrue(db_fragment.empty)
                    else:
                        self.assertEqual(len(db_fragment), int(report_volume))
                    median_report = report_company.loc[(company, tt), ('median_rate', col)]
                    median_db = (db_fragment['total_linehaul_cost'].astype(float) / db_fragment['miles']).median()
                    if np.isnan(median_report):
                        self.assertTrue(np.isnan(median_db))
                    else:
                        self.assertEqual(median_report, np.float64(median_db))
