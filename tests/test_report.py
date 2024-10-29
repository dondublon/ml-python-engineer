from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from companies import CompaniesManager
from report import ReportMaker
from tests.dummy_server import DummyServer


class TestReport(TestCase):
    def report_maker(self):
        obj = ReportMaker(CompaniesManager(DummyServer()))
        return obj

    def test_get_df_pure_company(self):
        df = pd.DataFrame({
            'pickup_date': ['2023-01-01', '2024-01-01', '2025-01-01']
        })
        with patch('pandas.read_parquet', return_value=df):
            obj = self.report_maker()
            result = obj.get_df_pure_company('companyA')
            self.assertEqual(len(result), 1)
            self.assertEqual(result.pickup_date.iat[0], '2024-01-01')

    def test_make_report(self):
        with patch('report.ReportMaker.get_df_jointg', return_value=1) as mock_get_jointg, \
             patch('report.ReportMaker.get_df_pure_company', return_value=2) as get_df_pure_company, \
             patch('report.ReportMaker.make_report_by_df', lambda self, x: 10+x) as mock_get_report:
            obj = self.report_maker()
            actual = obj.make_report('some_name')
            self.assertEqual(actual, 12)

            actual = obj.make_report('jointg18')
            self.assertEqual(actual, 11)