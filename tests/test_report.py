import io
from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

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

    def test_get_df_jointg(self):
        def get_df_mock(self_, company_name):
            df = pd.DataFrame({
                'companyName': company_name,
                'pickup_date': ['2023-01-01', '2024-01-01', '2025-01-01']
            })
            return df
        expected = pd.DataFrame({
            'companyName': ['jointg3', 'jointg3', 'jointg3', 'jointg3', 'jointg3', 'jointg3'],
            'pickup_date': ['2023-01-01', '2024-01-01', '2025-01-01', '2023-01-01', '2024-01-01', '2025-01-01']
        })
        expected.index = [0, 1, 2, 0, 1, 2]
        with patch('report.ReportMaker.get_df_pure_company', get_df_mock):
            obj = self.report_maker()
            actual = obj.get_df_jointg('jointg3')
            assert_frame_equal(actual, expected)

    def test_make_report_by_df(self):
        data = """
companyName,pickup_date,transport_type,total_linehaul_cost,miles
jointg3,    2023-01-01,R,               10,                  5
jointg3,    2024-01-01,R,               20,                  10
jointg3,    2024-01-01,R,               20,                  10
jointg3,    2025-01-01,D,               30,                  5
jointg3,    2023-01-01,D,               40,                  10
jointg3,    2024-01-01,N,               50,                  5
jointg3,    2025-01-01,N,               60,                  10
        """
        data1 = """
companyName,pickup_date,transport_type,total_linehaul_cost,miles
jointg3,    2023-01-01,     D,           40,                  10
                """
        df = pd.read_csv(io.StringIO(data), parse_dates=['pickup_date'])
        obj = self.report_maker()
        actual = obj.make_report_by_df(df)
        expected = data = pd.DataFrame(
    data=[
        [1.0, None, 1.0, 4.0, None, 6.0],
        [None, 1.0, 1.0, None, 10.0, 6.0],
        [1.0, 2.0, None, 2.0, 2.0, None]
    ],
    columns=pd.MultiIndex.from_product([['volume', 'median_rate'], ['2023-01', '2024-01', '2025-01']]),
    index=pd.MultiIndex.from_tuples([
        ('jointg3', 'D'),
        ('jointg3', 'N'),
        ('jointg3', 'R')
    ], names=['companyName', 'transport_type'])
)
        assert_frame_equal(actual, expected)