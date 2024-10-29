from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from companies import CompaniesManager
from report import ReportMaker
from tests.dummy_server import DummyServer


class TestReport(TestCase):
    def test_get_df_pure_company(self):
        df = pd.DataFrame({
            'pickup_date': ['2023-01-01', '2024-01-01', '2025-01-01']
        })
        with patch('pandas.read_parquet', return_value=df):
            obj = ReportMaker(CompaniesManager(DummyServer()))
            result = obj.get_df_pure_company('companyA')
            self.assertEqual(len(result), 1)
            self.assertEqual(result.pickup_date.iat[0], '2024-01-01')