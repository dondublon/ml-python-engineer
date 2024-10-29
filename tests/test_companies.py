from unittest import TestCase

import pandas as pd

from companies import CompaniesManager

class DummyServer:
    def get_companies_info(self):
        df = pd.DataFrame({
            'jointg1': [False, True],
            'jointg2': [False, True],
            'companyName': ['companyA', 'companyB']
        })
        return df


class TestCompanies(TestCase):
    def test_get_companies_only(self):
        obj = CompaniesManager(DummyServer())
        actual = obj.get_companies(False)
        self.assertListEqual(actual, ['companyA', 'companyB'])