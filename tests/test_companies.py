from unittest import TestCase

import pandas as pd

from companies import CompaniesManager

class DummyServer:
    def get_companies_info(self):
        df = pd.DataFrame({
            'jointg1': ['false', 'true', 'false'],
            'jointg2': ['true', 'false', 'false'],
            'jointg3': ['true', 'false', 'true'],
            'companyName': ['companyA', 'companyB', 'companyC']
        })
        return df


class TestCompanies(TestCase):
    def test_get_companies_only(self):
        obj = CompaniesManager(DummyServer())
        actual = obj.get_companies(False)
        self.assertListEqual(actual, ['companyA', 'companyB','companyC'])

    def test_get_companies_with_jointgs(self):
        obj = CompaniesManager(DummyServer())
        actual = obj.get_companies(True)
        self.assertListEqual(actual, ['companyA', 'companyB', 'companyC', 'jointg1', 'jointg2', 'jointg3'])

    def test_get_joint_companies(self):
        obj = CompaniesManager(DummyServer())
        actual = obj.get_jointg_companies('jointg3')
        self.assertListEqual(actual, ['companyA', 'companyC'])