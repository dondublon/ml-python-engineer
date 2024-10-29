from unittest import TestCase

from companies import CompaniesManager
from tests.dummy_server import DummyServer


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