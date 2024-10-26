from unittest import TestCase
from worker import Worker

class Test1(TestCase):
    def setUp(self):
        # self.db_server = DBServerEmulator()
        self.worker = Worker()

    def test_1(self):
        companies = self.worker.companies.get_companies(True)
        print(companies)
