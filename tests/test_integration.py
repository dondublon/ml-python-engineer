from unittest import TestCase
from worker import Worker
import random


random.seed(123)

class Test1(TestCase):
    def setUp(self):
        # self.db_server = DBServerEmulator()
        self.worker = Worker()

    def test_1(self):
        companies = ['ktkzxrffnd', 'grnescfhiv', 'bsjhgjllri']
        for company in companies:
            self.worker.make_snapshot(company)
