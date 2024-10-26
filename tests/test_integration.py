from unittest import TestCase
from .db_server import DBServerEmulator
from worker import Worker

class Test1(TestCase):
    def setUp(self):
        # self.db_server = DBServerEmulator()
        self.worker = Worker()

    def test_1(self):
        pass