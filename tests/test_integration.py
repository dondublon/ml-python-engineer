from unittest import TestCase
from .db_server import DBServerEmulator

class Test1(TestCase):
    def setUp(self):
        self.dbserver = DBServerEmulator()

    def test_1(self):
        pass