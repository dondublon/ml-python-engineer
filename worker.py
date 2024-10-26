from db_interface import DBInterface
# It requires additional work to hide this from server, I guess I can skip it.
from tests.db_server import DBServerEmulator


def get_db_server() -> DBInterface:
    db_server = DBServerEmulator()
    return db_server


class Worker:
    def __init__(self):
        self.db_server = get_db_server()
        self.companies_info = self.db_server.get_companies_info()