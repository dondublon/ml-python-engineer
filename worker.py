import os
import json

from db_interface import DBInterface
# It requires additional work to hide this from server, I guess I can skip it.
from tests.db_server import DBServerEmulator
from companies import CompaniesManager


def get_db_server() -> DBInterface:
    db_server = DBServerEmulator()
    return db_server

class Index:
    # In real job, could be implemented by Redis.
    def __init__(self):
        with open('snapshots/index.json') as f_index:
            index0 = json.load(f_index)
            self.index = index0['got_data']

    def last_updated_date(self, company_name):
        result = self.index.get(company_name, {'last_updated': None})['last_updated']
        return result

    def set_last_updated_date(self, company_name, date):
        self.index[company_name] = {'last_updated': str(date)}
        with open('snapshots/index.json', 'w') as f:
            json.dump({'got_data': self.index}, f, indent=4)

class Worker:
    def __init__(self):
        self.db_server = get_db_server()
        self.companies = CompaniesManager(self.db_server)
        self.index = Index()

    def make_snapshot(self, company_name):
        # company_name could be pure company of jointg.
        if company_name.startswith('joint'):
            pass
        else:
            self.make_snapshot_pure_company(company_name)

    def make_snapshot_pure_company(self, company_name):
        last_updated_date = self.index.last_updated_date(company_name)
        data = self.db_server.get_data(company_name, last_updated_date)
        if not data.empty:
            max_data_date = data.last_updated_date.max()
            if os.path.exists(f'snapshots/{company_name}.prq'):
                # TODO add to the previous snapshot
                pass
            self.index.set_last_updated_date(company_name, max_data_date)