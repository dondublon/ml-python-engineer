from datetime import timedelta, datetime, timezone
import json
import logging
import os
from typing import Optional

import pandas as pd

from db_interface import DBInterface
# It requires additional work to hide this from server, I guess I can skip it.
from tests.db_server import DBServerEmulator
from companies import CompaniesManager

logging.basicConfig(level=logging.DEBUG)

#FRESH_DATA_THRESHOLD = timedelta(days=1)
SERVICE_LAG = timedelta(minutes=1)

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
            # noinspection PyTypeChecker
            json.dump({'got_data': self.index}, f, indent=4)

def now():
    return datetime.now(timezone.utc)

def data_is_fresh(updated_date: Optional[str]):
    if updated_date is None:
        return False
    date_obj = datetime.fromisoformat(updated_date)
    fresh = date_obj.date() >= now().date()
    return fresh


class Worker:
    def __init__(self):
        self.db_server = get_db_server()
        self.companies = CompaniesManager(self.db_server)
        self.index = Index()

    def make_snapshot(self, company_name):
        # company_name could be pure company of jointg.
        if company_name.startswith('joint'):
            # We need 'fresh' data for each company.
            # We assume the data is 'fresh' if the last_updated_date more is today,
            # (even if the time of the day was earlier),
            # As an option, we can use now()-FRESH_DATA_THRESHOLD.
            # This setting potentially could be changed, for example, for 1 hour.
            # to avoid multiple requests that return small amount of data.
            jointg_companies = self.companies.get_jointg_companies(company_name)
            # can be in parallel in the real job
            logging.debug('Requested data for %s, enumerating %s', company_name, jointg_companies)
            for company in jointg_companies:
                last_updated_date = self.index.last_updated_date(company)
                if not data_is_fresh(last_updated_date):
                    logging.debug('\tData is obsolete for %s, making snapshot...', company)
                    self.make_snapshot_pure_company(company, last_updated_date)
                else:
                    logging.debug('\tData is fresh for %s', company)
        else:
            self.make_snapshot_pure_company(company_name)

    def make_snapshot_pure_company(self, company_name, last_updated_date_cache=None):
        logging.debug("Making snapshot for company %s", company_name)
        last_updated_date = last_updated_date_cache or self.index.last_updated_date(company_name)
        new_data = self.db_server.get_data(company_name, last_updated_date)
        # logging.debug('\tGot new data, %d lines', len(new_data))
        assume_updated_to = now() - SERVICE_LAG
        if not new_data.empty:
            filename = f'snapshots/{company_name}.prq'
            max_data_date = new_data.last_updated_date.max().to_pydatetime()
            max_data_date = max_data_date.replace(tzinfo=timezone.utc)
            if os.path.exists(filename):
                logging.debug('\tData already exists, concatenating')
                prev_data = pd.read_parquet(filename)
                new_result_data = pd.concat([prev_data, new_data], axis=0)
                new_result_data.to_parquet(filename)
            else:
                new_data.to_parquet(filename)
            date_to_write = max(max_data_date, assume_updated_to)
        else:
            logging.debug('\tNo new data, updating timestamp to %s', assume_updated_to)
            date_to_write = assume_updated_to
        date_to_write_str = date_to_write.strftime('%Y-%m-%d %H:%M:%S.%f')
        self.index.set_last_updated_date(company_name, date_to_write_str)
