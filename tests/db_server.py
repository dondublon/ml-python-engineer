import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

from db_interface import DBInterface


class DBServerEmulator(DBInterface):
    def __init__(self):
        self.database = pd.read_parquet('data.prq')
        self.companies_info = pd.read_parquet('meta.prq')
        # this date will be increased during the test:
        self.current_date = datetime.datetime(2023, 8, 1, hour=12, minute=1)

    def increase_date(self, timedelta=None):
        timedelta = timedelta or relativedelta(months=1)
        self.current_date += timedelta
        pass

    def get_current_date(self):
        return self.current_date

    def get_company_filter(self, company):
        if company[:5] == 'joint':
            company_filter = set(self.companies_info[self.companies_info[company] == 'true'].companyName)
        else:
            company_filter = {company}
        return company_filter

    def get_date_from_filter(self, from_date):
        if not from_date:
            return None
        try:
            date_filter = self.database.last_updated_date > from_date
        except:
            date_filter = self.database.last_updated_date > from_date
        return date_filter

    def get_date_to_filter(self):
        date_filter = self.database.last_updated_date <= self.current_date
        return date_filter

    def get_data(self, company, from_date) -> pd.DataFrame:
        company_filter = self.get_company_filter(company)
        date_filter = self.get_date_from_filter(from_date)
        data = self.get_data_by(company_filter, date_filter)
        return data

    def get_data_by(self, company_filter, date_filter):
        total_filter = self.database.companyName.isin(company_filter)
        if date_filter is not None:
            total_filter &= date_filter
        # our internal filter, for test purpose only:
        date_to_filter = self.get_date_to_filter()
        total_filter &= date_to_filter
        return self.database[total_filter]

    def get_companies_info(self):
        return self.companies_info
