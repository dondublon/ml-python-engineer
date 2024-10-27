import json
import pandas as pd

from db_interface import DBInterface


class DBServerEmulator(DBInterface):
    def __init__(self):
        self.database = pd.read_parquet('data.prq')
        self.companies_info = pd.read_parquet('meta.prq')

    def get_company_filter(self, company):
        if company[:5] == 'joint':
            company_filter = set(self.companies_info[self.companies_info[company] == 'true'].companyName)
        else:
            company_filter = {company}
        return company_filter

    def get_date_filter(self, from_date):
        if not from_date:
            return None
        date_filter = self.database.last_updated_date > from_date
        return date_filter

    def get_data(self, company, from_date):
        company_filter = self.get_company_filter(company)
        date_filter = self.get_date_filter(from_date)
        data = self.get_data_by(company_filter, date_filter)
        return data

    def get_data_by(self, company_filter, date_filter):
        total_filter = self.database.companyName.isin(company_filter)
        if date_filter is not None:
            total_filter &= date_filter
        return self.database[total_filter]

    def get_companies_info(self):
        return self.companies_info
