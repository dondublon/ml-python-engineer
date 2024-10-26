import json
import pandas as pd


class DBServerEmulator:
    def __init__(self):
        self.database = pd.read_parquet('data.prq')
        self.companies_info = pd.read_parquet('meta.prq')
        with open('snapshots/index.json') as f_index:
            self.index = json.load(f_index)

    def get_company_filter(self, company):
        if company[:5] == 'joint':
            company_filter = set(self.companies_info[self.companies_info[company] == 'true'].companyName)
        else:
            company_filter = {company}
        return company_filter

    def get_data(self, company):
        company_filter = self.get_company_filter(company)
        data = self.get_data_by(company_filter)
        return data

    def get_data_by(self, company_filter):
        return self.database[self.database.companyName.isin(company_filter)]