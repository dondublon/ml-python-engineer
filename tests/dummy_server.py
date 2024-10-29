import pandas as pd


class DummyServer:
    def get_companies_info(self):
        df = pd.DataFrame({
            'jointg1': ['false', 'true', 'false'],
            'jointg2': ['true', 'false', 'false'],
            'jointg3': ['true', 'false', 'true'],
            'companyName': ['companyA', 'companyB', 'companyC']
        })
        return df
