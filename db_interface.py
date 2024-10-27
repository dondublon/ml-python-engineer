import pandas as pd


class DBInterface:
    def get_companies_info(self):
        raise NotImplemented('must inherit')

    def get_data(self, company, from_date) -> pd.DataFrame:
        raise NotImplemented('must inherit')