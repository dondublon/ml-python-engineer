import pandas as pd
import numpy as np

LOWER_BORDER = '2024-01-01'
UPPER_BORDER = '2024-05-20'


class ReportMaker:
    def __init__(self, companies_manager):
        self.companies_info = companies_manager

    def get_df_pure_company(self, company_name):
        data_comp0 = pd.read_parquet(f'snapshots/{company_name}.prq')
        data_comp = data_comp0[(data_comp0.pickup_date >= LOWER_BORDER) & (data_comp0.pickup_date < UPPER_BORDER)].copy()
        return data_comp

    def make_report(self, company_name):
        """By one company or jointg"""
        if company_name.startswith('joint'):
            data_comp = self.get_df_jointg(company_name)
        else:
            data_comp = self.get_df_pure_company(company_name)
        return self.make_report_by_df(data_comp)

    def get_df_jointg(self, jointg_name):
        companies = self.companies_info.get_jointg_companies(jointg_name)
        dfs = [self.get_df_pure_company(company_name) for company_name in companies]
        result_df = pd.concat(dfs, axis=0)
        result_df['companyName'] = jointg_name
        return result_df

    def make_report_by_df(self, data_comp):
        data_comp['year_week'] = data_comp['pickup_date'].dt.year.astype(str) + '-' + data_comp[
            'pickup_date'].dt.isocalendar().week.astype(str).str.zfill(2)
        pickup_count_table = data_comp.pivot_table(
            index=['companyName', 'transport_type'],
            columns='year_week',
            values='pickup_date',
            aggfunc='count',
            fill_value=np.nan
        )
        pickup_count_table.columns = pd.MultiIndex.from_tuples([('volume', col) for col in pickup_count_table.columns])

        data_comp['median_rate'] = data_comp['total_linehaul_cost'].astype(float) / data_comp['miles']
        median_rate_table = data_comp.pivot_table(
            index=['companyName', 'transport_type'],
            columns='year_week',
            values='median_rate',
            aggfunc='median',
            fill_value=np.nan
        )
        median_rate_table.columns = pd.MultiIndex.from_tuples([('median_rate', col) for col in median_rate_table.columns])
        combined_table = pd.concat([pickup_count_table, median_rate_table], axis=1)

        return combined_table

    def make_report_by_list(self, names_list):
        dfs = [self.make_report(company_name) for company_name in names_list]
        result_df = pd.concat(dfs, axis=0)
        return result_df