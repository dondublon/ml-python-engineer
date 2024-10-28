import pandas as pd
import numpy as np

LOWER_BORDER = '2024-01-01'
UPPER_BORDER = '2024-05-20'


class ReportMaker:
    def make_report_one(self, company):
        data_comp0 = pd.read_parquet(f'snapshots/{company}.prq')
        data_comp = data_comp0[(data_comp0.pickup_date >= LOWER_BORDER) & (data_comp0.pickup_date < UPPER_BORDER)].copy()
        data_comp['year_week'] = data_comp['pickup_date'].dt.year.astype(str) + '-' + data_comp[
            'pickup_date'].dt.isocalendar().week.astype(str).str.zfill(2)
        pickup_count_table = data_comp.pivot_table(
            index='transport_type',
            columns='year_week',
            values='pickup_date',
            aggfunc='count',
            fill_value=np.nan
        )
        pickup_count_table.columns = pd.MultiIndex.from_tuples([('volume', col) for col in pickup_count_table.columns])

        data_comp['median_rate'] = data_comp['total_linehaul_cost'] / data_comp['miles']
        data_comp['median_rate'] = data_comp['total_linehaul_cost'].astype(float) / data_comp['miles']
        median_rate_table = data_comp.pivot_table(
            index='transport_type',
            columns='year_week',
            values='median_rate',
            aggfunc='median',
            fill_value=np.nan
        )
        median_rate_table.columns = pd.MultiIndex.from_tuples([('median_rate', col) for col in median_rate_table.columns])
        combined_table = pd.concat([pickup_count_table, median_rate_table], axis=1)
        return combined_table