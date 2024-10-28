import pandas as pd

LOW_BORDER = '2024-01-01'

class ReportMaker:
    def make_report_one(self, company):
        data_comp0 = pd.read_parquet(f'snapshots/{company}.prq')
        data_comp = data_comp0[data_comp0.pickup_date >= LOW_BORDER].copy()
        data_comp.loc[:, 'year_week'] = data_comp['pickup_date'].dt.year.astype(str) + '-' + data_comp[
            'pickup_date'].dt.isocalendar().week.astype(str).str.zfill(2)
        summary_table = data_comp.pivot_table(
            index='transport_type',
            columns='year_week',
            values='pickup_date',
            aggfunc='count',
            fill_value=np.nan  # Fill empty cells with 0
        )
        return summary_table