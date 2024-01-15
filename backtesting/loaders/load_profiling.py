import datetime as dt
import os

import pandas as pd

from lmax_analytics.dataloader.closed_positions import load_closed_positions
from lmax_analytics.dataloader.parquet import Parquet


class ProfilingLoader:
    def __init__(self, loader):
        self.loader = loader

    def load_data(
            self, datasource_label: str, job: str, start_date: dt.date, end_date: dt.date
    ):
        dates = list(pd.date_range(start_date, end_date))
        paths = [
            os.path.join(
                job,
                "parquet",
                Parquet.construct_file_path(
                    datasource_label=datasource_label,
                    filename=f"{day.strftime('%Y')}-{day.strftime('%m')}-{day.strftime('%d')}-{job}.parquet",
                    date=day.date(),
                ),
            )
            for day in dates
        ]
        data = self.loader.load_endpoints("broker", paths)
        return data

    def load_closed_positions(
            self,
            datasource_label: str,
            start_date: dt.date,
            end_date: dt.date,
            target_accounts: pd.DataFrame,
    ):
        closed_positions = load_closed_positions(
            self.loader,
            datasource_label,
            start_date,
            end_date,
            aggregate_by_account=True,
            pcols=["rpnl"],
            include_snapshot=True,
        )

        if not target_accounts.empty:
            closed_positions = closed_positions.join(
                target_accounts.set_index("account_id"), how="inner"
            )

        return closed_positions

    def load_markouts(
            self, datasource_label: str, start_date: dt.date, end_date: dt.date
    ):
        markouts = self.load_data(datasource_label, "markouts", start_date, end_date)

        return markouts

    def load_margin_state_changes(
            self, datasource_label: str, start_date: dt.date, end_date: dt.date
    ):
        margin_state_changes = self.load_data(
            datasource_label, "margin_state_changes", start_date, end_date
        )

        return margin_state_changes
