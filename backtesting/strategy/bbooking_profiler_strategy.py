import datetime as dt
from typing import Dict

import pandas as pd
import pytz

from .bbooking_strategy import BbookingStrategy
from ..exit_strategy.base import AbstractExitStrategy
from ..exit_strategy.exit_default import ExitDefault
from ..loaders.load_tob import load_tob
from ..model import create_model

eastern = pytz.timezone("US/Eastern")

DEFAULT_SCORE_BOOKING_RISK: dict = {0: 100, 1: 100, 2: 100, 3: 0, 4: 0, 5: 0}


class BbookingProfilerStrategy(BbookingStrategy):
    __output_slots__ = [
        "name",
        "exit_strategy",
        "model_type",
        "train_freq",
        "train_period",
        "score_booking_risk_string",
        "rank_evaluation_string",
    ]

    __slots__ = __output_slots__ + ["model", "score_booking_risk"]

    def __init__(
            self,
            train_freq: int,
            train_period: int,
            rank_evaluation_string: Dict = None,
            score_booking_risk: Dict = DEFAULT_SCORE_BOOKING_RISK,
            exit_strategy: AbstractExitStrategy = ExitDefault(),
            model_type: str = "profiling_ranks",
    ):

        self.name = "bbooking_profiler"
        self.train_freq = train_freq
        self.train_freq = train_freq
        self.train_period = train_period
        self.exit_strategy = exit_strategy
        self.model_type = model_type
        self.score_booking_risk_string = str(score_booking_risk)
        self.score_booking_risk = score_booking_risk
        self.rank_evaluation_string = str(rank_evaluation_string)
        self.model = create_model(
            {"model": model_type, "rank_evaluation_string": rank_evaluation_string}
        )
        self.next_training_day: dt.date = None
        self.account_booking_risk: dict = {}

    def set_next_training_day(self, day: dt.date):
        next_training_day = day + dt.timedelta(days=self.train_freq)
        while next_training_day.weekday() > 4:
            next_training_day = next_training_day + dt.timedelta(days=1)
        self.next_training_day = next_training_day

    def retrain_model(self, day: dt.date) -> bool:
        if self.next_training_day is None or self.next_training_day == day:
            self.set_next_training_day(day)
            return True
        else:
            return False

    def get_migration_timestamp(self, day: dt.date) -> int:
        datetime = dt.datetime.combine(day, dt.time())
        migration_timestamp = (
                pytz.UTC.localize(datetime).astimezone(eastern).replace(hour=17)
                + dt.timedelta(minutes=5)
        ).astimezone(pytz.UTC)

        return migration_timestamp

    def get_account_migrations(
            self, day, target_accounts, shard, instruments, tob_loader, **kwargs
    ):
        # TODO: add warning if load_booking_risk or load_internalisation_risk is True as this strategy ignores them.
        score_df = pd.DataFrame(
            self.model.score.items(), columns=["account_id", "score"]
        )
        target_accounts_with_scores = pd.merge(target_accounts, score_df, how="left")

        target_accounts_with_scores["booking_risk"] = target_accounts_with_scores[
            "score"
        ].map(lambda x: self.score_booking_risk.get(x, 0))

        current_booking_risk = pd.DataFrame(
            self.account_booking_risk.values(),
            index=self.account_booking_risk.keys(),
            columns=["current_booking_risk"],
        )

        target_accounts_with_scores = pd.merge(
            target_accounts_with_scores,
            current_booking_risk,
            left_on="account_id",
            right_index=True,
            how="left",
        )

        migrations = target_accounts_with_scores.query(
            "booking_risk != current_booking_risk"
        ).copy()

        timestamp = self.get_migration_timestamp(day)
        timestamp_micros = int(timestamp.timestamp() * 1000000)
        migrations["timestamp_micros"] = timestamp_micros
        migrations["timestamp"] = timestamp
        migrations["trading_session"] = (timestamp + dt.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        migrations["event_type"] = "account_migration"

        migrations_for_instruments = pd.DataFrame()
        for instrument in instruments:
            migrations_for_instrument = migrations.copy()
            migrations_for_instrument["order_book_id"] = instrument
            migrations_for_instruments = pd.concat(
                [migrations_for_instruments, migrations_for_instrument]
            )

        tob = load_tob(
            tob_loader,
            datasource_label=shard,
            instrument=instruments,
            start_date=timestamp,
            end_date=timestamp,
            tier=[1],
            period="minute",
        ).sort_values("timestamp_micros")

        migrations_with_tob = pd.merge_asof(
            migrations_for_instruments, tob, on="timestamp_micros", by=["order_book_id"]
        )

        migrations_with_tob = migrations_with_tob.set_index("timestamp")
        migrations_with_tob = migrations_with_tob.drop(
            columns=["score", "current_booking_risk"]
        )
        if "order_book_details" in kwargs.keys():
            order_book_details = kwargs["order_book_details"]
            if not order_book_details.empty:
                columns = [
                    x
                    for x in order_book_details.columns
                    if x not in migrations_with_tob.columns or x == "order_book_id"
                ]
            migrations_with_tob = (
                migrations_with_tob.reset_index()
                .merge(order_book_details[columns], on="order_book_id", how="left")
                .set_index("timestamp")
            )

        return migrations_with_tob
