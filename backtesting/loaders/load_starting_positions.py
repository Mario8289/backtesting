import datetime as dt
from typing import List

import pandas as pd
from pandas import DataFrame

from risk_backtesting.loaders.dataserver import DataServerLoader
from ..position import Position, OpenPosition


SCHEMAS = {
    "opening_positions": {
        "datasource": "int64",
        "shard": "object",
        "symbol": "object",
        "instrument_id": "int64",
        "unit_price": "float64",
        "price_increment": "float64",
        "currency": "object",
        "snapshot_date": "object",
        "next_trading_day": "object",
        "account_id": "int64",
        "position": "float64",
        "open_cost": "float64",
        "cumulative_cost": "float64",
        "realised_profit": "float64",
    }
}


class StartingPositionsLoader:
    def __init__(self, loader: DataServerLoader):
        self.loader = loader
        self.name = "starting_positions_dataserver_loader"

    @staticmethod
    def create_dataset_template(schema: dict) -> pd.DataFrame:
        # noinspection DuplicatedCode
        def df_empty(columns, dtypes, index=None):
            _df = pd.DataFrame(index=index)
            for c, d in zip(columns, dtypes):
                _df[c] = pd.Series(dtype=d)
            return _df

        cols = list(schema.keys())
        types = list(schema.values())
        df = df_empty(columns=cols, dtypes=types)
        return df

    def get_opening_positions(
            self, datasource_label, start_date, end_date, instruments, accounts
    ):
        opening_positions = self.loader.get_opening_positions(
            datasource_label, start_date, end_date, instruments, accounts
        )

        if opening_positions.empty:
            opening_positions = self.create_dataset_template(
                SCHEMAS["opening_positions"]
            )

        opening_positions["timestamp"] = pd.to_datetime(
            opening_positions["next_trading_day"], utc=True
        )
        opening_positions.set_index("timestamp", inplace=True)
        opening_positions.sort_index(inplace=True)

        return opening_positions

    def set_starting_positions(
            self, datasource_label, date, starting_positions, accounts, netting_engine
    ):

        position, net_position = load_starting_positions(
            starting_positions[starting_positions.account_id.isin(accounts)],
            venue=1,
            netting_engine=netting_engine,
        )

        return position, net_position

    def set_starting_positions_with_risk(
            self,
            datasource_label,
            date,
            starting_positions,
            accounts,
            invert_position,
            netting_engine,
            load_booking_risk: bool = False,
            load_booking_risk_from_target_accounts: bool = False,
            load_internalisation_risk: bool = False,
            snapshot: DataFrame = None,
            target_accounts: DataFrame = None,
    ):
        position, net_position = load_starting_positions_with_risk(
            starting_positions[starting_positions.account_id.isin(accounts)],
            venue=1,
            netting_engine=netting_engine,
            invert_position=invert_position,
            load_booking_risk=load_booking_risk,
            load_internalisation_risk=load_internalisation_risk,
            load_booking_risk_from_target_accounts=load_booking_risk_from_target_accounts,
            snapshot=snapshot,
            target_accounts=target_accounts,
        )

        return position, net_position


def load_starting_positions_with_risk(
        starting_positions,
        venue=1,
        netting_engine="fifo",
        position=Position,
        invert_position: bool = True,
        load_booking_risk: bool = False,
        load_booking_risk_from_target_accounts: bool = False,
        load_internalisation_risk: bool = False,
        snapshot: DataFrame = None,
        target_accounts: DataFrame = None,
        open_position=OpenPosition,
):
    open_positions = dict()
    x = 1
    # group by symbol then iter over group
    portfolio_net_position = 0
    if not starting_positions.empty:
        grps = starting_positions.groupby(
            [
                "symbol",
                "instrument_id",
                "account_id",
                "unit_price",
                "price_increment",
                "currency",
            ]
        )
        for (
                (
                        symbol,
                        instrument_id,
                        account,
                        instrument_unit_price,
                        price_increment,
                        currency,
                ),
                grp,
        ) in grps:
            pos_net_position = 0
            pos = position(
                symbol,
                instrument_unit_price,
                price_increment,
                netting_engine=netting_engine,
                currency=currency,
            )
            for index, row in grp.iterrows():
                if row.position != 0:
                    open_positions[venue, instrument_id, account] = pos
                    # date = row.snapshot_date
                    if load_booking_risk:
                        booking_risk = snapshot.loc[
                            (snapshot.account_id == account)
                            & (snapshot.instrument_id == instrument_id),
                            "booking_risk",
                        ]

                        if len(booking_risk):
                            risk = booking_risk.item() / 100
                        else:
                            risk = 0

                    elif load_booking_risk_from_target_accounts:
                        if "timestamp" in target_accounts.columns:
                            pass
                        else:
                            booking_risk = target_accounts.loc[
                                (target_accounts["account_id"] == account)
                                & (target_accounts["instrument_id"] == instrument_id),
                                "booking_risk",
                            ]

                        if len(booking_risk):
                            risk = booking_risk.item()
                        else:
                            risk = 0

                    elif load_internalisation_risk:
                        risk = (
                                snapshot.loc[
                                    (snapshot.account_id == account)
                                    & (snapshot.instrument_id == instrument_id),
                                    "internalisation_risk",
                                ].item()
                                / 100
                        )
                    else:
                        risk = 1

                    if invert_position:
                        row.position = row.position * -1

                    new_position = open_position.create_position_from_open_position_snapshot(
                        row, unit_price=instrument_unit_price, risk=risk
                    )

                    if netting_engine in ["fifo", "lifo"]:
                        pos.open_positions.append(new_position)
                    elif netting_engine == "avg_price":
                        pos.open_positions = new_position
                    x += 1

                    portfolio_net_position += new_position.quantity
                    pos_net_position += new_position.quantity

            pos.net_position = pos_net_position

    return open_positions, portfolio_net_position


def load_starting_positions(
        starting_positions,
        venue=1,
        netting_engine="fifo",
        position=Position,
        open_position=OpenPosition,
):
    open_positions = dict()
    x = 1
    # group by symbol then iter over group
    portfolio_net_position = 0

    if not starting_positions.empty:
        grps = starting_positions.groupby(
            [
                x
                for x in [
                "symbol",
                "instrument_id",
                "account_id",
                "unit_price",
                "price_increment",
                "currency",
                "contract_unit_of_measure",
            ]
                if x in starting_positions.columns
            ]
        )
        for (values, grp,) in grps:
            symbol = values[0]
            instrument_id = values[1]
            account = values[2]
            instrument_unit_price = values[3]
            price_increment = values[4]
            currency = values[5]
            contract_unit_of_measure = values[5:6] if len(values[5:6]) != 0 else "UNK"
            pos_net_position = 0

            pos = position(
                symbol,
                instrument_unit_price,
                price_increment,
                netting_engine=netting_engine,
                currency=currency,
                contract_unit_of_measure=contract_unit_of_measure,
            )
            for index, row in grp.iterrows():
                if row.position != 0:
                    open_positions[venue, instrument_id, account] = pos
                    # date = row.snapshot_date
                    new_position = open_position.create_position_from_open_position_snapshot(
                        row, unit_price=instrument_unit_price
                    )
                    if netting_engine in ["fifo", "lifo"]:
                        pos.open_positions.append(new_position)
                    elif netting_engine == "avg_price":
                        pos.open_positions = new_position
                    x += 1
                    portfolio_net_position += int(round(row.position * 100, 0))
                    pos_net_position += int(round(row.position * 100, 0))
            pos.net_position = pos_net_position

    return open_positions, portfolio_net_position


def load_open_positions(
        loader,
        datasource_label: str,
        start_date: dt.date,
        end_date: dt.date,
        account: List,
        instrument: List,
        netting_engine: str,
):
    starting_positions = loader.get_opening_positions(
        datasource_label=datasource_label,
        start_date=start_date,
        end_date=end_date,
        accounts=account,
        instruments=instrument,
    )

    position, portfolio_net_position = loader.set_starting_positions(
        datasource_label=datasource_label,
        date=start_date,
        starting_positions=starting_positions,
        accounts=account,
        netting_engine=netting_engine,
    )
    return position, portfolio_net_position


def load_open_positions_with_risk(
        loader,
        datasource_label: str,
        start_date: dt.date,
        end_date: dt.date,
        account: List,
        invert_position: bool,
        instrument: List,
        netting_engine: str,
        load_booking_risk: bool = False,
        load_booking_risk_from_target_accounts: bool = False,
        load_internalisation_risk: bool = False,
        snapshot: DataFrame = None,
        target_accounts: DataFrame = pd.DataFrame(),
):
    starting_positions = loader.get_opening_positions(
        datasource_label=datasource_label,
        start_date=start_date,
        end_date=end_date,
        accounts=account,
        instruments=instrument,
    )

    if not snapshot.empty:
        snapshot = (
            snapshot[
                (snapshot["instrument_id"].isin(instrument))
                & (snapshot["account_id"].isin(account))
                ]
            .groupby(["instrument_id", "account_id"])
            .first()
            .reset_index()
        )

    if not target_accounts.empty:
        target_accounts = target_accounts[(target_accounts["account_id"].isin(account))]

        if "instrument_id" in target_accounts.columns:
            target_accounts = target_accounts[
                (target_accounts["instrument_id"].isin(instrument))
            ]

    position, portfolio_net_position = loader.set_starting_positions_with_risk(
        datasource_label=datasource_label,
        date=start_date,
        starting_positions=starting_positions,
        accounts=account,
        invert_position=invert_position,
        netting_engine=netting_engine,
        load_booking_risk=load_booking_risk,
        load_internalisation_risk=load_internalisation_risk,
        load_booking_risk_from_target_accounts=load_booking_risk_from_target_accounts,
        snapshot=snapshot,
        target_accounts=target_accounts,
    )
    return position, portfolio_net_position
