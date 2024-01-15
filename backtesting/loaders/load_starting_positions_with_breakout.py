import datetime as dt
import logging
from typing import List

import numpy as np
import pandas as pd
import pytz

from lmax_analytics.dataloader.opening_positions import get_opening_positions
from lmax_analytics.dataloader.trades import get_broker_trades
from ..event import Event
from ..portfolio import Portfolio

eastern = pytz.timezone("US/Eastern")


class StartingPositionsLoader:
    def __init__(self):
        pass

    def get_opening_positions(
            self,
            datasource_label: str,
            start_date: dt.date,
            instrument: List,
            account: List,
    ):
        pass


class StartingPositionsParquetLoaderBreakOut(StartingPositionsLoader):
    def __init__(self, loader, portfolio: Portfolio = Portfolio(), max_days: int = 25):
        super().__init__()
        self.loader = loader
        self.portfolio = portfolio
        self.name = "starting_positions_parquet_loader"
        self.max_days = max_days

    def get_opening_positions(
            self,
            datasource_label: str,
            start_date: dt.date,
            instruments: List,
            accounts: List,
            end_date: dt.date = None,
    ):
        starting_positions = get_opening_positions(
            self.loader,
            datasource_label=datasource_label,
            start_date=start_date,
            end_date=end_date,
            instrument=instruments,
            account=accounts,
        )

        return starting_positions

    def set_starting_positions(
            self,
            datasource_label,
            date,
            starting_positions,
            accounts,
            netting_engine,
            **kwargs,
    ):
        position = dict()
        net_position = 0
        if not starting_positions.empty:
            delta_trades = load_delta_trades(
                loader=self.loader,
                datasource_label=datasource_label,
                date=date,
                starting_positions=starting_positions,
                max_days=self.max_days,
            )

            position, net_position = load_starting_positions(
                portfolio=self.portfolio, trades=delta_trades
            )

        return position, net_position


def get_delta_trades(starting_positions, trades):
    unique_cols = ["instrument_id", "account_id"]

    # ensure that there is only one position for an instrument account pair
    starting_positions_dedup = starting_positions[
        [x for x in starting_positions.columns if x != "snapshot_date"]
    ].drop_duplicates()

    # get only the starting positions for which we have trades for
    starting_positions_int = df_intersection(
        starting_positions_dedup, trades.reset_index(), unique_cols, "timestamp"
    )

    # ensure that the trades are in the correct order
    trades = trades.sort_values(unique_cols + ["timestamp", "execution_id"])

    trades = create_rolling_position(starting_positions_int, trades, unique_cols)

    trades = find_start_of_position(starting_positions_int, trades, unique_cols)

    delta_trades = get_trades_after_start_of_position(trades, unique_cols)

    return delta_trades


def get_trades_after_start_of_position(trades, unique_cols):
    # collate the trades that occured after the last inversion if there were no inversion then collate all trades in period
    delta_trades = trades.groupby(unique_cols).apply(
        lambda grp: grp
        if grp[grp["start_of_position"] == 1].size == 0
        else grp.loc[grp[grp["start_of_position"] == 1].tail(1).index.item() :,]
    )
    # for the first trade of each position overwrite trade qty with the net_qty
    delta_trades.loc[
        delta_trades["start_of_position"] == 1, "contract_qty"
    ] = delta_trades["net_qty_reverse_shift"]
    delta_trades = (
        delta_trades.reset_index(drop=True).reset_index().set_index("timestamp")
    )
    return delta_trades


def find_start_of_position(starting_positions, trades, unique_cols):
    trades["sign_net_qty_reverse"] = trades["net_qty_reverse"].map(lambda x: np.sign(x))

    # create columns that represents net position for client after to trade,
    trades["net_qty_reverse_shift"] = (
        trades.groupby(unique_cols)["net_qty_reverse"]
        .shift(-1)
        .fillna(trades["net_qty_reverse"])
    )

    trades.loc[
        trades.groupby(unique_cols).tail(1).sort_values(by=unique_cols).index,
        "net_qty_reverse_shift",
    ] = (starting_positions.set_index(unique_cols).sort_index()["position"].values)

    trades["sign_net_qty_reverse_shift"] = trades.groupby(unique_cols)[
        "net_qty_reverse_shift"
    ].apply(lambda x: np.sign(x))

    # identify inversion in net position to find when a position the last position for each instrument was started
    trades["start_of_position"] = np.where(
        (trades["sign_net_qty_reverse_shift"] != trades["sign_net_qty_reverse"]), 1, 0
    )
    # ignore if position is flattened as we only want to include from open not last position to close.
    trades.loc[trades.net_qty_reverse_shift == 0, "start_of_position"] = 0

    return trades


def create_rolling_position(starting_positions, trades, unique_cols):
    # create the rolling position by reversing out the trades from the starting position

    # because we are replaying trades backwards we need to invert the trade quantities
    trades.loc[:, "net_qty_reverse"] = trades["contract_qty"] * -1
    # the last event should be the opening positions minus the last trade quantity

    # if account has an order with multiple trades filled at same timestamp this
    # makes sure the sequence is correct, also sort starting position
    starting_positions = starting_positions.sort_values(unique_cols)
    trades = trades.sort_values(unique_cols + ["timestamp", "execution_id"])

    trades.loc[
        trades.groupby(unique_cols).tail(1).sort_values(by=unique_cols).index,
        "net_qty_reverse",
    ] = (
            starting_positions.set_index(unique_cols).sort_index()["position"].values
            - trades.groupby(unique_cols)
            .tail(1)
            .sort_values(by=unique_cols)["contract_qty"]
            .values
    )

    trades["net_qty_reverse"] = (
        trades.sort_index(ascending=False)
        .groupby(unique_cols)["net_qty_reverse"]
        .cumsum()
        .round(5)
    )
    return trades


def df_intersection(a, b, intx_cols, index):
    a = a.reset_index().set_index(intx_cols)
    output = (
        a.loc[
            a.index.isin(
                a.index.intersection(b.reset_index().set_index(intx_cols).index)
            )
        ]
        .reset_index()
        .set_index(index)
    )
    return output


def load_delta_trades(
        loader,
        datasource_label: str,
        date: dt.date,
        starting_positions: pd.DataFrame,
        max_days=20,
):
    logger = logging.getLogger("load starting positions")
    logger.info(f"find starting positions for {date}")
    delta_trades = pd.DataFrame()

    _starting_positions = starting_positions[starting_positions.position != 0].copy()

    # 0 - account id, 1 - instrument id
    instrument_account_pairs: pd.DataFrame = _starting_positions[
        ["account_id", "instrument_id"]
    ].drop_duplicates().copy()

    x = 0
    reload_starting_positions = False
    while not instrument_account_pairs.empty:

        if reload_starting_positions:
            _starting_positions = get_opening_positions(
                loader=loader,
                datasource_label=datasource_label,
                start_date=date,
                end_date=date,
                account=instrument_account_pairs.account_id.unique().tolist(),
                instrument=instrument_account_pairs.instrument_id.unique().tolist(),
            )
            _starting_positions = _starting_positions[_starting_positions.position != 0]

        # get only the starting positions for the instrument account pairs we are looking for
        _starting_positions = (
            _starting_positions.reset_index()
            .merge(instrument_account_pairs, how="inner")
            .set_index("timestamp")
        )

        date = date - dt.timedelta(days=1)

        # load trades
        trades = get_broker_trades(
            loader=loader,
            datasource_label=datasource_label,
            start_date=date - dt.timedelta(days=1),
            end_date=date,
            account=instrument_account_pairs.account_id.unique().tolist(),
            instrument=instrument_account_pairs.instrument_id.unique().tolist(),
        )

        # get only the trades for the instrument account pairs we we have starting positions for
        trades_filtered = (
            trades.loc[trades.trade_date == date]
            .reset_index()
            .merge(
                _starting_positions[["account_id", "instrument_id"]]
                .drop_duplicates()
                .copy(),
                how="inner",
                )
            .set_index("timestamp")
        )

        if not trades_filtered.empty:
            delta_trades_for_day = get_delta_trades(
                _starting_positions, trades_filtered.reset_index()
            )
            delta_trades = pd.concat([delta_trades, delta_trades_for_day])

            _instrument_account_pairs_start_found = delta_trades_for_day[
                delta_trades_for_day["start_of_position"] == 1
                ][["account_id", "instrument_id"]].drop_duplicates()

            instrument_account_pairs = remove_instrument_account_pairs_when_start_of_position_found(
                _instrument_account_pairs_start_found, instrument_account_pairs
            )

            reload_starting_positions = True
        else:
            reload_starting_positions = False
        x += 1

        if x == max_days:
            _starting_positions_left = df_intersection(
                _starting_positions,
                instrument_account_pairs,
                instrument_account_pairs.columns.tolist(),
                "timestamp",
            )
            # if max days exceed add a fictitious trade of the position remaining
            trades_exceptions = generate_fake_trades(_starting_positions_left, date)

            delta_trades = pd.concat([delta_trades, trades_exceptions])

            logger.warning(
                f"cannot load the previous trades for the following instrument, account pairs {_starting_positions_left[['instrument_id', 'account_id']].values},"
                f" remaining position will be opened with an execution_id of -1 and assigned to counterparty -1"
            )
            break

    delta_trades = delta_trades.sort_index()

    return delta_trades


def generate_fake_trades(_starting_positions, date):
    trades_exceptions = _starting_positions[
        [
            "datasource",
            "account_id",
            "currency",
            "unit_price",
            "price_increment",
            "symbol",
            "instrument_id",
            "position",
        ]
    ].rename(columns={"position": "contract_qty"})
    trades_exceptions["rate_to_usd"] = 0
    trades_exceptions["execution_id"] = -1
    trades_exceptions["trade_date"] = date.strftime("%Y-%m-%d")
    trades_exceptions["price"] = pd.Series(
        _starting_positions["open_cost"]
        / _starting_positions["position"]
        / _starting_positions["unit_price"]
    ).fillna(0)
    trade_datetime = eastern.localize(
        dt.datetime.combine(date - dt.timedelta(days=1), dt.time(17))
    ).astimezone(pytz.UTC)
    trades_exceptions["time_stamp"] = int(trade_datetime.timestamp() * 1000)
    trades_exceptions.index = pd.DatetimeIndex(
        [trade_datetime] * len(trades_exceptions)
    )
    return trades_exceptions


def remove_instrument_account_pairs_when_start_of_position_found(complete, all):
    unique_grp = ["account_id", "instrument_id"]
    remaining = (
        all.reset_index()
        .set_index(unique_grp)
        .loc[
            all.reset_index()
        .set_index(unique_grp)
        .index.difference(complete.reset_index().set_index(unique_grp).index)
        ]
        .reset_index()
        .set_index("timestamp")
    )
    return remaining


def load_starting_positions(portfolio, trades, venue=None):
    net_pos = 0

    if "counterparty_account_id" not in trades.columns:
        trades["counterparty_account_id"] = -1
    else:
        trades["counterparty_account_id"] = trades["counterparty_account_id"].fillna(-1)

    for row in trades.itertuples():
        event = Event(
            datasource=row.datasource,
            venue=venue,
            timestamp_micros=row.time_stamp,
            unit_price=row.unit_price,
            order_book_id=row.instrument_id,
            symbol=row.symbol,
            currency=row.currency,
            price_increment=row.price_increment,
            timestamp=row[0],
            account_id=row.account_id,
            counterparty_account_id=row.counterparty_account_id,
            execution_id=row.execution_id if hasattr(row, "execution_id") else -1,
            contract_qty=row.contract_qty * 100,
            rate_to_usd=row.rate_to_usd,
            price=row.price * 1000000,
        )

        portfolio.on_trade(event=event)

        net_pos += row.contract_qty * 100

    return portfolio.positions, net_pos
