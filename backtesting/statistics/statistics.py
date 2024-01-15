import datetime as dt

import numpy as np
import pandas as pd
import pytz

from ..statistics.base import AbstractStatistics

nytime = pytz.timezone("US/Eastern")


class Stats(AbstractStatistics):
    """


    """

    def __init__(self, portfolio):
        self.portfolio = portfolio
        self.rolling_sharpe = False
        self.periods = 30
        self.equity = {}
        self.equity_benchmark = {}
        self.events = []
        self.position_snapshots = {}

    def update_event_snapshot(
            self,
            ts=None,
            portfolio=None,
            event=None,
            order=None,
            store_md_snapshot=False,
            label=None,
            price=None,
    ):
        venue = event.venue
        order_book_id = event.order_book_id
        e_type = event.event_type
        price = event.get_tob_price(match=portfolio.matching_method)
        mid_price = price = event.get_tob_price(match="mid")

        if e_type in ["trade", "hedge", "internal", "bbook", "trigger"]:
            price = event.price / 1e6
            account = event.account_id
            pos = None
            if (venue, order_book_id, account) in portfolio.positions:
                pos = portfolio.positions[venue, order_book_id, account]
            elif (venue, order_book_id, account) in portfolio.closed_positions:
                pos = portfolio.closed_positions[venue, order_book_id, account]
            if pos is not None:
                net_qty = pos.net_position / 100
                currency_inventory_contracts = (
                        portfolio.inventory_contracts[event.contract_unit_of_measure] / 100
                )

                currency_inventory_dollars = (
                        portfolio.inventory_dollars[event.contract_unit_of_measure] / 1e8
                )

                event_snapshot = [
                    ts,
                    venue,
                    event.symbol,
                    event.contract_unit_of_measure,
                    event.currency,
                    order_book_id,
                    mid_price,
                    price,
                    pos.get_price() / 1000000,
                    net_qty,
                    currency_inventory_contracts,
                    currency_inventory_dollars,
                    event.contract_qty / 100,
                    1 if event.contract_qty > 0 else 0,
                    pos.unrealised_pnl,
                    pos.realised_pnl,
                    (net_qty * mid_price * pos.unit_price) * event.rate_to_usd,
                    pos.notional_traded,
                    pos.notional_traded_net,
                    pos.notional_rejected,
                    pos.tighten_cost,
                    int(account),
                    event.counterparty_account_id,
                    None if order is None else order.signal,
                    e_type,
                    label,
                    event.trading_session,
                    ]
                self.events.append(event_snapshot)
                # self.df_events.loc[self.evt, :] = event_snapshot
                # self.evt += 1

        elif (
                e_type == "market_data" and price is not None and store_md_snapshot
        ) or e_type == "closing_price":
            for ((venue, order_book_id, account), pos) in portfolio.positions.items():
                net_qty = pos.calculate_net_contracts() / 100
                currency_inventory_contracts = (
                        portfolio.inventory_contracts.get(event.contract_unit_of_measure, 0)
                        / 100
                )

                currency_inventory_dollars = (
                        portfolio.inventory_dollars.get(event.contract_unit_of_measure, 0)
                        / 1e8
                )

                event_snapshot = [
                    ts,
                    venue,
                    event.symbol,
                    event.contract_unit_of_measure,
                    event.currency,
                    order_book_id,
                    mid_price,
                    None,
                    pos.get_price() / 1000000,
                    net_qty,
                    currency_inventory_contracts,
                    currency_inventory_dollars,
                    0,
                    np.sign(net_qty),
                    pos.unrealised_pnl,
                    pos.realised_pnl,
                    (net_qty * mid_price * pos.unit_price) * event.rate_to_usd,
                    pos.notional_traded,
                    pos.notional_traded_net,
                    pos.notional_rejected,
                    pos.tighten_cost,
                    int(account),
                    event.counterparty_account_id,
                    None if order is None else order.signal,
                    e_type,
                    label,
                    event.trading_session,
                    ]
                self.events.append(event_snapshot)

    def events_to_df(self, event_features, upnl_reversals=pd.DataFrame()):
        try:

            df = pd.DataFrame(
                data=self.events,
                columns=[
                    "timestamp",
                    "venue",
                    "symbol",
                    "contract_unit_of_measure",
                    "currency",
                    "order_book_id",
                    "tob_price",
                    "trade_price",
                    "avg_price",
                    "net_qty",
                    "inventory_contracts_cum",
                    "inventory_dollars_cum",
                    "trade_qty",
                    "is_long",
                    "upnl",
                    "rpnl_cum",
                    "notional_mtm",
                    "notional_traded_cum",
                    "notional_traded_net_cum",
                    "notional_rejected_cum",
                    "tighten_cost_cum",
                    "account_id",
                    "counterparty_account_id",
                    "action",
                    "type",
                    "portfolio",
                    "trading_session",
                ],
            )
            df["trading_session"] = pd.to_datetime(df["trading_session"])
            df["net_rpnl_cum"] = df["rpnl_cum"] - df["tighten_cost_cum"]
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df.set_index("timestamp", inplace=True)
            for (new_col, old_col) in [
                ("rpnl", "rpnl_cum"),
                ("notional_mtm_change", "notional_mtm"),
                ("notional_traded", "notional_traded_cum"),
                ("notional_traded_net", "notional_traded_net_cum"),
                ("notional_rejected", "notional_rejected_cum"),
                ("tighten_cost", "tighten_cost_cum"),
                ("net_rpnl", "net_rpnl_cum"),
                ("upnl_reversal", "upnl"),
                ("inventory_contracts", "inventory_contracts_cum"),
                ("inventory_dollars", "inventory_dollars_cum"),
            ]:
                if old_col == "upnl":
                    df[new_col] = df.groupby(event_features)[old_col].transform(
                        lambda x: x.shift(1).fillna(0) * -1
                    )
                else:
                    df[new_col] = df.groupby(event_features)[old_col].transform(
                        lambda x: x - x.shift(1).fillna(0)
                    )

            if not upnl_reversals.empty:
                df = (
                    df.reset_index()
                    .merge(
                        upnl_reversals.reset_index().rename(
                            columns={
                                "instrument_id": "order_book_id",
                                "event_type": "type",
                                "trade_date": "trading_session",
                            }
                        ),
                        on=[
                            "timestamp",
                            "account_id",
                            "order_book_id",
                            "symbol",
                            "contract_unit_of_measure",
                            "type",
                            "trading_session",
                        ],
                        how="outer",
                        suffixes=("", "_delta"),
                    )
                    .set_index("timestamp")
                )
                # fix bug for not including upnl reversal for
                # positions that did have a position open at the end of day 1.
                df["upnl_reversal"] = df["upnl_reversal"].fillna(0)

                df.loc[
                    (df["upnl_reversal"] == 0) & (df["type"] == "closing_price"),
                    "upnl_reversal",
                ] = df[(df["upnl_reversal"] == 0) & (df["type"] == "closing_price")][
                    "upnl_reversal_delta"
                ].fillna(
                    0
                )
                df = df.drop(columns=["upnl_reversal_delta"])

            for col in ["upnl", "rpnl", "upnl_reversal"]:
                df[col] = df[col].fillna(0)

            df["pnl"] = df["rpnl"] + df["upnl"] + df["upnl_reversal"]
            df["upnl_change"] = df["upnl"] + df["upnl_reversal"]
        except KeyError as e:
            raise KeyError(f"key error {e}")
        return df

    @staticmethod
    def compute_trade_action_metrics(df, event_features):
        trades_actions = (
            df.reset_index()
            .groupby(event_features + ["action"])
            .agg({"rpnl": ["count", "sum"], "notional_traded": ["sum"]})
            .unstack(level="action")
            .fillna(0)
        )

        trades_actions.columns = [
            "".join([l + "_" if i != len(x) - 1 else l for i, l in enumerate(x)])
            for x in trades_actions.columns
        ]

        trades_types = (
            df.reset_index()
            .groupby(event_features + ["type"])
            .agg({"rpnl": ["count", "sum"], "notional_traded": "sum"})
        )

        types = list(
            trades_types["notional_traded"].index.get_level_values("type").unique()
        )
        trades_actions_summary = trades_types.unstack(level="type").fillna(0)
        trades_actions_ratios = pd.DataFrame()
        for i, type in enumerate(types):
            trades_actions_ratios[f"notional_{type}_ratio"] = trades_actions_summary[
                "notional_traded"
            ]["sum"].apply(lambda x: round(1 / x.sum() * x[i], 2), axis=1)
            trades_actions_ratios[f"count_{type}_ratio"] = trades_actions_summary[
                "rpnl"
            ]["count"].apply(lambda x: round(1 / x.sum() * x[i], 2), axis=1)
        trades_actions_ratios.index = trades_actions_summary.index
        trades_actions_summary.columns = [
            "".join([l + "_" if i != len(x) - 1 else l for i, l in enumerate(x)])
            for x in trades_actions_summary.columns
        ]

        return pd.concat(
            [trades_actions, trades_actions_summary, trades_actions_ratios], axis=1
        )

    @staticmethod
    def compute_strategy_pnl_metrics(df, event_features):
        agg_feats = {
            "tob_price": "last",
            "pnl": "sum",
            "rpnl": "sum",
            "upnl": "last",
            "upnl_change": "sum",
            "upnl_reversal": "sum",
            "avg_price": "last",
            "net_qty": "last",
            "notional_mtm": "last",
            "notional_mtm_change": "sum",
            "net_rpnl": "sum",
            "notional_traded": "sum",
            "notional_rejected": "sum",
            "tighten_cost": "sum",
            "trade_qty": [lambda x: x.abs().sum(), "sum", "count"],
        }
        agg_feats = {k: v for (k, v) in agg_feats.items() if k in df.columns}
        df = df.copy()
        grps = df.reset_index().groupby(event_features)
        overall_performance = grps.agg(agg_feats)
        overall_performance.columns = list(agg_feats.keys()) + [
            "trade_net_qty",
            "trade_cnt",
        ]
        return overall_performance

    def aggregate_returns(
            self,
            df,
            resample_rule,
            event_features=None,
            metrics=[
                "performance_overview",
                "trading_actions_breakdown",
                "inventory_overview",
            ],
    ):
        df = df.copy()

        time_col = df.index.name
        if event_features is None:
            event_features = [pd.Grouper(key="timestamp", freq=resample_rule)]
        elif resample_rule == "summary":
            time_col = "trading_session"
            event_features = [x for x in event_features if x != time_col] + [
                pd.Grouper(key="trading_session", freq="1D")
            ]
        else:
            event_features = event_features + [
                pd.Grouper(key="timestamp", freq=resample_rule)
            ]

        metric_views = self.compute_metrics_by_features(df, event_features, metrics)

        df = pd.concat(metric_views, axis=1).reset_index()

        if "trading_session" not in df.columns:
            df["trading_session"] = pd.to_datetime(
                (
                        pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(
                            nytime
                        )
                        + dt.timedelta(hours=7)
                ).dt.date
            )

        # if time_col in df.columns:
        df.set_index(time_col, inplace=True)

        return df

    def compute_metrics_by_features(self, df, event_features, metrics):
        metric_views = []
        if "performance_overview" in metrics:
            metric_views.append(self.compute_strategy_pnl_metrics(df, event_features))
        if "trading_actions_breakdown" in metrics:
            metric_views.append(self.compute_trade_action_metrics(df, event_features))
        if "trading_drawdowns" in metrics:
            metric_views.append(
                df.reset_index()
                .groupby(event_features)
                .apply(
                    lambda x: self.create_drawdown(
                        x, col="rpnl_cum", feature="absolute"
                    )
                )
            )
        if "inventory_overview" in metrics:
            metric_views.append(self.compute_net_weighted_pos(df, event_features))

        return metric_views

    @staticmethod
    def add_bounding_rows(df, grouper, event_features):
        if grouper.key == "trading_session":
            sessions = [
                dt.datetime.fromtimestamp(x / 1e9)
                for x in df.trading_session.unique().tolist()
            ]
            if len(sessions) > 1:
                sessions_intra_period = sessions[1:]
                sessions.extend(sessions_intra_period)
                sessions = sorted(sessions)
            bounds = [
                         x.astimezone(pytz.UTC)
                         .astimezone(nytime)
                         .replace(hour=17, tzinfo=pytz.UTC)
                         for x in sessions
                     ] + [sessions[-1].replace(hour=17)]
            bounds = [
                x if (i // 2 or i == 0) else x - dt.timedelta(microseconds=1)
                for (i, x) in enumerate(bounds)
            ]
            trading_session = sessions + [sessions[-1]]
            if len(sessions) > 2:
                trading_session = [
                    x if (i // 2 or i == 0) else x - dt.timedelta(days=1)
                    for (i, x) in enumerate(trading_session)
                ]
            df2 = pd.DataFrame(
                {"timestamp": bounds, "trading_session": trading_session}
            )
        else:
            converted_dt = [
                dt.datetime.fromtimestamp(x.astype(int) / 1e9)
                for x in [
                    df.reset_index().timestamp.head(1).dt.floor(grouper.freq),
                    df.reset_index().timestamp.tail(1).dt.ceil(grouper.freq),
                ]
            ]
            bounds = list(
                pd.date_range(converted_dt[0], converted_dt[1], freq=grouper.freq)
            )
            if len(bounds) > 1:
                converted_dt_intra_period = [
                    x - dt.timedelta(microseconds=1) for x in bounds[1:]
                ]
                bounds = bounds[:-1] + converted_dt_intra_period
                bounds = sorted(bounds)

            df2 = pd.DataFrame({"timestamp": bounds})
        df2["timestamp"] = pd.to_datetime(df2["timestamp"], utc=True)

        df3 = df.copy()
        for event_grp in list(df.groupby(event_features).groups.keys()):
            event_grp = event_grp if type(event_grp) == tuple else tuple([event_grp])
            df_grp = df2.copy()
            for i, f in enumerate(event_features):
                df_grp[f] = event_grp[i]
            df3 = pd.concat([df3, df_grp.set_index("timestamp")]).sort_index()
        df3["net_qty"] = (
            df3.reset_index()
            .groupby(event_features)["net_qty"]
            .transform(lambda x: x.ffill())
            .values
        )
        df3["net_qty"] = df3["net_qty"].fillna(
            df3.net_qty.shift(-1) - df3.trade_qty.shift(-1)
        )
        return df3

    def compute_net_weighted_pos(self, df, event_features):
        df2 = df.copy()

        temporal_period = [
            x for x in event_features if type(x) == pd.core.resample.TimeGrouper
        ][0]

        df2 = self.add_bounding_rows(
            df2,
            temporal_period,
            [x for x in event_features if type(x) != pd.core.resample.TimeGrouper],
        )

        def wavg(group):
            try:
                d = group["net_qty"]
                w = group["timedelta"]
                return (d * w).sum() / w.sum()
            except ZeroDivisionError:
                return None

        df2["timedelta"] = (
            df2.reset_index()
            .groupby(event_features)["timestamp"]
            .transform(lambda x: x.shift(-1) - x)
            .values
        )
        wnq_view = df2.reset_index().groupby(event_features).apply(wavg)
        wnq_view.name = "weighted_net_qty"
        return wnq_view

    @staticmethod
    def create_sharpe_ratio(df, returns_col="hourly_returns", periods=252):
        """
        Create the Sharpe ratio for the strategy, based on a
        benchmark of zero (i.e. no risk-free rate information).
        Parameters:
        returns - A pandas Series representing period percentage returns.
        periods - Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
        """

        return np.sqrt(periods) * (np.mean(df[returns_col])) / np.std(df[returns_col])

    @staticmethod
    def create_expected_return(df, returns_col="hourly_returns"):
        return np.mean(df[returns_col])

    @staticmethod
    def create_expected_volatility(df, returns_col="hourly_returns"):
        return np.std(df[returns_col])

    @staticmethod
    def final_realised_pnl(df, returns_col="rpnl_cum"):
        idx = [i for i, v in enumerate(df.columns) if v == returns_col][0]
        return df.iat[-1, idx]

    @staticmethod
    def create_drawdown(df, col="rpnl_cum", feature="absolute"):
        df = df.copy().set_index("timestamp")
        d = {}

        # types ratio notional
        roll_max = df[col].cummax()
        # TODO fix ratio drawdown as a proportion of your current position
        if feature == "ratio":
            daily_drawdown = df[col] / roll_max
            df["drawdown"] = daily_drawdown
            df["max_drawdown"] = daily_drawdown.cummin()
            max_drawdown = df.iloc[-1].at["max_drawdown"].item()
            drawdown_end_idx = df["max_drawdown"].idxmin()
            drawdown_start_idx = df.loc[:drawdown_end_idx, :][col].idxmax()

        elif feature == "absolute":
            daily_drawdown = roll_max - df[col]
            df["roll_max"] = roll_max
            df["drawdown"] = daily_drawdown
            max_drawdown = (
                df.loc[df["drawdown"] == df["drawdown"].max(), "drawdown"]
                .values[0]
                .item()
            )
            drawdown_end_idx = df.loc[df["drawdown"] == df["drawdown"].max()][
                col
            ].idxmax()
            drawdown_start_idx = df.loc[:drawdown_end_idx][
                df.loc[:drawdown_end_idx]["roll_max"]
                == df.loc[:drawdown_end_idx]["roll_max"].max()
                ]["roll_max"].idxmin()

        d[f"{feature}_duration"] = drawdown_end_idx - drawdown_start_idx
        d[f"{feature}_max_drawdown_start"] = df.loc[drawdown_start_idx].at[col].item()
        d[f"{feature}_max_drawdown_end"] = df.loc[drawdown_end_idx].at[col].item()
        d[f"{feature}_max_drawdown"] = max_drawdown
        drawdown_series = pd.Series(
            d,
            index=[
                f"{feature}_max_drawdown",
                f"{feature}_duration",
                f"{feature}_max_drawdown_start",
                f"{feature}_max_drawdown_end",
            ],
        )

        return drawdown_series
