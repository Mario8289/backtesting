from typing import Dict

import numpy as np
import pandas as pd

from backtesting.model.base import AbstractModel

DEFAULT_RANK_EVALUATION_STRING: dict = {
    "rank1": "profitable_rpnl_ratio > losing_rpnl_ratio",
    "rank2": "profitable_rpnl_trades_cnt > losing_rpnl_trades_cnt",
    "rank3": "profitable_rpnl_avg_event > losing_rpnl_avg_event",
    "rank4": "profitable_rpnl_avg_hold_time > losing_rpnl_avg_hold_time",
    "rank5": "hft_trade_ratio > .5 and hft_total_rpnl_usd > 0",
}


class ProfilingRankModel(AbstractModel):
    __slots__ = ["name", "rank_evaluation_string"]

    def __init__(self, rank_evaluation_string: Dict = DEFAULT_RANK_EVALUATION_STRING):
        self.name: str = "profiling_ranks"
        self.rank_evaluation_string: Dict = str(rank_evaluation_string)
        self.rank_evaluation: Dict = rank_evaluation_string
        self.score: Dict = {}

    @staticmethod
    def evaluate_boolean(df: pd.DataFrame, eval_string: str) -> np.array:
        return np.where(df.eval(eval_string), 1, 0)

    def train(self, cp: pd.DataFrame):
        cp = self.calc_rank_score(cp)
        self.score = cp["score"].to_dict()

    def calc_rank_score(self, df: pd.DataFrame, include_score=True) -> pd.DataFrame:
        for col, eval_string in self.rank_evaluation.items():
            df[col] = self.evaluate_boolean(df, eval_string)
        if include_score:
            df["score"] = df[self.rank_evaluation.keys()].sum(axis=1)
        return df

    def predict(self, account_id):
        return self.score_booking_risk.get(self.score[account_id])
