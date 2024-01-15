import unittest

import pandas as pd

from risk_backtesting.model.profiling_ranks import ProfilingRankModel


class ProfilingRankModelTests(unittest.TestCase):
    def test_retrain_model_on_first_train_event(self):

        rank_evaluation_strings: dict = {
            "rank1": "profitable_rpnl_ratio > losing_rpnl_ratio",
            "rank2": "profitable_rpnl_trades_cnt > losing_rpnl_trades_cnt",
        }

        cp: pd.DataFrame = pd.DataFrame(
            data={
                "profitable_rpnl_ratio": [0.6, 0.2, 0.6],
                "losing_rpnl_ratio": [0.2, 0.6, 0.2],
                "profitable_rpnl_trades_cnt": [2, 2, 4],
                "losing_rpnl_trades_cnt": [2, 2, 2],
            },
            index=[1, 2, 3],
        )

        model = ProfilingRankModel(rank_evaluation_string=rank_evaluation_strings)

        model.train(cp)

        self.assertEqual(model.score, {1: 1, 2: 0, 3: 2})


if __name__ == "__main__":
    unittest.main()
