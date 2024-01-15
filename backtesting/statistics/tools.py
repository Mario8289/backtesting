import numpy as np


def normalised_return(returns):
    """
    This gives you normalised return based on the first item of the series
    :param returns:
    returns = A pandas series representing pnl
    :return:
    """
    return returns / returns[0]


def create_sharpe_ratio(returns, periods=252):
    """
    Create the Sharpe ratio for the strategy, based on a
    benchmark of zero (i.e. no risk-free rate information).
    Parameters:
    returns - A pandas Series representing period percentage returns.
    periods - Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
    """
    return np.sqrt(periods) * (np.mean(returns)) / np.std(returns)


def create_sortino_ratio(returns, periods=252):
    """
    Create the Sortino ratio for the strategy, based on a
    benchmark of zero (i.e. no risk-free rate information).
    Parameters:
    returns - A pandas Series representing period percentage returns.
    periods - Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
    """
    return np.sqrt(periods) * (np.mean(returns)) / np.std(returns[returns < 0])


# def create_drawdowns(returns):
#     """
#     Calculate the largest peak-to-trough drawdown of the equity curve
#     as well as the duration of the drawdown. Requires that the
#     pnl_returns is a pandas Series.
#     Parameters:
#     equity - A pandas Series representing period percentage returns.
#     Returns:
#     drawdown, drawdown_max, duration
#     """
#     # Calculate the cumulative returns curve
#     # and set up the High Water Mark
#     idx = returns.index
#     hwm = np.zeros(len(idx))
#
#     # Create the high water mark
#     for t in range(1, len(idx)):
#         hwm[t] = max(hwm[t - 1], returns.ix[t])
#
#     # Calculate the drawdown and duration statistics
#     perf = pd.DataFrame(index=idx)
#     perf["Drawdown"] = (hwm - returns) / hwm
#     perf["Drawdown"].ix[0] = 0.0
#     perf["DurationCheck"] = np.where(perf["Drawdown"] == 0, 0, 1)
#     duration = max(
#         sum(1 for i in g if i == 1)
#         for k, g in groupby(perf["DurationCheck"])
#     )
#     return perf["Drawdown"], np.max(perf["Drawdown"]), duration
