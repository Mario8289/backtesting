import pytest
import  datetime as dt
from unittest.mock import Mock
from backtesting.strategy.dca import DCA
from backtesting.event import Event
from backtesting.order import Order
from backtesting.portfolio import Portfolio
from backtesting.portfolio import Position
from backtesting.exit_strategy import AbstractExitStrategy


@pytest.fixture
def mock_exit_strategy():
    return Mock(spec=AbstractExitStrategy)


@pytest.fixture
def dca(mock_exit_strategy):
    dca = DCA(
        account_id=1,
        contract_qty=0.1,
        time="11:00:00",
        day='monday',
        freq='1d',
        exit_strategy=mock_exit_strategy
    )
    date = dt.datetime.strptime("2024-12-01", "%Y-%m-%d").date()
    dca.update(start_date=date)
    return dca


class TestDCA:

    def test_initialization(self, dca):
        assert dca.account_id == 1
        assert dca.contract_qty == 0.1
        assert dca.day == "Monday"
        assert dca.time == "11:00:00"
        assert dca.freq == '1d'

    def test_create(self):
        strategy = DCA.create(
            account_id=1,
            contract_qty=0.1,
            time="11:00",
            day='monday',
            freq='daily',
            exit_strategy=mock_exit_strategy
        )
        assert isinstance(strategy, DCA)
        assert strategy.account_id == 1

    def test_place_trade(self, dca):
        mock_portfolio = Mock()
        mock_portfolio.positions = {}

        datetime = "2024-12-02 11:00:00"
        datetime = dt.datetime.strptime(datetime, "%Y-%m-%d %H:%M:%S")
        mock_event = Mock(spec=Event)
        mock_event.get_timestamp = Mock(return_value=datetime)
        mock_event.timestamp = datetime
        mock_event.timestamp_millis = datetime.timestamp() * 1000
        mock_event.source = 'source'
        mock_event.symbol_id = 'symbol_id'
        mock_event.symbol = 'symbol'
        mock_event.price = 1.1

        orders = dca.calculate_market_signals(mock_portfolio, mock_event)

        assert dca.next_trade_timestamp == datetime + dt.timedelta(days=1)
        assert orders[0].contract_qty == 0.1
        assert len(orders) == 1

    def test_dont_place_trade_but_create_an_exit_order(self, dca, mock_exit_strategy):
        mock_portfolio = Mock(spec=Portfolio)
        position = Mock(spec=Position)

        mock_portfolio.positions = {
            ('source', 'symbol_id', 1): position
        }
        mock_portfolio.matching_method = 'mid_price'

        datetime = "2024-12-02 09:00:00"
        datetime = dt.datetime.strptime(datetime, "%Y-%m-%d %H:%M:%S")
        mock_event = Mock(spec=Event)
        mock_event.get_timestamp = Mock(return_value=datetime)
        mock_event.timestamp = datetime
        mock_event.timestamp_millis = datetime.timestamp() * 1000
        mock_event.source = 'source'
        mock_event.symbol_id = 'symbol_id'
        mock_event.symbol = 'symbol'
        mock_event.price = 1.1

        mock_exit_order = Mock(spec=Order)
        mock_exit_order.is_long = True
        mock_exit_order.cancelled = 0
        mock_exit_order.contract_qty = dca.contract_qty
        mock_exit_strategy.generate_exit_order_signal.return_value = [mock_exit_order]

        orders = dca.calculate_market_signals(mock_portfolio, mock_event)
        assert len(orders) == 1

    def test_dont_place_trade_and_no_exit(self, dca, mock_exit_strategy):
        mock_portfolio = Mock(spec=Portfolio)
        position = Mock(spec=Position)

        mock_portfolio.positions = {
        }
        mock_portfolio.matching_method = 'mid_price'

        datetime = "2024-12-02 09:00:00"
        datetime = dt.datetime.strptime(datetime, "%Y-%m-%d %H:%M:%S")
        mock_event = Mock(spec=Event)
        mock_event.get_timestamp = Mock(return_value=datetime)
        mock_event.timestamp = datetime
        mock_event.timestamp_millis = datetime.timestamp() * 1000
        mock_event.source = 'source'
        mock_event.symbol_id = 'symbol_id'
        mock_event.symbol = 'symbol'
        mock_event.price = 1.1

        mock_exit_order = Mock(spec=Order)
        mock_exit_order.is_long = True
        mock_exit_order.cancelled = 0
        mock_exit_order.contract_qty = dca.contract_qty
        mock_exit_strategy.generate_exit_order_signal.return_value = [mock_exit_order]

        orders = dca.calculate_market_signals(mock_portfolio, mock_event)
        assert len(orders) == 0

