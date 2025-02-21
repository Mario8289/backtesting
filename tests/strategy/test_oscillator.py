import pytest
from unittest.mock import Mock
from backtesting.strategy.oscillator import Oscillator
from backtesting.event import Event
from backtesting.order import Order
from backtesting.exit_strategy import AbstractExitStrategy


@pytest.fixture
def mock_exit_strategy():
    return Mock(spec=AbstractExitStrategy)


@pytest.fixture
def oscillator(mock_exit_strategy):
    return Oscillator(
        account_id=1,
        opening_qty=10.0,
        trade_qty=1.0,
        long_trade_limit=3,
        short_trade_limit=3,
        min_position_size=0.0,
        max_position_size=10.0,
        exit_strategy=mock_exit_strategy
    )


class TestThresholdBouncer:

    def test_initialization(self, oscillator):
        assert oscillator.account_id == 1
        assert oscillator.opening_qty == 10.0
        assert oscillator.trade_qty == 1.0
        assert oscillator.long_trade_limit == 3
        assert oscillator.short_trade_limit == 3
        assert oscillator.min_position_size == 0.0
        assert oscillator.max_position_size == 10.0
        assert oscillator.long_trade_cnt == 0
        assert oscillator.short_trade_cnt == 0

    def test_create(self):
        strategy = Oscillator.create(
            account_id=1,
            opening_qty=10.0,
            trade_qty=5.0,
            long_trade_limit=3,
            short_trade_limit=3,
            min_position_size=0.0,
            max_position_size=20.0,
            exit_strategy=Mock(spec=AbstractExitStrategy)
        )
        assert isinstance(strategy, Oscillator)
        assert strategy.account_id == 1

    def test_calculate_market_signals_with_no_positions(self, oscillator):
        mock_portfolio = Mock()
        mock_portfolio.positions = {}

        mock_event = Mock(
            timestamp_millis=1727773271000,
            price=1.1
        )

        orders = oscillator.calculate_market_signals(mock_portfolio, mock_event)
        assert len(orders) == 1

    def test_calculate_market_signals_trade_if_under_max_position(self, oscillator, mock_exit_strategy):
        mock_position = Mock()
        mock_position.get_price.return_value = 100.0
        mock_position.net_position = 8

        mock_portfolio = Mock()
        mock_portfolio.positions = {
            ("source", "symbol_id", 1): mock_position
        }
        mock_portfolio.matching_method = "matching_method"

        mock_event = Mock(spec=Event)
        mock_event.source = 'source'
        mock_event.symbol_id = 'symbol_id'
        mock_event.get_price.return_value = 105.0

        mock_exit_order = Mock(spec=Order)
        mock_exit_order.is_long = True
        mock_exit_order.contract_qty = oscillator.trade_qty
        mock_exit_order.cancelled = 0
        mock_exit_strategy.generate_exit_order_signal.return_value = [mock_exit_order]

        orders = oscillator.calculate_market_signals(mock_portfolio, mock_event)

        assert len(orders) == 1
        assert orders[0].cancelled == 0
        assert oscillator.long_trade_cnt == 1

    def test_calculate_market_signals_cancel_order_if_over_max_position(self, oscillator, mock_exit_strategy):
        mock_position = Mock()
        mock_position.get_price.return_value = 100.0
        mock_position.net_position = 10
        mock_position.get_price.return_value = 100

        mock_portfolio = Mock()
        mock_portfolio.positions = {
            ("source", "symbol_id", 1): mock_position
        }
        mock_portfolio.matching_method = "matching_method"

        mock_event = Mock(spec=Event)
        mock_event.source = 'source'
        mock_event.symbol_id = 'symbol_id'
        mock_event.get_price.return_value = 105.0

        mock_exit_order = Mock(spec=Order)
        mock_exit_order.is_long = True
        mock_exit_order.contract_qty = oscillator.trade_qty
        mock_exit_order.cancelled = 0
        mock_exit_strategy.generate_exit_order_signal.return_value = [mock_exit_order]

        orders = oscillator.calculate_market_signals(mock_portfolio, mock_event)

        assert len(orders) == 1
        assert orders[0].cancelled == 1
        assert oscillator.long_trade_cnt == 0

    def test_calculate_market_signals_short_trade_resets_long_trade_cnt(self, oscillator, mock_exit_strategy):
        oscillator.long_trade_cnt = 3
        oscillator.last_trade_sign = 1

        mock_position = Mock()
        mock_position.get_price.return_value = 100.0
        mock_position.net_position = 8
        mock_position.get_price.return_value = 100

        mock_portfolio = Mock()
        mock_portfolio.positions = {
            ("source", "symbol_id", 1): mock_position
        }

        mock_event = Mock(spec=Event)
        mock_event.source = "source"
        mock_event.symbol_id = "symbol_id"
        mock_event.get_price.return_value = 105.0

        mock_exit_order = Mock(spec=Order)
        mock_exit_order.is_long = False
        mock_exit_order.contract_qty = oscillator.trade_qty*-1
        mock_exit_order.cancelled = 0
        mock_exit_strategy.generate_exit_order_signal.return_value = [mock_exit_order]

        oscillator.calculate_market_signals(mock_portfolio, mock_event)

        mock_exit_order.is_long = True
        mock_exit_order.contract_qty = oscillator.trade_qty
        orders = oscillator.calculate_market_signals(mock_portfolio, mock_event)

        assert orders[0].cancelled == 0
        assert oscillator.long_trade_cnt == 1

    def test_calculate_market_signals_long_trade_limit_exceeded(self, oscillator, mock_exit_strategy):
        mock_position = Mock()
        mock_position.get_price.return_value = 100.0
        mock_position.net_position = .5

        mock_portfolio = Mock()
        mock_portfolio.positions = {
            ("source", "symbol_id", 1): mock_position
        }

        mock_event = Mock(spec=Event)
        mock_event.source = "source"
        mock_event.symbol_id = "symbol_id"
        mock_event.get_price.return_value = 105.0

        mock_exit_order = Mock(spec=Order)
        mock_exit_order.is_long = True
        mock_exit_order.cancelled = 0
        mock_exit_order.contract_qty = oscillator.trade_qty
        mock_exit_strategy.generate_exit_order_signal.return_value = [mock_exit_order]

        for _ in range(oscillator.long_trade_limit):
            oscillator.calculate_market_signals(mock_portfolio, mock_event)

        orders = oscillator.calculate_market_signals(mock_portfolio, mock_event)

        assert orders[0].cancelled == 1
        assert oscillator.long_trade_cnt == oscillator.long_trade_limit

    def test_on_state_with_market_data_event(self, oscillator):
        mock_portfolio = Mock()
        mock_event = Mock(spec=Event)
        mock_event.event_type = "market_data"

        oscillator.calculate_market_signals = Mock(return_value=["order1"])

        orders = oscillator.on_state(mock_portfolio, mock_event)
        assert orders == ["order1"]
        oscillator.calculate_market_signals.assert_called_once_with(mock_portfolio, mock_event)
