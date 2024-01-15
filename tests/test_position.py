import unittest
from risk_backtesting.position import Position


class TestPositionFifoLifo(unittest.TestCase):
    def test_fifo_buy_realised_on_short_position_non_USD_instrument(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.001,
            netting_engine="fifo",
        )
        current_positions = [(-0.1, 101.01), (-0.1, 101.02)]

        rate_to_usd = 0.010

        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, rate_to_usd)

        position.on_trade(0.1 * 100, 101.03 * 1000000, rate_to_usd)
        expected_realised_pnl = -0.2
        result = position.realised_pnl
        self.assertEqual(expected_realised_pnl, result)
        self.assertEqual(position.open_positions.__len__(), 1)
        self.assertEqual(position.open_positions[0].price, 101020000.0)

    def test_fifo_buy_realised_on_short_position(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(-0.1, 1.1), (-0.1, 1.2)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        position.on_trade(0.1 * 100, 1.0 * 1000000, 1)
        expected_realised_pnl = 100
        result = position.realised_pnl
        self.assertEqual(expected_realised_pnl, result)
        self.assertEqual(position.open_positions.__len__(), 1)
        self.assertEqual(position.open_positions[0].price, 1200000.0)

    def test_fifo_sell_realised_on_long_position(self):

        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(0.1, 1.0), (0.1, 1.1)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(-0.1 * 100, 1.2 * 1000000, 1)

        expected = 200
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_lifo_sell_realised_on_long_position(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="lifo",
        )
        current_positions = [(0.1, 1.0), (0.1, 1.1)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(-0.1 * 100, 1.2 * 1000000, 1)

        expected = 100
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_fifo_buy_realised_on_multiple_short_positions(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(-0.1, 1.2), (-0.1, 1.1), (-0.1, 1.0)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(0.2 * 100, 1.0 * 1000000, 1)

        expected = 300
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_lifo_buy_realised_on_muliple_short_positions(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="lifo",
        )
        current_positions = [(-0.1, 1.0), (-0.1, 1.1), (-0.1, 1.2)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(0.2 * 100, 1.0 * 1000000, 1)

        expected = 300
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_fifo_sell_realised_on_muliple_short_positions(self):

        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(0.1, 1.00000), (0.1, 1.00001), (0.1, 1.00002)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(-0.2 * 100, 1.00003 * 1000000, 1)

        expected = 0.05
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_lifo_sell_realised_on_muliple_long_positions(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="lifo",
        )
        current_positions = [(0.1, 1.2), (0.1, 1.1), (0.1, 1.0)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(-0.2 * 100, 1.3 * 1000000, 1)

        expected = 500
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_fifo_sell_realised_makes_a_loss_on_long_position(self):

        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(0.1, 1.2), (0.1, 1.0)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(-0.1 * 100, 1.1 * 1000000, 1)

        expected = -100
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_lifo_sell_realised_makes_a_loss_on_long_position(self):

        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="lifo",
        )
        current_positions = [(0.1, 1.0), (0.1, 1.2)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(-0.1 * 100, 1.1 * 1000000, 1)

        expected = -100
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_fifo_buy_realised_makes_a_loss_on_short_position(self):

        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(-0.1, 1.0), (-0.1, 1.1)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(0.1 * 100, 1.2 * 1000000, 1)

        expected = -200
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_lifo_buy_realised_makes_a_loss_on_short_position(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="lifo",
        )
        current_positions = [(-0.1, 1.1), (-0.1, 1.0)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(0.1 * 100, 1.2 * 1000000, 1)

        expected = -200
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_no_realised_when_sell_extends_short_position(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(-0.1, 1.00000), (-0.1, 1.00001)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1], 1)

        position.on_trade(-0.1, 1.00002, 1)

        expected = 0
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_no_realised_when_buy_extends_long_position(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(0.1, 1.0), (0.1, 1.1)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(0.1 * 100, 1.2 * 1000000, 1)

        expected = 0
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_fifo_buy_realised_of_partial_fill_on_short_position(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(0.2, 1.2), (0.2, 1.1), (0.2, 1.0)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(-0.3 * 100, 1.3 * 1000000, 1)

        expected = 400
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_lifo_buy_realised_of_partial_fill_on_short_position(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(0.2, 1.00000), (0.2, 1.00001), (0.2, 1.00002)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(-0.3 * 100, 1.00003 * 1000000, 1)

        expected = 0.08
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_fifo_sell_realised_of_partial_fill_on_long_position(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(-0.2, 1.00002), (-0.2, 1.00001), (-0.2, 1.00000)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(0.3 * 100, 1.0 * 1000000, 1)

        expected = 0.05
        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_lifo_sell_returns_partial_fill_on_long_position(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="lifo",
        )
        current_positions = [(-0.2, 1.00000), (-0.2, 1.00001), (-0.2, 1.00002)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(0.3 * 100, 1.0 * 1000000, 1)

        expected = 0.05

        result = round(position.realised_pnl, 2)
        self.assertEqual(expected, result)

    def test_partial_realised_open_position_quantity_is_reduced(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="lifo",
        )
        current_positions = [(-0.2, 1.00000), (-0.2, 1.00001), (-0.2, 1.00002)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(0.3 * 100, 1.00000 * 1000000, 1)

        expected = -10
        result = round(position.open_positions[0].quantity, 2)
        self.assertEqual(expected, result)

    def test_position_switches_position_from_long_to_short(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(0.1, 1.00000), (0.1, 1.00001)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(-0.3 * 100, 1.0 * 1000000, 1)

        expected = -10
        net = position.calculate_net_contracts()
        self.assertAlmostEqual(expected, net)

    def test_position_switches_position_from_short_to_long(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="fifo",
        )
        current_positions = [(-0.1, 1.00000), (-0.1, 1.00001)]
        for p in current_positions:
            position.on_trade(p[0] * 100, p[1] * 1000000, 1)

        position.on_trade(0.3 * 100, 1.0 * 1000000, 1)

        expected = 10
        result = position.calculate_net_contracts()
        self.assertAlmostEqual(expected, result)


class TestPositionAvg(unittest.TestCase):
    def test_extend_short_avg_position_then_parital_relised_pnl_win(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(-0.1, 1.1), (-0.1, 1.2)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(-20, position.open_positions.quantity)
        self.assertEqual(-23000000.0, position.open_positions.cost)
        self.assertEqual(1150000.0, position.open_positions.price)

        # test relised pnl on half position
        position.on_trade(0.1 * 100, 1.0 * 1000000, 1)

        self.assertEqual(-10, position.open_positions.quantity)
        self.assertEqual(1150000.0, position.open_positions.price)
        self.assertEqual(-13000000.0, position.open_positions.cost)
        self.assertEqual(150.0, position.realised_pnl)

    def test_extend_short_avg_position_then_parital_relised_pnl_loss(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(-0.1, 1.1), (-0.1, 1.2)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(-20, position.open_positions.quantity)
        self.assertEqual(-23000000.0, position.open_positions.cost)
        self.assertEqual(1150000.0, position.open_positions.price)

        # test relised pnl on half position
        position.on_trade(0.1 * 100, 1.3 * 1000000, 1)

        self.assertEqual(-10, position.open_positions.quantity)
        self.assertEqual(1150000.0, position.open_positions.price)
        self.assertEqual(-10000000.0, position.open_positions.cost)

        self.assertEqual(-150.0, position.realised_pnl)

    def test_extend_long_avg_position_then_parital_relised_pnl_win(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(0.1, 1.1), (0.1, 1.2)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(20, position.open_positions.quantity)
        self.assertEqual(1150000.0, position.open_positions.price)

        # test relised pnl on half position
        position.on_trade(-0.1 * 100, 1.3 * 1000000, 1)

        self.assertEqual(10, position.open_positions.quantity)
        self.assertEqual(1150000.0, position.open_positions.price)
        self.assertEqual(10000000.0, position.open_positions.cost)
        self.assertEqual(150.0, position.realised_pnl)

    def test_extend_long_avg_position_then_parital_relised_pnl_loss(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(0.1, 1.1), (0.1, 1.2)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(20, position.open_positions.quantity)
        self.assertEqual(1150000.0, position.open_positions.price)

        # test relised pnl on half position
        position.on_trade(-0.1 * 100, 1.0 * 1000000, 1)

        self.assertEqual(10, position.open_positions.quantity)
        self.assertEqual(1150000.0, position.open_positions.price)
        self.assertEqual(13000000.0, position.open_positions.cost)
        self.assertEqual(-150.0, position.realised_pnl)

    def test_avg_price_after_inverting_position_long_to_short(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(0.1, 1.1), (0.1, 1.2), (-0.3, 1.3)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(-10, position.open_positions.quantity)
        self.assertEqual(position.realised_pnl, 300)
        self.assertEqual(1300000.0, position.open_positions.price)

    def test_avg_price_after_inverting_position_short_to_long(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(-0.1, 1.1), (-0.1, 1.2), (0.3, 1.3)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(10, position.open_positions.quantity)
        self.assertEqual(position.realised_pnl, -300)
        self.assertEqual(1300000.0, position.open_positions.price)

    def test_close_long_position_fully_avg_price_win(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(0.1, 1.1), (0.1, 1.2), (-0.2, 1.3)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(None, position.open_positions)
        self.assertEqual(position.realised_pnl, 300)

    def test_close_long_position_fully_avg_price_loss(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(0.1, 1.1), (0.1, 1.2), (-0.2, 1.0)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(None, position.open_positions)
        self.assertEqual(position.realised_pnl, -300)

    def test_close_short_position_fully_avg_price_loss(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(-0.1, 1.1), (-0.1, 1.2), (0.2, 1.3)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(None, position.open_positions)
        self.assertEqual(position.realised_pnl, -300)

    def test_close_short_position_fully_avg_price_win(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        current_positions = [(-0.1, 1.1), (-0.1, 1.2), (0.2, 1.0)]
        for p in current_positions:
            contract_qty_std = p[0] * 100
            price_std = p[1] * 1000000
            position.on_trade(contract_qty_std, price_std, 1)

        self.assertEqual(None, position.open_positions)
        self.assertEqual(position.realised_pnl, 300)

    def test_close_avg_position_price_when_pos_reduced(self):
        position = Position(
            name="symbol2",
            unit_price=10000,
            price_increment=0.00001,
            netting_engine="avg_price",
        )

        positions = ((10, 1.2, 1200000), (5, 1.1, 1166666), (-4, 1.25, 1166666))
        for (qty, price, avg_price) in positions:
            contract_qty_std = qty * 100
            price_std = price * 1000000
            position.on_trade(contract_qty_std, price_std, 1)
            self.assertEqual(position.open_positions.price, avg_price)


if __name__ == "__main__":
    unittest.main()
