import operator

import numpy as np


# todo: abstract position away, make sure you keep the portfolio-position relationship
class Position:
    __slots__ = (
        "name",
        "price_increment",
        "unit_price",
        "currency",
        "contract_unit_of_measure",
        "realised_pnl",
        "unrealised_pnl",
        "position_value",
        "net_position",
        "no_of_trades",
        "notional_traded",
        "notional_traded_net",
        "netting_engine",
        "open_positions",
        "exit_attr",
        "tighten_cost",
        "notional_rejected",
    )

    def __init__(
            self,
            name,
            unit_price,
            price_increment,
            netting_engine="fifo",
            currency=None,
            contract_unit_of_measure=None,
    ):
        self.name = name
        self.price_increment = price_increment
        self.unit_price = unit_price
        self.currency = currency
        self.contract_unit_of_measure = contract_unit_of_measure
        self.realised_pnl = 0
        self.unrealised_pnl = 0
        self.position_value = 0
        self.net_position = 0
        self.no_of_trades = 0
        self.notional_traded_net = 0
        self.notional_traded = 0
        self.notional_rejected = 0
        self.netting_engine = netting_engine
        self.open_positions: list = [] if self.netting_engine in [
            "fifo",
            "lifo",
        ] else None  # noqa
        self.exit_attr: dict = dict()
        self.tighten_cost: int = 0

    def get_price(self):
        if self.netting_engine in ["fifo", "lifo"]:
            if self.open_positions.__len__() != 0:
                return np.average(
                    [p.price for p in self.open_positions],
                    weights=[p.quantity for p in self.open_positions],
                )
                # return np.mean([p.price for p in self.open_positions])
            else:
                return 0
        elif self.netting_engine == "avg_price":
            if self.open_positions is not None:
                # todo: when using fifo/lifo, open_positions is a list of objects of type Position
                #       if avg_price, then open positions is a list of 1 element. but need to make that clearer
                return self.open_positions.price
            else:
                return 0

    def on_trade(self, quantity, price, rate_to_usd):
        self.no_of_trades += 1
        self.net_position += quantity
        self.notional_traded += (
                                        abs(price * quantity * self.unit_price) / 100000000
                                ) * rate_to_usd
        self.notional_traded_net += (
                                            (price * quantity * self.unit_price) / 100000000
                                    ) * rate_to_usd
        return self.update_realised_pnl(OpenPosition(quantity, price), rate_to_usd)

    def calculate_net_contracts(self):
        return self.net_position

    def is_long(self):
        return self.calculate_net_contracts() > 0

    def apply_netting(self, open_positions):
        if self.netting_engine == "lifo":
            return reversed(open_positions)
        else:
            return open_positions

    def update_realised_pnl(self, new_position, rate_to_usd):
        switcher = {
            "avg_price": self.update_realised_pnl_avg_price,
            "lifo": self.update_realised_pnl_ordered,
            "fifo": self.update_realised_pnl_ordered,
        }
        func = switcher.get(self.netting_engine)
        # todo: python doesn't do class method switching very nicely (or at least IntelliJ doesn't like it)
        #       maybe clean it up a bit
        return func(new_position, rate_to_usd)

    def update_realised_pnl_ordered(self, new_position, rate_to_usd):
        qty = new_position.quantity
        old_pnl = self.realised_pnl
        while qty != 0:
            open_positions = [
                p for p in self.open_positions if p.is_long != new_position.is_long
            ]
            if len(open_positions) > 0:
                op = operator.ge if new_position.is_long else operator.le
                for i, pos in enumerate(self.apply_netting(open_positions)):
                    qty += pos.quantity
                    if op(qty, 0):  # open position[i] fully filled
                        pnl = (
                                      (pos.quantity * (new_position.price - pos.price))
                                      * self.unit_price
                              ) / 100000000
                        self.realised_pnl += pnl * rate_to_usd
                        new_position.quantity += pos.quantity
                        self.open_positions.pop(0)
                    else:  # new_position fully filled
                        pnl = (
                                      (
                                              (new_position.quantity * -1)
                                              * (new_position.price - pos.price)
                                      )
                                      * self.unit_price
                              ) / 100000000
                        self.realised_pnl += pnl * rate_to_usd
                        pos.quantity += new_position.quantity
                        pos.cost = pos.price * pos.quantity
                        qty = 0
                        break
            else:
                self.open_positions.append(new_position)
                break
        return self.realised_pnl - old_pnl

    def update_realised_pnl_avg_price(self, new_position, rate_to_usd):
        old_pnl = self.realised_pnl
        # first position
        if self.open_positions is None:
            self.open_positions = new_position
        # extend position
        elif self.open_positions.is_long == new_position.is_long:
            # prices = [self.open_positions.price, new_position.price]
            quantities = [self.open_positions.quantity, new_position.quantity]
            costs = [self.open_positions.cost, new_position.cost]
            avg_price = int(sum(costs) / sum(quantities))
            # avg_price = int(np.average(prices, weights=quantities))
            self.open_positions = OpenPosition(
                sum(quantities), avg_price, cum_cost=sum(costs)
            )
        # relise, invert, partial relise position
        elif self.open_positions.is_long != new_position.is_long:

            quantity = self.open_positions.quantity - (new_position.quantity * -1)
            price_dif = self.open_positions.price - new_position.price

            if quantity == 0:  # close
                realised_qty = new_position.quantity
                self.open_positions = None

            # partial
            elif np.sign(quantity) == np.sign(self.open_positions.quantity):
                self.open_positions.quantity = quantity
                self.open_positions.cost = self.open_positions.cost + new_position.cost
                realised_qty = new_position.quantity
            # invert
            else:  # if np.sign(quantity) != np.sign(self.open_positions.quantity):
                realised_qty = self.open_positions.quantity * -1
                self.open_positions = OpenPosition(quantity, new_position.price)

            pnl = ((realised_qty * price_dif) * self.unit_price) / 100000000

            self.realised_pnl += pnl * rate_to_usd

        return self.realised_pnl - old_pnl

    def update_unrealised_pnl(self, evt_price, rate_to_usd):
        self.unrealised_pnl = 0
        if self.netting_engine in ["fifo", "lifo"]:
            for p in self.open_positions:
                open_p_unrealised_pnl = (
                                                (p.quantity * (evt_price - p.price)) * self.unit_price
                                        ) / 100000000
                self.unrealised_pnl += open_p_unrealised_pnl * rate_to_usd
        elif self.netting_engine == "avg_price":
            if self.open_positions:
                # todo: when using fifo/lifo, open_positions is a list of objects of type Position
                #       if avg_price, then open positions is a list of 1 element. but need to make that clearer
                open_p_unrealised_pnl = (
                                                (
                                                        self.open_positions.quantity
                                                        * (evt_price - self.open_positions.price)
                                                )
                                                * self.unit_price
                                        ) / 100000000
                self.unrealised_pnl += open_p_unrealised_pnl * rate_to_usd
            else:
                self.unrealised_pnl = 0


class OpenPosition:
    def __init__(self, quantity, price, cum_cost=None):
        self.quantity = quantity
        self.is_long = True if self.quantity > 0 else False
        self.price = price
        self.cost = (price * quantity) if cum_cost is None else cum_cost

    @classmethod
    def create_position_from_open_position_snapshot(cls, row, unit_price, risk=1):
        quantity: int = int(round(row.position * 100, 0) * risk)
        open_cost: int = row.open_cost * risk
        return cls(
            quantity,
            abs(int((row.open_cost / row.position / unit_price) * 1000000)),
            (open_cost * 100000000) / unit_price,
            )
