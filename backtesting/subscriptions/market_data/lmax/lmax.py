from backtesting.subscriptions.subscription import Subscription


class LmaxMarketData(Subscription):
    __slots__ = ("auth", "directory")

    def __init__(self, auth, directory):
        self.auth = auth
        self.directory = directory
        super().__init__()

    def load(self, start_date: str, end_date: str):
        pass

    def subscribe(self):
        pass
