import stats


class Signals:
    def __init__(self, universe, ticker_universe, price) -> None:
        self.universe = universe
        self.ticker_uiverse = ticker_universe
        self.price = price
        self.ret = self.get_return_df()

    def get_return_df(self):
        return (
            self.price
            .groupby("Symbol")
            .pct_change()
        )

    def get_agg_ret(self, freq):
        return (
            self.ret
            .groupby('Symbol')
            .resample(freq, level=1)
            .apply(stats.compound)
        )
