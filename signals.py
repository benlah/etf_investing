import stats
import numpy as np


class Signals:
    def __init__(self, universe, ticker_universe, price) -> None:
        self.universe = universe
        self.ticker_uiverse = ticker_universe
        self.price = price
        self.ret = self.get_return_df()
        self.signals = (
            self.ret
            .pipe(self.get_roll_mom, 20)
            .pipe(self.get_roll_mom, 60)
            .pipe(self.get_roll_mom, 252)
        )
        # self.ret_60_roll = self.get_roll_ret(self.ret, window=60)
        # self.ret_252_roll = self.get_roll_ret(self.ret, window=252)

    def get_return_df(self, col=['Adj Close']):
        return (
            self.price[col]
            # .drop(labels='Volume', axis=1)
            .groupby("Symbol")
            .pct_change()
        )

    def get_roll_ret(self, ret, window):
        return (
            ret
            .dropna(axis=0, how='all')
            .groupby('Symbol')
            .rolling(window=window, min_periods=window)
            .apply(stats.aggregate_ret)
        )

    def get_roll_vol(self, ret, window):
        return (
            ret.dropna(axis=0, how='all')
            .groupby('Symbol')
            .rolling(window=window, min_periods=window)
            .apply(np.std)
            .mul(np.sqrt(window))
        )

    def get_roll_mom(self, ret, window):
        ret[f'roll_mom_{window}'] = (
            ret['Adj Close']
            .dropna(axis=0, how='all')
            .groupby('Symbol')
            .rolling(window=window, min_periods=window)
            .apply(lambda x: stats.aggregate_ret(x) / (np.std(x) * np.sqrt(window)))
            .droplevel(0, axis=0)
        )
        return ret

    def get_sma_ret(self, window=60, period=20):
        return (
            (1 + self.ret
                .dropna(axis=0, how='all')
                .groupby('Symbol')
                .rolling(window)
                .mean())**period - 1
        )

    def get_smv_ret(self, window=60, period=20):
        return (
            self.ret
            .dropna(axis=0, how='all')
            .groupby("Symbol")
            .rolling(window)
            .std()
            .mul(np.sqrt(period))
        )

    def get_sma(self, col=['Adj Close'], window=200):
        return (
            self.price[col]
            .groupby("Symbol")
            .rolling(window)
            .mean()
        )

    def get_ema(self, col=['Adj Close'], window=200):
        return (
            self.price[col]
            .groupby("Symbol")
            .ewm(span=window, min_periods=window, adjust=True)
            .mean()
        )

    def get_mom_signal(self, freq):
        return (
            (self.get_agg_ret(freq=freq) - self.get_sma_ret(window=60, period=20)) / self.get_smv_ret(window=60, period=20)
        )
