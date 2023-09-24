import sys
sys.path.append('../')

import yfinance as yf
import pandas as pd
import pickle as pkl


class Data:
    def __init__(self, updated: bool = True) -> None:
        self.ticker_universe = self.load_universe(symbol_only=True)
        self.price = self.load_price(updated=updated)

    def load_universe(self, symbol_only: bool = False) -> pd.DataFrame | pd.Series:
        with open("data_universe.pkl", "rb") as universe:
            if symbol_only:
                return pkl.load(universe)["Symbol"]
            else:
                return pkl.load(universe)

    def load_price(self, updated: bool = True) -> pd.DataFrame:
        with open("data_histo_price.pkl", "rb") as histo_price:
            if updated:
                self.update_price()
                return pkl.load(histo_price)
            else:
                return pkl.load(histo_price)

    def download_price(self, tick_set: set, start: str | pd.Timestamp, end: str | pd.Timestamp = '9999-12-31') -> pd.DataFrame:
        price = yf.download(list(tick_set), start=start, end=end, threads=False)
        df_price = (
            price.stack()
            .reset_index()
            .rename(columns={"level_1": "Symbol"})
            .sort_values(["Symbol", "Date"])
            .set_index("Date")
        )
        return df_price

    def update_price(self) -> str:
        df = self.load_price(updated=False)
        if df.index.max() == pd.Timestamp.now().date() + pd.offsets.BDay(1) - pd.offsets.BDay(1):
            return print(f'Prices already updated as of {df.index.max().date()}')
        else:
            df_updated = pd.concat(
                [
                    df,
                    self.download_price(
                        self.ticker_universe,
                        start=df.index.max() + pd.offsets.Day(1),
                    ),
                ],
                axis=1,
            )
            with open("data_histo_price.pkl", "wb") as histo_price:
                pkl.dump(df_updated, histo_price)
            return print(f'Prices successfully updated as of {df_updated.index.max().date()}')

    def update_universe(self) -> str:
        universe = pd.read_excel(io='screener-etf.xlsx')
        with open("data_histo_price.pkl", "wb") as f:
            pkl.dump(universe, f)
        return print(f'Universe successfully updated with {len(universe)} tickers')
