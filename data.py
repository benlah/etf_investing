import sys
sys.path.append('../')

import yfinance as yf
import pandas as pd
import pickle as pkl
from typing import Any

import os
ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__)))
screener_etf = os.path.join(ROOT_DIR, 'screener-etf.xlsx')
data_universe = os.path.join(ROOT_DIR, 'data_storage', 'data_universe.pkl')
data_histo_price = os.path.join(ROOT_DIR, 'data_storage', 'data_histo_price.pkl')


class Data:
    def __init__(self, updated: bool = True) -> None:
        self.universe = self.load_universe(symbol_only=False)
        self.ticker_universe = self.load_universe(symbol_only=True)
        self.price = self.load_price(updated=updated)

    def load_universe(self, symbol_only: bool = False) -> pd.DataFrame | pd.Series:
        with open(data_universe, "rb") as universe:
            if symbol_only:
                return pkl.load(universe)["Symbol"]
            else:
                return pkl.load(universe)

    def load_price(self, updated: bool = True) -> pd.DataFrame:
        with open(data_histo_price, "rb") as histo_price:
            if updated:
                self.update_price()
            return pkl.load(histo_price)

    def download_price(self, tick_set: set, start: Any | None = None, end: Any | None = None) -> pd.DataFrame:
        price = yf.download(list(tick_set), start=start, end=end, threads=False)
        df_price = (
            price.stack()
            .reset_index()
            .rename(columns={"level_1": "Symbol"})
            .sort_values(["Symbol", "Date"])
            .set_index(["Symbol", "Date"])
        )
        return df_price

    def update_price(self) -> str:
        if os.path.getsize(data_histo_price) > 0:
            df = self.load_price(updated=False)
            if df.index.max() == pd.Timestamp.now().date() - pd.offsets.BDay(1):
                return print(f'Prices already updated as of {df.index.max().date()}')
            else:
                df_updated = pd.concat(
                    [
                        df,
                        self.download_price(
                            self.ticker_universe,
                            start=df.index.max() + pd.offsets.Day(1),
                            end=pd.Timestamp.now().date() - pd.offsets.BDay(1)
                        ),
                    ],
                    axis=1,
                )
                with open(data_histo_price, "wb") as histo_price:
                    pkl.dump(df_updated, histo_price)
                return print(f'Prices successfully updated as of {df_updated.index.max().date()}')
        else:
            print(f'Pickel {data_histo_price} was empthy, reloading of all data...')
            df_updated = self.download_price(self.ticker_universe)
            with open(data_histo_price, "wb") as histo_price:
                pkl.dump(df_updated, histo_price)
            return print(f'Prices successfully updated as of {df_updated.index.max().date()}')

    def update_universe(self) -> str:
        universe = pd.read_excel(io=screener_etf)
        with open(data_universe, "wb") as f:
            pkl.dump(universe, f)
        return print(f'Universe successfully updated with {len(universe)} tickers')
