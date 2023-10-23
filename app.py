import streamlit as st
import pandas as pd
import numpy as np
from data import Data
import plotly.express as px


@st.cache_data(ttl=56000)
def data_cache():
    return Data(updated=True)


def app():
    data = data_cache()
    tickers = st.multiselect('Choose tickers', options=data.ticker_universe.values)
    st.plotly_chart(px.line(data.price[data.price['Symbol'].isin(tickers)], y='Adj Close', color='Symbol'))


if __name__ == '__main__':
    app()
