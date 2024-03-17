from enum import Enum
from pykrx import stock


def stock_symbols(stock_exchange):
    df_krx = stock_exchange.fetch_kosdaq_symbols()
    # print(df_krx.columns)
    df_krx = df_krx.sort_values(by="ROE", ascending=False).head(100)
    return list(zip(df_krx["한글명"], df_krx["단축코드"]))


class upbit_symbols(Enum):
    BTC = "BTC/KRW"
    ETH = "ETH/KRW"
    XRP = "XRP/KRW"
    EOS = "EOS/KRW"


class bybit_symbols(Enum):
    BTC = "BTCUSD"
    ETH = "ETHUSD"
    XRP = "XRPUSD"
    EOS = "EOSUSD"
