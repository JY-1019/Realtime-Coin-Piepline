import os
import json
import uuid
import pprint
import asyncio
import requests
import datetime
import ccxt.pro as ccxtpro


EXCHANGE_OPEN_URL = (
    "https://quotation-api-cdn.dunamu.com/v1/forex/recent?codes=FRX.KRWUSD"
)


def get_upbit_exchange():
    current_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(current_path, "config.json"), "r") as config_file:
        config = json.load(config_file)
        upbit_api_key = config["upbit_access_key"]
        upbit_secret_key = config["upbit_secret_key"]

    return ccxtpro.upbit(
        {
            "apiKey": upbit_api_key,
            "secret": upbit_secret_key,
            "enableRateLimit": True,
        }
    )


def get_bybit_exchange():
    current_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(current_path, "config.json"), "r") as config_file:
        config = json.load(config_file)
        bybit_api_key = config["bybit_access_key"]
        bybit_secret_key = config["bybit_secret_key"]

    return ccxtpro.bybit(
        {
            "apiKey": bybit_api_key,
            "secret": bybit_secret_key,
            "enableRateLimit": True,
        }
    )


def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


async def send_coin_data_to_kafka(exchange, symbol, producer, topic):
    try:
        guid = uuid.uuid4()
        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
        # timestamp는 밀리세컨드 단위까지
        timestamp = round(now.timestamp() * 1000)
        isotime = now.isoformat()
        exchange_data = requests.get(EXCHANGE_OPEN_URL).json()
        ticker = await exchange.watch_ticker(symbol)
        json_ticker = json.dumps(
            {   
                "guid" : str(guid),
                "timestamp" : timestamp,
                "isotime" : isotime,
                "symbol": symbol,
                "data": ticker,
                "exchangeRate": exchange_data[0]["basePrice"],
            }
        )
        # pprint.pprint(json_ticker)
        producer.produce(
            topic, value=json_ticker.encode("utf-8"), callback=delivery_report
        )
        producer.flush()
    except Exception as error:
        print("Exception occurred: {}".format(error))



async def send_multiple_coins_to_kafka(exchange, symbols: list, producer, topic):
    while True :
        coins = [
            send_coin_data_to_kafka(exchange, symbol, producer, topic) for symbol in symbols
        ]
        await asyncio.gather(*coins)
        await exchange.close()
        