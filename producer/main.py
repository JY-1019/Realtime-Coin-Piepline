import asyncio
import threading
from coin_producer import *
from stock_producer import *
from symbols import upbit_symbols, bybit_symbols, stock_symbols
from confluent_kafka import Producer

def send_coins_to_kafka(exchange, coins, producer, topic) :
    asyncio.run(send_multiple_coins_to_kafka(exchange, coins, producer, topic))


async def main():
    upbit_exchange = get_upbit_exchange()
    bybit_exchange = get_bybit_exchange()
    # stock_exchange = get_stock_exchange()

    upbit_coins = [coin.value for coin in upbit_symbols]
    bybit_coins = [coin.value for coin in bybit_symbols]

    # stocks = stock_symbols(stock_exchange)
    kafka_brokers = "broker1:9092"
    producer_config = {"bootstrap.servers": kafka_brokers}
    producer = Producer(producer_config)

    while True:
        threads = [
            threading.Thread(target=send_coins_to_kafka,            
                            args = (upbit_exchange, upbit_coins, producer, "upbit")) ,
            threading.Thread(target=send_coins_to_kafka,
                            args = (bybit_exchange, bybit_coins, producer, "bybit"))
        ]

        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    asyncio.run(main())
