import sys
import os
import json
import websocket
from kafka import KafkaProducer
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from utils.config import (
    KAFKA_BROKER_EXTERNAL,
    TOPIC_TRADES,
    TOPIC_ALERTS,
    TOPIC_TRADES_PARTITIONS,
    BINANCE_WS_URL,
)


def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER_EXTERNAL],
            retries=5,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            key_serializer=lambda x: x.encode("utf-8") if x else None,
        )
        print(f"Producer had been created")
        return producer
    except Exception as e:
        print(f"Error in create Producer {e}")
        sys.exit(1)


def create_topic():
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER_EXTERNAL, client_id="crypto_ingestor_admin"
        )

        topic = [
            NewTopic(
                name=TOPIC_TRADES,
                num_partitions=TOPIC_TRADES_PARTITIONS,
                replication_factor=1,
            ),
            NewTopic(name=TOPIC_ALERTS, num_partitions=1, replication_factor=1),
        ]

        admin.create_topics(topic)
        print(f"Topic has been created")

    except TopicAlreadyExistsError as e:
        print("TOPIC TRADE was created")
    except NoBrokersAvailable as e:
        print(f"Kafka is broken - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Erorr in create topic trade {e}")


# {
#     "stream": "btcusdt@aggTrade",
#     "data": {
#         "e": "aggTrade",      Event type
#         "E": 1779091675833,   Event time
#         "s": "BTCUSDT",       Symbol
#         "a": 3959421941,      Aggregate trade ID
#         "p": "77155.70000000",Price
#         "q": "0.00074000",    Quantity
#         "f": 6303820575,      First trade ID
#         "l": 6303820575,      Last trade ID
#         "T": 1779091675832,   Trade time
#         "m": true,            Is the buyer the market maker?
#         "M": true,            Ignore
#     },
# }


def main():
    producer = create_producer()
    create_topic()

    def on_message(ws, mess):
        COIN_PARTITION_MAP = {
            "BTCUSDT": 0,
            "ETHUSDT": 1,
            "BNBUSDT": 2,
            "SOLUSDT": 3,
            "XRPUSDT": 4,
            "ADAUSDT": 5,
        }
        try:
            data = json.loads(mess)
            data = data.get("data", {})
            # print(data)
            #   log
            symbol = data.get("s", "")
            price = data.get("p", "")
            quantity = data.get("q", "")

            # push data -> kafka
            target= COIN_PARTITION_MAP.get(symbol, 0)
            producer.send(topic=TOPIC_TRADES, key=symbol, value=data, partition=target)

            print(f"{symbol}: Price-> {price} - Quantity-> {quantity}")
        except Exception as e:
            print(f"fail {e}")

    def on_error(ws, e):
        print(e)

    def on_close(ws, status, msg):
        print(status, msg)

    def on_open(ws):
        print("connected")

    ws = websocket.WebSocketApp(
        url=BINANCE_WS_URL,
        on_message=on_message,
        on_close=on_close,
        on_open=on_open,
        on_error=on_error,
    )

    ws.run_forever()


if __name__ == "__main__":
    main()
