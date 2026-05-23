import os
import sys
import json
import requests
import time
from kafka import KafkaConsumer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from utils.config import TELEGRAM_CHAT_ID, TELEGRAM_TOKEN, KAFKA_BROKER_INTERNAL, TOPIC_ALERTS, TOPIC_CANDLES_1M, TOPIC_CANDLES_5M

from utils.schema.whale import parse_whale_alert, format_whale_alert
from utils.db.alert_redis import check_rate_limit, set_rate_limit, cache_alert
from utils.db.alert_postgre import save_alert_history
from utils.schema.ohlcv import parse_ohlcv_alert, format_ohlcv_alert
from utils.db.ohlcv_postgre import save_ohlcv
from utils.db.ohlcv_redis import set_candle

def send_telegram_message(mess):
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "parse_mode": "Markdown",
        'text':mess
    }

    for i in range(3):
        try:
            r=requests.post(url, json=payload, timeout=5)
            if r.status_code==200:
                return True
            else:
                print(f'Error for send mess to user - {r.text}. Server will try again {i+1}') 
        except Exception as e:
            print(f'Error send mess. Here is error {e}. Server will try again')
        time.sleep(1)
    return False # miss message


def consume_whale_alerts(consumer):
    print("🦈 Started Whale Alert Consumer...")
    for message in consumer:
        raw = message.value
        alert = parse_whale_alert(raw)
        if not alert:
            print(f"Message is None: {raw}")
            continue
            
        print(f"{'─' * 40}")
        print(f"🐳 System had been received: {alert.symbol} — ${alert.total_usd:,.2f}")

        if not check_rate_limit(alert.symbol):
            print(f"Rate limited: {alert.symbol} — bỏ qua (đã gửi gần đây)")
            continue

        mess_to_user = format_whale_alert(alert)
        success = send_telegram_message(mess_to_user)

        if success:
            print(f'Send message Successfully')
            set_rate_limit(alert.symbol, 10) # Redis
            save_alert_history(alert.symbol, alert.total_usd) # Postgres
            cache_alert(alert.to_dict()) # Redis
        else:
            print(f'Error in send message after 3 tries')


# to_json(struct(
#             col('symbol'),
#             col('window.start').cast(StringType()).alias('timestamp'),
#             lit('5 minutes').alias('timeframe'),
#             col('open'),
#             col('high'),
#             col('low'),
#             col('close'),
#             col('volume'),
#             (col('isBuyer')-col('isSeller')).alias('buyer'),
#             col('cnt')
#         )).alias("value")

def consume_ohlcv(consumer, timeframe_name):
    print(f"📊 Started OHLCV {timeframe_name} Consumer...")
    for message in consumer:
        raw = message.value
        ohlcv_alert = parse_ohlcv_alert(raw) 
        
        if not ohlcv_alert:
            print(f"Message is None: {raw}")
            continue

        mess_to_user = format_ohlcv_alert(ohlcv_alert)
        success = send_telegram_message(mess_to_user)

        if success:
            print(f'Send {timeframe_name} message Successfully')
        else:
            print(f'Error in send {timeframe_name} message after 3 tries')

        # Luôn lưu candle bất kể Telegram thành công hay thất bại
        set_candle(ohlcv_alert.symbol, ohlcv_alert.timeframe, ohlcv_alert.timestamp, ohlcv_alert.open,
            ohlcv_alert.high, ohlcv_alert.low, ohlcv_alert.close, ohlcv_alert.volume, ohlcv_alert.buyer, ohlcv_alert.cnt)
     
        save_ohlcv(ohlcv_alert.symbol, ohlcv_alert.timeframe, ohlcv_alert.timestamp, ohlcv_alert.open,
            ohlcv_alert.high, ohlcv_alert.low, ohlcv_alert.close, ohlcv_alert.volume, ohlcv_alert.buyer, ohlcv_alert.cnt)

import threading
def main():
    # conn Kafka
    while True:
        try:
            consumer_alert = KafkaConsumer(
                TOPIC_ALERTS,
                bootstrap_servers=[KAFKA_BROKER_INTERNAL],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='whale-group',                         
                auto_offset_reset='earliest'                   
            )
            print('✅ Kafka [Whale Alert] connected')
            break
        except Exception as e:
            time.sleep(2)

    while True:
        try:
            consumer_ohlcv_1m = KafkaConsumer(
                TOPIC_CANDLES_1M,
                bootstrap_servers=[KAFKA_BROKER_INTERNAL],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='ohlcv-1m-group',                      # Tách group_id riêng
                auto_offset_reset='earliest'
            )
            print('✅ Kafka [OHLCV 1m] connected')
            break
        except Exception as e:
            time.sleep(2)
    
    while True:
        try:
            consumer_ohlcv_5m = KafkaConsumer(
                TOPIC_CANDLES_5M,
                bootstrap_servers=[KAFKA_BROKER_INTERNAL],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='ohlcv-5m-group',                      # Tách group_id riêng
                auto_offset_reset='earliest'
            )
            print('✅ Kafka [OHLCV 5m] connected')
            break
        except Exception as e:
            time.sleep(2)

    t_alert = threading.Thread(
        target=consume_whale_alerts, 
        args=(consumer_alert,), 
        daemon=True # Daemon giúp thread tự chết khi file python bị tắt
    )
    t_1m = threading.Thread(
        target=consume_ohlcv, 
        args=(consumer_ohlcv_1m, "1m"), 
        daemon=True
    )
    t_5m = threading.Thread(
        target=consume_ohlcv, 
        args=(consumer_ohlcv_5m, "5m"), 
        daemon=True
    )

    t_alert.start()
    t_1m.start()
    t_5m.start()

    try:
        t_alert.join()
        t_1m.join()
        t_5m.join()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down all consumers...")
        

if __name__ == '__main__':
    main()