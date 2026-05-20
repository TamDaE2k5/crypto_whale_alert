import os
import sys
import json
import requests
import time
from kafka import KafkaConsumer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from utils.config import TELEGRAM_CHAT_ID, TELEGRAM_TOKEN, KAFKA_BROKER_INTERNAL, TOPIC_ALERTS

from utils.schema.whale import parse_whale_alert, format_whale_alert
from utils.db.alert_redis import check_rate_limit, set_rate_limit, cache_alert
from utils.db.alert_postgre import save_alert_history

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


def main():
    print("=" * 50)
    print("🔔 NOTIFIER SERVICE — Whale Watch System")
    print("=" * 50)

    # conn Kafka
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_ALERTS,
                bootstrap_servers=[KAFKA_BROKER_INTERNAL],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='notifier-group',
                auto_offset_reset='latest'
            )
            print(f'conn successfully')
            break
        except Exception as e:
            print(f'Error in Kafka Consumer - {e}')
            time.sleep(2)

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
            # set ttl -> not spam
            set_rate_limit(alert.symbol, 1) #redis
            save_alert_history(alert.symbol, alert.total_usd) #postgre

            cache_alert(alert.to_dict()) #redis
        else:
            print(f'Error in send message after 3 tries')
        

if __name__ == '__main__':
    main()