# get_alert_history
# get_alert_stats
# get_configs
# update_config

import psycopg2
from utils.config import POSTGRES_HOST_INTERNAL, POSTGRES_PORT_EXTERNAL, POSTGRES_PORT_INTERNAL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

def get_pg_internal_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        host=POSTGRES_HOST_INTERNAL,
        user=POSTGRES_USER,
        port=POSTGRES_PORT_INTERNAL,
        password=POSTGRES_PASSWORD
    )

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

def save_ohlcv(symbol, timeframe, timestamp, open, high, low, close, volume, buyer, cnt):
    conn = None
    cur = None
    try: 
        conn = get_pg_internal_connection()
        cur = conn.cursor()
        
        # Dùng UPSERT: Nếu trùng (symbol, timestamp, timeframe) thì UPDATE đè lên
        cur.execute(
            """
            INSERT INTO ohlcv_candles (symbol, timestamp, timeframe, open, high, low, close, volume, buyer, cnt)
            VALUES (%s, CAST(%s AS TIMESTAMP), %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timeframe, timestamp) 
            DO UPDATE SET 
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                buyer = EXCLUDED.buyer,
                cnt = EXCLUDED.cnt;
            """,
            (symbol, timestamp, timeframe, open, high, low, close, volume, buyer, cnt)
        )
        conn.commit()
        print(f'[Postgre] UPSERT nến thành công: {symbol} - {timeframe}')
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f'[Postgre] Lỗi lưu nến OHLCV - {e}')
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()