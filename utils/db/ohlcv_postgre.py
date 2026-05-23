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


# ── API Query Functions ──

def get_candles(symbol="BTCUSDT", timeframe="1m", limit=100):
    """Lấy nến OHLCV từ PostgreSQL"""
    conn = None
    try:
        conn = get_pg_internal_connection()
        cur = conn.cursor()
        cur.execute(
            """SELECT symbol, timeframe, timestamp, open, high, low, close, volume, buyer, cnt
               FROM ohlcv_candles 
               WHERE symbol = %s AND timeframe = %s
               ORDER BY timestamp DESC LIMIT %s""",
            (symbol, timeframe, limit)
        )
        rows = cur.fetchall()
        return [
            {
                "symbol": r[0], "timeframe": r[1], 
                "timestamp": r[2].isoformat() if r[2] else None,
                "open": float(r[3]), "high": float(r[4]), 
                "low": float(r[5]), "close": float(r[6]),
                "volume": float(r[7]), "buyer": r[8], "cnt": r[9]
            }
            for r in rows
        ]
    except Exception as e:
        print(f'[Postgre] get_candles error: {e}')
        return []
    finally:
        if conn:
            conn.close()