# cache_price
# get_latest_prices
# get_recent_alerts
import redis
from utils.config import REDIS_HOST_INTERNAL, REDIS_PORT, REDIS_DB
import json

client = None

def get_internal_redis_client() -> redis.Redis:
    global client
    if not client:
        client = redis.Redis(
            host=REDIS_HOST_INTERNAL,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
    return client

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

def set_candle(symbol, timeframe, timestamp, open, high, low, close, volume, buyer, cnt):
    try:
        r = get_internal_redis_client()
        key = f"ohlcv:{timeframe}:{symbol}"
        
        candle_data = {
            "timestamp": timestamp,
            "open": float(open),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume),
            "delta_volume": float(buyer), 
            "trades_count": int(cnt)
        }
        # dict -> str
        r.set(key, json.dumps(candle_data))
        
        r.expire(key, 350) 
        
        print(f"[Redis] receive {key} - {r.get(key)}")
        
    except Exception as e:
        print(f"[Redis] Lỗi lưu nến OHLCV: {e}")


# ── API Query Functions ──

def get_latest_candle(symbol, timeframe):
    """Lấy nến đang chạy (chưa đóng) từ Redis"""
    try:
        r = get_internal_redis_client()
        key = f"ohlcv:{timeframe}:{symbol}"
        data = r.get(key)
        if data:
            return json.loads(data)
        return None
    except Exception as e:
        print(f"[Redis] get_latest_candle error: {e}")
        return None

