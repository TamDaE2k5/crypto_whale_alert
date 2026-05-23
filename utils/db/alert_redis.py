import redis
from utils.config import REDIS_HOST_EXTERNAL, REDIS_HOST_INTERNAL, REDIS_PORT, REDIS_DB, RATE_LIMIT_SECONDS

client = None
def get_external_redis_client() -> redis.Redis:
    global client
    if not client:
        client = redis.Redis(
            host=REDIS_HOST_EXTERNAL,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
    return client

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

# for notifier
def set_rate_limit(symbol, ttl_rate):
    try:
        r = get_internal_redis_client()
        key = f"rate_limit:{symbol}"
        ttl = ttl_rate or RATE_LIMIT_SECONDS
        r.setex(key, ttl, "1")
        print(f"[Redis] Rate limit set: {symbol} ({ttl}s)")
    except Exception as e:
        print(f"[Redis] Lỗi set rate limit: {e}")

#for notifier
def check_rate_limit(symbol):
    try:
        r = get_internal_redis_client()
        key =f"rate_limit:{symbol}"
        return not r.exists(key)
    except Exception as e:
        print(f"[Redis] error in check rate limit: {e}")
        return True  # Nếu Redis lỗi → cho phép gửi

#for notifier
import json
def cache_alert(alert_data: dict): # for api ->redis
    try:
        r = get_internal_redis_client()
        symbol = alert_data.get("symbol", "UNKNOWN")
        
        # Lưu alert mới nhất theo symbol
        r.hset("latest_alerts", symbol, json.dumps(alert_data))
        
        # Thêm vào list alerts gần đây (giới hạn 100)
        r.lpush("recent_alerts", json.dumps(alert_data))
        r.ltrim("recent_alerts", 0, 99)
        
        # Tăng counter alerts hôm nay
        r.incr("stats:alerts_today")
        # Tự reset lúc nửa đêm (TTL 24h)
        r.expire("stats:alerts_today", 86400)
        
        print(f"[Redis] Cached alert: {symbol}")
    except Exception as e:
        print(f"[Redis] Lỗi cache alert: {e}")


# ── API Query Functions ──

def get_recent_alerts(limit=20):
    """Lấy danh sách alerts gần đây từ Redis"""
    try:
        r = get_internal_redis_client()
        raw_list = r.lrange("recent_alerts", 0, limit - 1)
        return [json.loads(item) for item in raw_list]
    except Exception as e:
        print(f"[Redis] get_recent_alerts error: {e}")
        return []


def cache_price(symbol, price):
    """Cache giá realtime vào Redis"""
    try:
        r = get_internal_redis_client()
        r.hset("latest_prices", symbol, json.dumps({
            "price": float(price),
            "updated_at": __import__('datetime').datetime.utcnow().isoformat()
        }))
    except Exception as e:
        print(f"[Redis] cache_price error: {e}")


def get_latest_prices():
    """Lấy giá mới nhất tất cả coins từ Redis"""
    try:
        r = get_internal_redis_client()
        raw = r.hgetall("latest_prices")
        result = {}
        for symbol, data in raw.items():
            result[symbol] = json.loads(data)
        return result
    except Exception as e:
        print(f"[Redis] get_latest_prices error: {e}")
        return {}