"""
Cấu hình chung cho toàn bộ hệ thống.
Tất cả services import từ đây để đảm bảo đồng nhất.
"""
import os

# ──────────────────────────────────────────────
# Kafka
# ──────────────────────────────────────────────
KAFKA_BROKER_EXTERNAL = os.getenv("KAFKA_BROKER", "localhost:9092")      # Từ host machine
KAFKA_BROKER_INTERNAL = os.getenv("KAFKA_BROKER_INTERNAL", "kafka:29092")  # Từ Docker network

TOPIC_TRADES = "crypto_trades"
TOPIC_ALERTS = "whale_alerts"
TOPIC_TRADES_PARTITIONS = 6

# ──────────────────────────────────────────────
# Redis
# ──────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Rate limit: thời gian tối thiểu giữa 2 lần gửi alert cho cùng 1 coin (giây)
RATE_LIMIT_SECONDS = int(os.getenv("RATE_LIMIT_SECONDS", "60"))

# ──────────────────────────────────────────────
# PostgreSQL
# ──────────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
POSTGRES_DB = os.getenv("POSTGRES_DB", "whale_watch")

POSTGRES_DSN = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ──────────────────────────────────────────────
# Telegram
# ──────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ──────────────────────────────────────────────
# Binance WebSocket
# ──────────────────────────────────────────────
TRACKED_COINS = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "ordiusdt", "xrpusdt"]

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join([f"{coin}@aggTrade" for coin in TRACKED_COINS])
)

# ──────────────────────────────────────────────
# Whale Thresholds (USD) — Ngưỡng mặc định
# ──────────────────────────────────────────────
DEFAULT_THRESHOLDS = {
    "BTCUSDT": 500000.0,
    "ETHUSDT": 20000.0,
    "SOLUSDT": 10000.0,
    "BNBUSDT": 10000.0,
    "ORDIUSDT": 5000.0,
    "XRPUSDT": 5000.0,
}
DEFAULT_THRESHOLD = 10000.0  # Cho các coin không có trong config
