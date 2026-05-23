import sys
import os
import json
import asyncio
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from kafka import KafkaConsumer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.config import (
    KAFKA_BROKER_INTERNAL, TOPIC_TRADES, TOPIC_ALERTS,
    TOPIC_CANDLES_1M, TOPIC_CANDLES_5M, DEFAULT_THRESHOLDS
)
from utils.db.alert_redis import (
    get_recent_alerts, get_latest_prices, cache_price, cache_alert
)
from utils.db.alert_postgre import get_alert_history, get_alert_stats
from utils.db.ohlcv_postgre import get_candles
from utils.db.ohlcv_redis import get_latest_candle


# ════════════════════════════════════════════════
# Timeframe mapping: frontend uses "1m"/"5m", DB uses "1 minute"/"5 minutes"
# ════════════════════════════════════════════════

TIMEFRAME_MAP = {
    "1m": "1 minute",
    "5m": "5 minutes",
    "1 minute": "1 minute",
    "5 minutes": "5 minutes",
}

def resolve_timeframe(tf: str) -> str:
    """Convert frontend timeframe shorthand to DB format"""
    return TIMEFRAME_MAP.get(tf, tf)


# ════════════════════════════════════════════════
# WebSocket Connection Manager
# ════════════════════════════════════════════════

class ConnectionManager:
    def __init__(self):
        self.price_conn: list[WebSocket] = []
        self.alert_conn: list[WebSocket] = []

    async def connect_prices(self, ws: WebSocket):
        await ws.accept()
        self.price_conn.append(ws)
        print(f"[WS] Price client connected ({len(self.price_conn)} total)")

    async def connect_alerts(self, ws: WebSocket):
        await ws.accept()
        self.alert_conn.append(ws)
        print(f"[WS] Alert client connected ({len(self.alert_conn)} total)")

    def disconnect_prices(self, ws: WebSocket):
        if ws in self.price_conn:
            self.price_conn.remove(ws)
        print(f"[WS] Price client disconnected ({len(self.price_conn)} total)")

    def disconnect_alerts(self, ws: WebSocket):
        if ws in self.alert_conn:
            self.alert_conn.remove(ws)
        print(f"[WS] Alert client disconnected ({len(self.alert_conn)} total)")

    async def broadcast_price(self, data: dict):
        dead = []
        for ws in self.price_conn:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect_prices(ws)

    async def broadcast_alert(self, data: dict):
        dead = []
        for ws in self.alert_conn:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect_alerts(ws)


manager = ConnectionManager()
event_loop = None  # Will be set in lifespan


# ════════════════════════════════════════════════
# Kafka Background Consumers
# ════════════════════════════════════════════════

def kafka_trades_consumer():
    """Background thread: consume raw trades from Kafka → broadcast price via WebSocket"""
    import time
    print("🔄 Starting Kafka Trades Consumer...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_TRADES,
                bootstrap_servers=[KAFKA_BROKER_INTERNAL],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='api-trades-group',
                auto_offset_reset='latest'
            )
            print("✅ Kafka [Trades] connected for API")
            break
        except Exception as e:
            print(f"⏳ Waiting for Kafka trades... {e}")
            time.sleep(3)

    for message in consumer:
        try:
            data = message.value
            symbol = data.get('s', '')
            price = float(data.get('p', 0))

            # Cache to Redis
            cache_price(symbol, price)

            # Broadcast via WebSocket
            ws_data = {
                "type": "price_update",
                "symbol": symbol,
                "price": price
            }
            if event_loop and not event_loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    manager.broadcast_price(ws_data), event_loop
                )
        except Exception as e:
            pass  # Silently skip bad messages


def kafka_alerts_consumer():
    """Background thread: consume whale alerts from Kafka → broadcast via WebSocket"""
    import time
    print("🔄 Starting Kafka Alerts Consumer...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_ALERTS,
                bootstrap_servers=[KAFKA_BROKER_INTERNAL],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='api-alerts-group',
                auto_offset_reset='latest'
            )
            print("✅ Kafka [Alerts] connected for API")
            break
        except Exception as e:
            print(f"⏳ Waiting for Kafka alerts... {e}")
            time.sleep(3)

    for message in consumer:
        try:
            data = message.value
            ws_data = {
                "type": "whale_alert",
                "symbol": data.get('Symbol', ''),
                "total_usd_value": float(data.get('total_usd_value', 0)),
                "window_start": data.get('window_start', ''),
                "window_end": data.get('window_end', ''),
                "alert_message": data.get('alert_message', '')
            }
            if event_loop and not event_loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    manager.broadcast_alert(ws_data), event_loop
                )
        except Exception as e:
            print(f"[Kafka Alert] Error: {e}")


# ════════════════════════════════════════════════
# FastAPI App
# ════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    global event_loop
    event_loop = asyncio.get_event_loop()

    # Start Kafka consumers in background threads
    t_trades = threading.Thread(target=kafka_trades_consumer, daemon=True)
    t_alerts = threading.Thread(target=kafka_alerts_consumer, daemon=True)
    t_trades.start()
    t_alerts.start()
    print("🚀 API Server started with Kafka consumers")

    yield

    print("🛑 API Server shutting down")


app = FastAPI(
    title="Whale Watch API",
    description="Real-time Crypto Whale Alert Dashboard API",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Static Files & Pages ──
FRONTEND_DIR = Path(__file__).parent / "font-end" / "src"

# Mount static files for CSS, JS, etc.
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")
    print(f"📁 Frontend directory: {FRONTEND_DIR}")
else:
    print(f"⚠️ Frontend directory not found: {FRONTEND_DIR}")


@app.get("/")
async def root():
    """Root → Dashboard"""
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"message": "Whale Watch API", "docs": "/docs"}


@app.get("/dashboard")
async def dashboard_page():
    return FileResponse(str(FRONTEND_DIR / "index.html"))


from fastapi.responses import FileResponse, Response
import mimetypes

@app.get("/realtime")
async def realtime_page():
    path = str(FRONTEND_DIR / "realtime.html")
    content = Path(path).read_text(encoding="utf-8")
    return Response(content=content, media_type="text/html", headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
    })


# ════════════════════════════════════════════════
# REST API Endpoints
# ════════════════════════════════════════════════

# ── Alert APIs ──

@app.get("/api/alerts/stats")
async def api_alert_stats():
    """Thống kê alerts hôm nay"""
    return get_alert_stats()


@app.get("/api/alerts/history")
async def api_alert_history(
    limit: int = Query(20, ge=1, le=200),
    symbol: str = Query(None)
):
    """Lịch sử alerts từ PostgreSQL"""
    alerts = get_alert_history(limit=limit, symbol=symbol)
    return {"alerts": alerts}


@app.get("/api/alerts/recent")
async def api_recent_alerts(limit: int = Query(20, ge=1, le=100)):
    """Alerts mới nhất từ Redis (nhanh hơn)"""
    alerts = get_recent_alerts(limit=limit)
    return {"alerts": alerts}


# ── Config APIs ──

@app.get("/api/configs")
async def api_get_configs():
    """Lấy cấu hình ngưỡng cảnh báo"""
    configs = [
        {"symbol": symbol, "threshold_usd": threshold, "is_active": True}
        for symbol, threshold in DEFAULT_THRESHOLDS.items()
    ]
    return {"configs": configs}


@app.put("/api/configs/{symbol}")
async def api_update_config(symbol: str, threshold_usd: float = Query(...)):
    """Cập nhật ngưỡng cảnh báo (in-memory)"""
    symbol = symbol.upper()
    DEFAULT_THRESHOLDS[symbol] = threshold_usd
    return {"status": "ok", "symbol": symbol, "threshold_usd": threshold_usd}


# ── Market Data APIs ──

@app.get("/api/market/candles")
async def api_get_candles(
    symbol: str = Query("BTCUSDT"),
    timeframe: str = Query("1m"),
    limit: int = Query(100, ge=1, le=500)
):
    """Lấy nến OHLCV từ PostgreSQL"""
    db_timeframe = resolve_timeframe(timeframe)
    candles = get_candles(symbol=symbol.upper().strip(), timeframe=db_timeframe, limit=limit)
    return {"candles": candles}


@app.get("/api/market/candles/all")
async def api_get_all_candles(
    timeframe: str = Query("1m"),
    limit: int = Query(100, ge=1, le=500)
):
    """Lấy nến OHLCV cho TẤT CẢ coins tracked — dùng cho dashboard init"""
    db_timeframe = resolve_timeframe(timeframe)
    result = {}
    for symbol in DEFAULT_THRESHOLDS.keys():
        candles = get_candles(symbol=symbol.strip(), timeframe=db_timeframe, limit=limit)
        result[symbol] = candles
    return {"candles": result}


@app.get("/api/market/prices")
async def api_get_prices():
    """Giá realtime từ Redis"""
    prices = get_latest_prices()
    return {"prices": prices}


@app.get("/api/market/candle/latest")
async def api_latest_candle(
    symbol: str = Query("BTCUSDT"),
    timeframe: str = Query("1m")
):
    """Nến đang chạy hiện tại từ Redis"""
    db_timeframe = resolve_timeframe(timeframe)
    candle = get_latest_candle(symbol=symbol.upper().strip(), timeframe=db_timeframe)
    return {"candle": candle}


# ── Health Check ──

@app.get("/api/health")
async def health_check():
    """System health check"""
    return {
        "status": "ok",
        "kafka_trades_ws_clients": len(manager.price_conn),
        "kafka_alerts_ws_clients": len(manager.alert_conn),
        "tracked_coins": list(DEFAULT_THRESHOLDS.keys()),
    }


# ════════════════════════════════════════════════
# WebSocket Endpoints
# ════════════════════════════════════════════════

@app.websocket("/ws/prices")
async def ws_prices(websocket: WebSocket):
    await manager.connect_prices(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Keep-alive ping
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect_prices(websocket)


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    await manager.connect_alerts(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect_alerts(websocket)
        
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
