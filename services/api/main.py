import sys
import os
import json
import asyncio
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from kafka import KafkaConsumer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.config import KAFKA_BROKER_EXTERNAL, TOPIC_TRADES, TOPIC_ALERTS
from utils.schema.ohlcv import parse_raw_trade
from utils.schema.whale import parse_whale_alert
from utils.db.alert_redis import cache_alert, cache_price, get_recent_alerts, get_latest_prices
from utils.db.alert_postgre import get_alert_history, get_alert_stats, get_configs, update_config, save_alert_history

class ConnectionManager:
    def __init__(self):
        self.price_conn: list[WebSocket] = []
        self.alert_conn: list[WebSocket] = []

    async def connect_prices(self, ws: WebSocket):


    async def connect_alerts(self, ws: WebSocket):


    def disconnect_prices(self, ws: WebSocket):


    def disconnect_alerts(self, ws: WebSocket):


    async def broadcast_price(self, data: dict):
        dead = []

    async def broadcast_alert(self, data: dict):
        dead = []
