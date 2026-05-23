from typing import Optional

class WhaleAlertSchema:
    symbol: str             # "BTCUSDT"
    total_usd: float  # 523000.50
    window_start: str       # "2024-01-15 10:30:00"
    window_end: str         # "2024-01-15 10:30:10"
    alert_message: str      # "Spark find out Whale"

    def __init__(self, symbol, total_usd, window_start, window_end, alert_message):
        self.symbol=symbol
        self.total_usd=total_usd
        self.window_start=window_start
        self.window_end=window_end
        self.alert_message=alert_message
    
    def to_dict(self):
        return {
            'symbol':self.symbol,
            'total_usd':self.total_usd,
            'window_start':self.window_start,
            "window_end": self.window_end,
            "alert_message": self.alert_message,
        }
    
# alert_kafka = whale_df.select(
#         col("Symbol").alias("key"),
#         to_json(struct(
#             col("window.start").cast(StringType()).alias("window_start"),
#             col("window.end").cast(StringType()).alias("window_end"),
#             col("Symbol"),
#             col("total_usd_value"),
#             lit("Spark Streaming find out WHALE").alias("alert_message")
#         )).alias("value")
#     )
def parse_whale_alert(raw) -> Optional[WhaleAlertSchema]:
    try:
        return WhaleAlertSchema(
            symbol=raw['Symbol'],
            total_usd=raw['total_usd_value'],
            window_start=raw['window_start'],
            window_end=raw['window_end'],
            alert_message=raw['alert_message']
        )
    except (KeyError, ValueError, TypeError) as e:
        print(f"[WhaleAlertSchema] Lỗi parse alert: {e}")
        return None

def format_whale_alert(alert: WhaleAlertSchema) -> str:
    if alert.total_usd >= 1_000_000:
        level_icon = "🚨🚨🚨" 
        alert_title = "MASSIVE WHALE DETECTED"
    else:
        level_icon = "⚠️"     
        alert_title = "WHALE ACTIVITY DETECTED"

    return (
        f"{level_icon} *{alert_title}* {level_icon}\n\n"
        f"💎 *Coin:* #{alert.symbol}\n"
        f"💵 *Value Transaction:* `${alert.total_usd:,.2f}`\n"
        f"⏱️ *Window:* 10 Giây\n"
        f"🕒 *At time:* `{alert.window_start}`\n"
        f"───────────────\n"
        f"🐳 _Powered by Spark Structured Streaming_"
    )