import json
from typing import Optional
#{
#         "e": "aggTrade",      Event type
#         "E": 1779091675833,   Event time
#         "s": "BTCUSDT",       Symbol
#         "a": 3959421941,      Aggregate trade ID
#         "p": "77155.70000000",Price
#         "q": "0.00074000",    Quantity
#         "f": 6303820575,      First trade ID
#         "l": 6303820575,      Last trade ID
#         "T": 1779091675832,   Trade time
#         "m": true,            Is the buyer the market maker?
#         "M": true,            Ignore
#     }
class OHLCV: 
    symbol : str
    timestamp : str
    timeframe : str
    open : float
    high : float
    low : float
    close : float
    volume : float
    buyer: bool
    cnt: int
    def __init__(self, symbol, timestamp, timeframe, open, high, low, close, volume, buyer, cnt):
        self.symbol = symbol
        self.timestamp=timestamp
        self.timeframe=timeframe
        self.open=open
        self.high=high
        self.low=low
        self.close=close
        self.volume=volume
        self.buyer=buyer
        self.cnt=cnt

    def to_dict(self):
        return {
            'symbol': self.symbol,
            'timestamp':self.timestamp,
            'timeframe':self.timeframe,
            'open':self.open,
            'high':self.high,
            'low':self.low,
            'close':self.close,
            'volume':self.volume,
            'buyer':self.buyer,
            'cnt':self.cnt
        }
    
def parse_ohlcv_alert(raw) -> Optional[OHLCV]:
    try:
        return OHLCV(
            symbol = raw['symbol'],
            timestamp=raw['timestamp'],
            timeframe= raw['timeframe'],
            open= raw['open'],
            high= raw['high'],
            low=raw['low'],
            close=raw['close'],
            volume=raw['volume'],
            buyer = raw['buyer'] > 0,
            cnt = raw['cnt']
        )
    except (KeyError, ValueError, TypeError) as e:
        print(f"[OPHLCV] Lỗi parse alert: {e}")
        return None
    
def format_ohlcv_alert(alert: 'OHLCV') -> str:
    price_change = ((float(alert.close) - float(alert.open)) / float(alert.open)) * 100    
    if price_change > 0:
        trend_icon, trend_text = "🟢", "Increase"
    elif price_change < 0:
        trend_icon, trend_text = "🔴", "Decrease"
    else:
        trend_icon, trend_text = "⚪", "Stable"

    delta_val = alert.buyer
    if delta_val == True:
        delta_msg = f"🟢 Buy `${abs(delta_val):,.0f}`"
    else:
        delta_msg = f"🔴 Sell `${abs(delta_val):,.0f}`"

    return (
        f"📊 *Candle {alert.timeframe.upper()}* 📊\n\n"
        f"{trend_icon} *#{alert.symbol}*\n"
        f"📈 *Open:* `${float(alert.open):,.2f}`\n"
        f"📉 *Close:* `${float(alert.close):,.2f}`\n"
        f"🚀 *Change:* `{price_change:+.2f}%` ({trend_text})\n"
        f"───────────────\n"
        f"⚖️ *Delta Volume:* {delta_msg}\n"
        f"⚡ *Count Trade:* `{int(alert.cnt):,}` trades"
    )
        