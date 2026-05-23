import psycopg2
from utils.config import POSTGRES_HOST_EXTERNAL, POSTGRES_HOST_INTERNAL, POSTGRES_PORT_EXTERNAL, POSTGRES_PORT_INTERNAL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

def get_pg_external_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        host=POSTGRES_HOST_EXTERNAL,
        user=POSTGRES_USER,
        port=POSTGRES_PORT_EXTERNAL,
        password=POSTGRES_PASSWORD
    )

def get_pg_internal_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        host=POSTGRES_HOST_INTERNAL,
        user=POSTGRES_USER,
        port=POSTGRES_PORT_INTERNAL,
        password=POSTGRES_PASSWORD
    )

# for notifier
def save_alert_history(symbol: str, total_usd: float, price: float = None, 
                       volume: float = None, is_buyer_maker: bool = None):
    conn=None
    try:
        conn = get_pg_internal_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO alert_history (symbol, price, volume, total_usd, is_buyer_maker)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (symbol, price, volume, total_usd, is_buyer_maker)
        )
        conn.commit()
        print(f'[Postgre] insert data {symbol} ${total_usd:,.2f}')
    except Exception as e:
        print(f'[Postgre] fail in save data - {e}')
    finally:
        if conn:
            conn.close()


# ── API Query Functions ──

def get_alert_history(limit=20, symbol=None):
    """Lấy lịch sử alerts từ PostgreSQL"""
    conn = None
    try:
        conn = get_pg_internal_connection()
        cur = conn.cursor()
        if symbol:
            cur.execute(
                """SELECT symbol, total_usd, alert_time 
                   FROM alert_history 
                   WHERE symbol = %s
                   ORDER BY alert_time DESC LIMIT %s""",
                (symbol, limit)
            )
        else:
            cur.execute(
                """SELECT symbol, total_usd, alert_time 
                   FROM alert_history 
                   ORDER BY alert_time DESC LIMIT %s""",
                (limit,)
            )
        rows = cur.fetchall()
        return [
            {"symbol": r[0], "total_usd": float(r[1]) if r[1] else 0, "alert_time": r[2].isoformat() if r[2] else None}
            for r in rows
        ]
    except Exception as e:
        print(f'[Postgre] get_alert_history error: {e}')
        return []
    finally:
        if conn:
            conn.close()


def get_alert_stats():
    """Thống kê alerts: tổng số, lệnh lớn nhất, coin hot nhất, tổng volume"""
    conn = None
    try:
        conn = get_pg_internal_connection()
        cur = conn.cursor()
        
        # Tổng alerts hôm nay
        cur.execute("SELECT COUNT(*) FROM alert_history WHERE alert_time >= CURRENT_DATE")
        total_alerts = cur.fetchone()[0] or 0
        
        # Lệnh lớn nhất hôm nay
        cur.execute("SELECT MAX(total_usd) FROM alert_history WHERE alert_time >= CURRENT_DATE")
        biggest = cur.fetchone()[0]
        biggest_whale = float(biggest) if biggest else 0
        
        # Coin hot nhất hôm nay (nhiều alerts nhất)
        cur.execute(
            """SELECT symbol, COUNT(*) as cnt 
               FROM alert_history 
               WHERE alert_time >= CURRENT_DATE
               GROUP BY symbol ORDER BY cnt DESC LIMIT 1"""
        )
        row = cur.fetchone()
        most_active = row[0] if row else "N/A"
        
        # Tổng volume
        cur.execute("SELECT COALESCE(SUM(total_usd), 0) FROM alert_history WHERE alert_time >= CURRENT_DATE")
        total_volume = float(cur.fetchone()[0] or 0)
        
        return {
            "total_alerts": total_alerts,
            "biggest_whale": biggest_whale,
            "most_active_coin": most_active,
            "total_volume": total_volume
        }
    except Exception as e:
        print(f'[Postgre] get_alert_stats error: {e}')
        return {"total_alerts": 0, "biggest_whale": 0, "most_active_coin": "N/A", "total_volume": 0}
    finally:
        if conn:
            conn.close()

