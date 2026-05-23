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

