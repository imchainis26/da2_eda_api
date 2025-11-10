import os
import time
import pika
from psycopg2.pool import SimpleConnectionPool
import requests

PG_HOST = os.getenv("PGHOST", "172.31.19.186")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDATABASE", "citypass_logs")
PG_USER = os.getenv("PGUSER", "citypass")
PG_PASS = os.getenv("PGPASSWORD", "citypass")

pg_pool = None
while not pg_pool:
    try:
        pg_pool = SimpleConnectionPool(
            1, 10,
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASS,
            connect_timeout=5
        )
        print("[PG] Pool creado.")
    except Exception as e:
        print(f"[PG] Error creando pool: {e}. Reintentando...")
        time.sleep(3)


def init_db():
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS logs (
              id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              event_ts TIMESTAMPTZ NOT NULL,
              "user" TEXT,
              state TEXT NOT NULL,
              routing_keys TEXT[] NOT NULL,
              publisher TEXT,
              subscriber TEXT,
              exchange_name TEXT,
              node TEXT
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_event_ts ON logs (event_ts DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_state ON logs (state);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_logs_routing_keys ON logs USING GIN (routing_keys);")
        conn.commit()
        print("[PG] Esquema listo")
    except Exception as e:
        print("[PG] Error inicializando tabla:", e)
    finally:
        pg_pool.putconn(conn)


def save_event(trace_data):
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (
                  event_ts, "user", exchange_name, routing_keys,
                  publisher, subscriber, node, state
                )
                VALUES (
                  to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s
                );
            """, (
                trace_data.get("timestamp"),
                trace_data.get("user"),
                trace_data.get("exchange_name"),
                trace_data.get("routing_keys") or [],
                trace_data.get("publisher"),
                trace_data.get("subscriber") or trace_data.get("suscriber"),
                trace_data.get("node"),
                trace_data.get("state"),
            ))
        conn.commit()
    except Exception as e:
        print("[DB] Error guardando evento:", e)
    finally:
        pg_pool.putconn(conn)


init_db()

credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters(
    host=os.getenv('RABBITMQ_HOST', '54.221.111.195'),
    port=5672,
    virtual_host='/',
    credentials=credentials,
    heartbeat=30,
    blocked_connection_timeout=300
)

def connect_rabbit():
    while True:
        try:
            conn = pika.BlockingConnection(parameters)
            print("[Rabbit] Conectado")
            return conn
        except Exception as e:
            print(f"[Rabbit] No disponible aún: {e}. Reintentando...")
            time.sleep(3)

connection = connect_rabbit()
channel = connection.channel()

def get_consumer_info_from_tag(consumer_tag):
    import requests
    try:
        response = requests.get(
            'http://54.221.111.195:15672/api/consumers',
            auth=('guest', 'guest'),
            timeout=2
        )
        for consumer in response.json():
            if consumer.get('consumer_tag') == consumer_tag:
                return {
                    'queue': consumer['queue']['name'],
                    'connection_name': consumer.get('channel_details', {}).get('connection_name'),
                    'node': consumer.get('node')
                }
    except Exception as e:
        print(f"[API] Error consultando consumer: {e}")
    return None

def callback(ch, method, properties, body):
    trace_data = {}
    routing_key = getattr(method, "routing_key", "")
    parts = routing_key.split(".")

    state = getattr(method, "routing_key", None).split(".")[0]
    trace_data["state"] = state

    headers = getattr(properties, "headers", {}) if properties else {}
    trace_props = headers.get("properties", {})

    trace_data["timestamp"] = trace_props.get("timestamp")
    trace_data["id"] = trace_props.get("message_id")
    trace_data["user"] = headers.get("user")
    trace_data["exchange_name"] = headers.get("exchange_name")
    trace_data["routing_keys"] = headers.get("routing_keys", [])
    trace_data["publisher"] = trace_data["routing_keys"][0].split(".")[0] if trace_data["routing_keys"] else None

    if state == "deliver" and len(parts) > 1:
        destination = parts[1]
        trace_data["subscriber"] = [destination]

    if not trace_data.get("subscriber"):
        trace_data["subscriber"] = headers.get("routed_queues") or ["(desconocido)"]

    trace_data["node"] = trace_data.get("node") or headers.get("node")

    try:
        save_event(trace_data)
        print("[OK] Evento guardado:", trace_data)
    except Exception as e:
        print("[DB] Error guardando:", e)


channel.basic_consume(queue="trazabilidad", on_message_callback=callback, auto_ack=True)

print(" [*] Esperando mensajes en trazabilidad. Ctrl+C para salir")
channel.start_consuming()
