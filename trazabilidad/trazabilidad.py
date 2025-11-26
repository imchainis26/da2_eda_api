import os
import time
import pika
from psycopg2.pool import SimpleConnectionPool
import requests

PG_HOST = os.getenv("PGHOST", "postgres")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDATABASE", "citypass_logs")
PG_USER = os.getenv("PGUSER", "citypass")
PG_PASS = os.getenv("PGPASSWORD", "citypass")

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'eda-nlb-5a241fed574cc8e8.elb.us-east-1.amazonaws.com')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
RABBITMQ_MGMT_PORT = int(os.getenv('RABBITMQ_MGMT_PORT', '15672'))

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

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    virtual_host='/',
    credentials=credentials,
    heartbeat=600,
    blocked_connection_timeout=300,
    connection_attempts=3,
    retry_delay=2,
    socket_timeout=10,
    stack_timeout=10
)

def connect_rabbit():
    print(f"[Rabbit] Intentando conectar a {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    print(f"[Rabbit] Usuario: {RABBITMQ_USER}, VHost: /")
    while True:
        try:
            conn = pika.BlockingConnection(parameters)
            # Obtener info del nodo conectado
            server_props = conn._impl.server_properties
            node_name = server_props.get('cluster_name', 'unknown')
            print(f"[Rabbit] Conectado al cluster via NLB - Node info: {node_name}")
            return conn
        except Exception as e:
            import traceback
            print(f"[Rabbit] Error de conexión: {type(e).__name__}: {str(e)}")
            print(f"[Rabbit] Traceback: {traceback.format_exc()}")
            print("[Rabbit] Reintentando en 3 segundos...")
            time.sleep(3)

def get_consumer_info_from_tag(consumer_tag):
    """Consulta info del consumer via Management API a través del NLB"""
    try:
        response = requests.get(
            f'http://{RABBITMQ_HOST}:{RABBITMQ_MGMT_PORT}/api/consumers',
            auth=(RABBITMQ_USER, RABBITMQ_PASS),
            timeout=5
        )
        if response.status_code == 200:
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

    state = routing_key.split(".")[0] if routing_key else "unknown"
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

    trace_data["node"] = headers.get("node") or trace_props.get("node")

    try:
        save_event(trace_data)
        print(f"[OK] Evento guardado: state={state}, node={trace_data.get('node', 'N/A')}, exchange={trace_data.get('exchange_name', 'N/A')}")
    except Exception as e:
        print(f"[DB] Error guardando: {e}")


connection = connect_rabbit()
channel = connection.channel()

channel.basic_qos(prefetch_count=10)
channel.basic_consume(queue="trazabilidad", on_message_callback=callback, auto_ack=True)

print(f" [*] Esperando mensajes en 'trazabilidad' desde {RABBITMQ_HOST}")
print(" [*] Conectado via NLB al cluster RabbitMQ. Ctrl+C para salir")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("\n[*] Cerrando conexión...")
    channel.stop_consuming()
    connection.close()
    print("[*] Conexión cerrada")
