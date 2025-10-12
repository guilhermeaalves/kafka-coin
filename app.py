from dotenv import load_dotenv
load_dotenv(override=True)

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from confluent_kafka import Consumer, KafkaException
from collections import deque
import os
import threading
import time
from typing import List

app = FastAPI()

BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", "1000"))
messages = deque(maxlen=BUFFER_SIZE)

stop_event = threading.Event()
consumer = None
topic = os.getenv("KAFKA_TOPIC", "recommendations")

def _validate_env():
    required = ["KAFKA_BOOTSTRAP", "KAFKA_USERNAME", "KAFKA_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Variáveis ausentes: {', '.join(missing)}")

    ca_path = os.getenv("SSL_CA_LOCATION")
    if not ca_path:
        raise RuntimeError("Variável ausente: SSL_CA_LOCATION (caminho para o arquivo .pem da CA)")
    if not os.path.isfile(ca_path):
        raise RuntimeError(f"Arquivo SSL_CA_LOCATION não encontrado: {ca_path}")

def consume_loop():
    global consumer
    conf = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "orchestrate-consumer"),
        "auto.offset.reset": os.getenv("KAFKA_OFFSET_RESET", "latest"),

        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": os.getenv("KAFKA_SASL_MECH", "SCRAM-SHA-512"),
        "sasl.username": os.getenv("KAFKA_USERNAME"),
        "sasl.password": os.getenv("KAFKA_PASSWORD"),
        "ssl.ca.location": os.getenv("SSL_CA_LOCATION"),
        "ssl.endpoint.identification.algorithm": "https",

    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while not stop_event.is_set():
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"[Kafka] Erro: {msg.error()}")
                    continue
                try:
                    value = msg.value().decode("utf-8")
                except UnicodeDecodeError:
                    value = msg.value().hex()
                messages.append(value)
            except KafkaException as e:
                print(f"[Kafka] KafkaException: {e}")
                time.sleep(2)
            except Exception as e:
                print(f"[Kafka] Erro inesperado: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass

@app.on_event("startup")
def on_startup():
    _validate_env()
    threading.Thread(target=consume_loop, daemon=True).start()

@app.on_event("shutdown")
def on_shutdown():
    stop_event.set()

@app.get("/events")
def get_events() -> dict:
    return {"events": list(messages)}

## def get_events() -> dict:
##    out: List[str] = []
##    while True:
##        try:
##            out.append(messages.popleft())
##        except IndexError:
##            break
##    return {"events": out}
