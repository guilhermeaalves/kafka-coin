import os, json, datetime
import time
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv
load_dotenv(override=True)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
USERNAME  = os.getenv("KAFKA_USERNAME_BTC")
PASSWORD  = os.getenv("KAFKA_PASSWORD_BTC")
TOPIC     = os.getenv("KAFKA_TOPIC_BTC")
CA_PEM    = os.getenv("SSL_CA_LOCATION")

VS_CURRENCIES = "brl,usd,eur"  # moedas que vamos trazer da API

# API pública de cotações (CoinGecko)
API_URL = "https://api.coingecko.com/api/v3/simple/price"
PARAMS = {
    "ids": "bitcoin",
    "vs_currencies": VS_CURRENCIES,
    "include_24hr_change": "true"
}

def kafka_producer():
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": USERNAME,
        "sasl.password": PASSWORD,
        # Mantém verificação do hostname no certificado (boa prática)
        "ssl.endpoint.identification.algorithm": "https",
        "client.id": "btc-producer",
    }
    # Necessário quando o certificado do broker é assinado por uma CA não pública (caso das rotas internas)
    if CA_PEM:
        conf["ssl.ca.location"] = CA_PEM
    return Producer(conf)

def fetch_prices():
    r = requests.get(API_URL, params=PARAMS, timeout=15)
    r.raise_for_status()
    data = r.json()["bitcoin"]
    payload = {
        "symbol": "BTC",
        "currency": "BRL",
        "price": float(data.get("brl")),
        "price_usd": float(data.get("usd")),
        "price_eur": float(data.get("eur")),
        "price_change_pct_24h": float(data.get("brl_24h_change")),  # pode trocar p/ usd_24h_change se preferir
        "source": "coingecko",
        "ts": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    }
    return payload

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

if __name__ == "__main__":
    producer = kafka_producer()
    event = fetch_prices()
    key = f"{event['symbol']}-{event['ts']}"
    producer.produce(topic=TOPIC, key=key, value=json.dumps(event), callback=delivery_report)
    producer.flush(10)
    time.sleep(4 * 3600)
