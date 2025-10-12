import os
import requests
import json
import time
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv(override=True)

GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
GNEWS_ENDPOINT = f'https://gnews.io/api/v4/search?q=crypto&lang=en&token={GNEWS_API_KEY}'

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NEWS", "news")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username=os.getenv("KAFKA_USERNAME"),
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
    ssl_cafile=os.getenv("SSL_CA_LOCATION"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_crypto_news():
    try:
        response = requests.get(GNEWS_ENDPOINT, timeout=10)
        if response.status_code == 200:
            articles = response.json().get('articles', [])
            return articles
        else:
            print(f"Erro ao buscar notícias: {response.status_code}")
            return []
    except Exception as e:
        print(f"Erro na requisição: {e}")
        return []

def send_news_to_kafka(articles):
    for article in articles:
        message = {
            "title": article.get("title"),
            "description": article.get("description"),
            "content": article.get("content"),
            "publishedAt": article.get("publishedAt"),
            "source": article.get("source", {}).get("name"),
            "url": article.get("url")
        }
        producer.send(KAFKA_TOPIC, message)
        print(f"Enviado: {message['title']}")

if __name__ == "__main__":
    while True:
        news = fetch_crypto_news()
        if news:
            send_news_to_kafka(news)
        else:
            print("Nenhuma notícia encontrada.")
        time.sleep(4 * 3600)
