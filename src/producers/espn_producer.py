import requests
import json
from kafka import KafkaProducer
from time import sleep

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def fetch_nfl_data():
    base_url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl"
    endpoints = {
        "teams": f"{base_url}/teams",
        "scores": f"{base_url}/scoreboard",
        "news": f"{base_url}/news"
    }
    
    data = {}
    for key, url in endpoints.items():
        try:
            response = requests.get(url)
            response.raise_for_status()
            data[key] = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {key}: {e}")
            data[key] = None
    return data

def main():
    producer = create_kafka_producer()
    topic = "nfl_data"
    
    while True:
        try:
            data = fetch_nfl_data()
            for data_type, payload in data.items():
                if payload:
                    producer.send(f"{topic}_{data_type}", value=payload)
                    print(f"Published {data_type} data to {topic}_{data_type}")
            sleep(300)  # Wait 5 minutes between updates
            
        except Exception as e:
            print(f"Error in main loop: {e}")
            sleep(60)  # Wait a minute before retrying on error

if __name__ == "__main__":
    main()
