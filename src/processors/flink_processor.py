from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
import os
import json

def process_events():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    kafka_jar = os.path.abspath("libs/flink-sql-connector-kafka-3.0.0-1.17.jar")
    if not os.path.exists(kafka_jar):
        raise FileNotFoundError(f"Kafka JAR not found at: {kafka_jar}")
        
    env.add_jars(f"file://{kafka_jar}")

    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-group',
        'auto.offset.reset': 'earliest'
    }

    kafka_consumer = env.add_source(
        FlinkKafkaConsumer(
            topics='nfl_data_scores',
            deserialization_schema=SimpleStringSchema(),
            properties=kafka_props
        )
    )

    kafka_consumer.print()
    env.execute("NFL Data Pipeline")

def parse_json(event: str):
    return json.loads(event)

if __name__ == "__main__":
    process_events()