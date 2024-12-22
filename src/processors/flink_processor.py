from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
import psycopg2
from psycopg2.extras import Json
import json
import os
from typing import Dict, Any

def get_db_connection():
    return psycopg2.connect(
        dbname="nfl_stats",
        user="admin",
        host=os.getenv("POSTGRES_PASSWORD"), 
        password=os.getenv("POSTGRES_USER"), 
        port="5432"
    )

def process_message(msg: str):
    try:
        conn = get_db_connection()
        data = json.loads(msg)
        table_name = "nfl_data"
        
        # Store raw JSON
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nfl_data (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data JSONB
                )
            """)
            cur.execute("INSERT INTO nfl_data (data) VALUES (%s)", (Json(data),))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error processing message: {e}")
        return False

def process_events():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    kafka_jar = os.path.abspath("libs/flink-sql-connector-kafka-3.0.0-1.17.jar")
    env.add_jars(f"file://{kafka_jar}")

    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics='nfl_data_scores',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    stream = env.add_source(consumer)
    stream.map(lambda x: process_message(x))
    env.execute("NFL Data Pipeline")

if __name__ == "__main__":
    process_events()