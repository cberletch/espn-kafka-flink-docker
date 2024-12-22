from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import json
from datetime import datetime
import os

def create_timescale_sink():
    USER_NAME = os.getenv('POSTGRES_USER')
    PASSWORD = os.getenv('POSTGRES_PASSWORD')
    return f"""
        CREATE TABLE game_events_sink (
            event_time TIMESTAMP(3),
            game_id STRING,
            event_type STRING,
            team_id STRING,
            player_id STRING,
            score_home INT,
            score_away INT,
            quarter INT,
            time_remaining STRING,
            yard_line INT,
            down INT,
            yards_to_go INT,
            event_description STRING,
            metadata STRING,
            created_at TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://timescaledb:5432/nfl_stats',
            'table-name' = 'game_events',
            'username' = '{USER_NAME}',
            'password' = '{PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """

def process_events():
    
    # Set up the streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()
    
    t_env = StreamTableEnvironment.create(env, settings)

    # Add required JARs
    t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    "file:///libs/postgresql-42.2.27.jar;"
    "file:///libs/flink-connector-jdbc-3.1.0-1.17.jar;"
    "file:///libs/flink-sql-connector-kafka-3.0.0-1.17.jar;"
    "file:///libs/flink-json-1.17.0.jar"
    )

    # Add format jar
    t_env.get_config().get_configuration().set_string(
        "pipeline.auto-classpaths", "true"
    )

    # Create Kafka source table
    kafka_sources = """
    CREATE TABLE kafka_scores (
        payload STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'nfl_data_scores',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink-consumer-group',
        'format' = 'raw'
    );

    CREATE TABLE kafka_games (
        payload STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'nfl_data_games',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink-consumer-group',
        'format' = 'raw'
    );

    CREATE TABLE kafka_news (
        payload STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'nfl_data_news',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink-consumer-group',
        'format' = 'raw'
    );
    """

    # Update process_events to execute multiple source creations
    t_env.execute_sql(kafka_sources)

    t_env.execute_sql(create_timescale_sink())

    # Transform and insert data
    t_env.sql_query("""
        INSERT INTO game_events_sink
        SELECT
            CAST(JSON_VALUE(payload, '$.event_time') AS TIMESTAMP(3)) as event_time,
            JSON_VALUE(payload, '$.game_id') as game_id,
            JSON_VALUE(payload, '$.event_type') as event_type,
            JSON_VALUE(payload, '$.team_id') as team_id,
            JSON_VALUE(payload, '$.player_id') as player_id,
            CAST(JSON_VALUE(payload, '$.score_home') AS INT) as score_home,
            CAST(JSON_VALUE(payload, '$.score_away') AS INT) as score_away,
            CAST(JSON_VALUE(payload, '$.quarter') AS INT) as quarter,
            JSON_VALUE(payload, '$.time_remaining') as time_remaining,
            CAST(JSON_VALUE(payload, '$.yard_line') AS INT) as yard_line,
            CAST(JSON_VALUE(payload, '$.down') AS INT) as down,
            CAST(JSON_VALUE(payload, '$.yards_to_go') AS INT) as yards_to_go,
            JSON_VALUE(payload, '$.event_description') as event_description,
            payload as metadata,
            CURRENT_TIMESTAMP as created_at
        FROM kafka_events
    """).execute()

if __name__ == "__main__":
    process_events()