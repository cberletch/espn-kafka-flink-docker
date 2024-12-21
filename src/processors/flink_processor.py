from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance()\
                    .in_streaming_mode()\
                    .build()
    
    t_env = StreamTableEnvironment.create(env, settings)

    t_env.get_config().get_configuration().set_string(
        "pipeline.jars", 
        "file:///home/cole/source/repos/streaming/libs/flink-connector-kafka-1.17.0.jar;"
        "file:///home/cole/source/repos/streaming/libs/kafka-clients-3.2.3.jar;"
        "file:///home/cole/source/repos/streaming/libs/flink-json-1.20.0.jar"
    )
    
    env.set_parallelism(1)
    
    create_kafka_source_tables(t_env)
    
    print_sink_ddl = """
        CREATE TABLE print_sink (
            events STRING
        ) WITH (
            'connector' = 'print'
        )
    """
    t_env.execute_sql(print_sink_ddl)
    
    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql("""
        INSERT INTO print_sink
        SELECT events FROM kafka_scores_raw
    """)
    
    statement_set.execute()

    print("\nQuerying the raw table:")
    t_env.execute_sql("""
        SELECT * FROM kafka_scores_raw
        LIMIT 5
    """).print()

def create_kafka_source_tables(t_env):
    scores_ddl = """
        CREATE TABLE kafka_scores_raw (
            events STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'nfl_data_scores',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flink_nfl_consumer',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'latest-offset'
        )
    """
    t_env.execute_sql(scores_ddl)
    
    print("\nRegistered Tables:")
    print(t_env.list_tables())

if __name__ == "__main__":
    main()
