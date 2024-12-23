-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create tables for NFL stats
CREATE TABLE IF NOT EXISTS game_events (
    event_time TIMESTAMPTZ NOT NULL,
    game_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    team_id VARCHAR(50),
    player_id VARCHAR(50),
    score_home INT,
    score_away INT,
    quarter INT,
    time_remaining VARCHAR(20),
    yard_line INT,
    down INT,
    yards_to_go INT,
    event_description TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Convert to hypertable
SELECT create_hypertable(game_events, event_time);

-- Create indexes for common queries
CREATE INDEX idx_game_events_game_id ON game_events (game_id);
CREATE INDEX idx_game_events_team_id ON game_events (team_id);
CREATE INDEX idx_game_events_player_id ON game_events (player_id);

-- Create materialized view for game summaries
CREATE MATERIALIZED VIEW game_summaries
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(1 hour, event_time) AS bucket,
    game_id,
    COUNT(*) as event_count,
    MAX(score_home) as final_score_home,
    MAX(score_away) as final_score_away,
    COUNT(DISTINCT player_id) as players_involved
FROM game_events
GROUP BY bucket, game_id
WITH NO DATA;

-- Refresh policy for materialized view
SELECT add_continuous_aggregate_policy(game_summaries,
    start_offset => INTERVAL 1 hour,
    end_offset => INTERVAL 1 minute,
    schedule_interval => INTERVAL 1 hour);

-- Create compression policy
SELECT add_compression_policy(game_events, INTERVAL 7 days);

-- Create retention policy (adjust retention period as needed)
SELECT add_retention_policy(game_events, INTERVAL 1 year);

-- Grants
GRANT SELECT, INSERT ON game_events TO admin;
GRANT SELECT ON game_summaries TO admin;

# init.sql
ALTER SYSTEM SET password_encryption = 'scram-sha-256';

CREATE USER airflow_user WITH PASSWORD 'local_password';
GRANT ALL PRIVILEGES ON DATABASE nfl_stats TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
