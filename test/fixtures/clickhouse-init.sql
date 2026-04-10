-- Test schema for ClickHouse adapter integration tests

CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

CREATE TABLE IF NOT EXISTS events (
    id UInt64,
    event_name String,
    user_id UInt32,
    tenant_id String,
    properties String,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (tenant_id, timestamp, id)
COMMENT 'Event tracking table';

CREATE TABLE IF NOT EXISTS metrics (
    metric_name String,
    value Float64,
    tenant_id String,
    tags Array(String),
    timestamp DateTime
) ENGINE = MergeTree()
PRIMARY KEY (tenant_id, metric_name, timestamp)
ORDER BY (tenant_id, metric_name, timestamp)
COMMENT 'Metrics aggregation table';

-- Insert test data
INSERT INTO events (id, event_name, user_id, tenant_id, properties) VALUES
    (1, 'page_view', 101, 'tenant-1', '{"page": "/home"}'),
    (2, 'click', 101, 'tenant-1', '{"button": "signup"}'),
    (3, 'page_view', 102, 'tenant-2', '{"page": "/pricing"}');

INSERT INTO metrics (metric_name, value, tenant_id, tags, timestamp) VALUES
    ('cpu_usage', 45.5, 'tenant-1', ['server-1', 'production'], now() - INTERVAL 1 HOUR),
    ('memory_usage', 78.2, 'tenant-1', ['server-1', 'production'], now() - INTERVAL 30 MINUTE),
    ('cpu_usage', 23.1, 'tenant-2', ['server-2', 'staging'], now());
