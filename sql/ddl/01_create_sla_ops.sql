CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_prefix}_ops;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_prefix}_ops.sla_batch_registry (
    batch_id STRING,
    business_domain STRING,
    criticality STRING,
    expected_data_available_minute INT,
    sla_offset_minutes INT,
    owner_team STRING,
    escalation_team STRING,
    active_flag BOOLEAN,
    processes ARRAY<STRING>,
    created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_prefix}_ops.sla_runtime_events (
    event_id STRING,
    batch_id STRING,
    event_time TIMESTAMP,
    data_available_time TIMESTAMP,
    actual_start_time TIMESTAMP,
    current_status STRING,
    current_process STRING,
    current_resource STRING,
    process_status STRING,
    process_delay_min DOUBLE,
    message STRING,
    created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_prefix}_ops.sla_prediction_results (
    prediction_id STRING,
    batch_id STRING,
    prediction_time TIMESTAMP,
    business_domain STRING,
    criticality STRING,
    risk_level STRING,
    risk_score INT,
    will_miss_sla BOOLEAN,
    sla_due_time TIMESTAMP,
    data_available_time TIMESTAMP,
    actual_start_time TIMESTAMP,
    estimated_completion_time TIMESTAMP,
    estimated_minutes_late DOUBLE,
    elapsed_since_data_available_min DOUBLE,
    elapsed_since_start_min DOUBLE,
    historical_avg_duration_min DOUBLE,
    historical_p95_duration_min DOUBLE,
    resource_delay_min DOUBLE,
    degraded_processes_json STRING,
    event_payload_json STRING,
    created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_prefix}_ops.sla_rca_kb_vectorized (
    kb_id STRING,
    process STRING,
    resource STRING,
    symptoms ARRAY<STRING>,
    likely_root_causes ARRAY<STRING>,
    mitigations ARRAY<STRING>,
    resources_to_check ARRAY<STRING>,
    escalation_team STRING,
    text_blob STRING,
    embedding_vector ARRAY<DOUBLE>,
    created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_prefix}_ops.sla_agent_recommendations (
    recommendation_id STRING,
    batch_id STRING,
    event_time TIMESTAMP,
    risk_level STRING,
    risk_score INT,
    rca_summary STRING,
    rca_items_json STRING,
    next_best_actions ARRAY<STRING>,
    created_at TIMESTAMP
)
USING DELTA;
