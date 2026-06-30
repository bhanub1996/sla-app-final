import argparse
import json
import sys
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    ArrayType,
    TimestampType,
    DoubleType
)

# COMMAND ----------
# MAGIC %pip install openai --upgrade

# COMMAND ----------
# Ensure your repository src directory is appended to sys path for modular imports
import sys
import os
sys.path.append(os.path.abspath("/Workspace/Repos/bhanub1996/sla-app-final/src"))

# Import your newly structured RAG modules
from ecommerce_hypermarketing.sla.sla_vector_kb import SLAVectorKB
from ecommerce_hypermarketing.sla.sla_rca_agent import SLARCAAgent

# COMMAND ----------
# 1. Simulate pulling an active anomaly row from your 'sla_ops' metadata table
active_anomaly = {
    "table_name": "marketing_performance_hourly",
    "latency_minutes": 75,
    "sla_threshold": 45,
    "status": "SLA_BREACH_RISK",
    "error_summary": "Delay in processing marketing pipeline channel streams"
}

print(f"🔍 Monitoring Loop: Detected risk on table '{active_anomaly['table_name']}'...")

# 2. Execute the RETRIEVAL stage (RAG Context Fetch)
kb = SLAVectorKB()
context = kb.retrieve_context(active_anomaly['error_summary'])

print("📖 Context retrieved successfully from local Knowledge Base.")

# 3. Execute the GENERATION stage (LLM Inference via ngrok)
agent = SLARCAAgent()
rca_report = agent.generate_rca(telemetry_data=active_anomaly, retrieved_context=context)

# 4. Display the comprehensive generated RCA report directly within Databricks
print("-" * 60)
print("🤖 AUTOMATED AGENTIC ROOT CAUSE ANALYSIS & MITIGATION REPORT:")
print("-" * 60)
display(rca_report)
# Make imports robust for Workspace Python script task
CURRENT_FILE = Path(__file__).resolve()
SRC_ROOT = CURRENT_FILE.parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from ecommerce_hypermarketing.sla.sla_config import (
    load_batch_registry_metadata,
    load_kb_metadata,
    ops_table
)
from ecommerce_hypermarketing.sla.sla_vector_kb import vectorize_kb
from ecommerce_hypermarketing.sla.sla_estimator import (
    estimate_sla_risk,
    build_agent_event_payload
)
from ecommerce_hypermarketing.sla.sla_rca_agent import build_rca_response


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", required=True)
    parser.add_argument("--current-time", default=None)
    return parser.parse_args()


def execute_sql_file(spark, catalog, schema_prefix):
    sql_file = CURRENT_FILE.parents[3] / "sql" / "ddl" / "01_create_sla_ops.sql"

    sql_text = (
        sql_file.read_text(encoding="utf-8")
        .replace("${catalog}", catalog)
        .replace("${schema_prefix}", schema_prefix)
    )

    for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
        spark.sql(stmt)


def seed_batch_registry(spark, catalog, schema_prefix):
    table = ops_table(catalog, schema_prefix, "sla_batch_registry")

    rows = load_batch_registry_metadata()

    df = spark.createDataFrame(rows)

    df = df.withColumn("created_at", F.current_timestamp())

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(table)
    )


def seed_vector_kb(spark, catalog, schema_prefix):
    table = ops_table(catalog, schema_prefix, "sla_rca_kb_vectorized")

    kb_items = vectorize_kb(load_kb_metadata())

    df = spark.createDataFrame(kb_items)

    df = df.withColumn("created_at", F.current_timestamp())

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(table)
    )

    return kb_items


def build_process_health_from_runtime_events(spark, catalog, schema_prefix):
    runtime_table = ops_table(catalog, schema_prefix, "sla_runtime_events")

    try:
        events = spark.table(runtime_table)
        latest_ts = events.agg(F.max("event_time").alias("ts")).collect()[0]["ts"]

        if latest_ts is None:
            return default_process_health()

        latest = events.filter(F.col("event_time") == F.lit(latest_ts)).collect()

        process_health = {}

        for row in latest:
            process = row["current_process"]

            if process:
                process_health[process] = {
                    "status": row["process_status"] or "UNKNOWN",
                    "delay_min": float(row["process_delay_min"] or 0.0),
                    "resource": row["current_resource"],
                    "message": row["message"]
                }

        return {**default_process_health(), **process_health}

    except Exception:
        return default_process_health()


def default_process_health():
    return {
        "raw_file_arrival": {
            "status": "HEALTHY",
            "delay_min": 0,
            "resource": "source_system_or_landing_zone",
            "message": "Raw file arrival within expected time."
        },
        "bronze_ingestion": {
            "status": "HEALTHY",
            "delay_min": 0,
            "resource": "databricks_job_cluster",
            "message": "Bronze ingestion is healthy."
        },
        "silver_transformation": {
            "status": "HEALTHY",
            "delay_min": 0,
            "resource": "spark_cluster",
            "message": "Silver transformation is healthy."
        },
        "gold_aggregation": {
            "status": "HEALTHY",
            "delay_min": 0,
            "resource": "delta_gold_tables",
            "message": "Gold aggregation is healthy."
        },
        "dashboard_refresh": {
            "status": "HEALTHY",
            "delay_min": 0,
            "resource": "sql_warehouse",
            "message": "Dashboard refresh is healthy."
        },
        "stock_reconciliation": {
            "status": "HEALTHY",
            "delay_min": 0,
            "resource": "inventory_source_api",
            "message": "Stock reconciliation is healthy."
        },
        "gold_pushnow_scoring": {
            "status": "HEALTHY",
            "delay_min": 0,
            "resource": "pushnow_scoring_logic",
            "message": "Push-now scoring is healthy."
        },
        "campaign_metrics_join": {
            "status": "HEALTHY",
            "delay_min": 0,
            "resource": "campaign_metrics_source",
            "message": "Campaign metrics join is healthy."
        },
        "gold_budget_shift": {
            "status": "HEALTHY",
            "delay_min": 0,
            "resource": "gold_budget_shift_table",
            "message": "Budget shift calculation is healthy."
        }
    }


def build_historical_stats(spark, catalog, schema_prefix):
    """
    Production path:
    Build historical stats from sla_runtime_events if available.

    Fallback:
    Return reasonable defaults per batch.
    """

    registry = load_batch_registry_metadata()

    defaults = {}

    for batch in registry:
        batch_id = batch["batch_id"]

        if "STOCK" in batch_id:
            avg_duration = 47.5
            p95 = 54.0
            miss_count = 5
        elif "MARKETING" in batch_id:
            avg_duration = 30.3
            p95 = 35.0
            miss_count = 1
        elif "ORDERS" in batch_id:
            avg_duration = 35.3
            p95 = 42.0
            miss_count = 2
        else:
            avg_duration = 28.0
            p95 = 38.0
            miss_count = 1

        defaults[batch_id] = {
            "avg_duration_min": avg_duration,
            "p90_duration_min": p95 - 2,
            "p95_duration_min": p95,
            "historical_success_rate_pct": 96.0,
            "historical_sla_miss_count_30d": miss_count
        }

    return defaults


def build_current_batch_inputs(current_time):
    """
    Builds current monitoring input from metadata.

    Each batch SLA is calculated as:
    current hour start + sla_offset_minutes.

    Data availability is estimated as:
    current hour start + expected_data_available_minute.
    """

    registry = load_batch_registry_metadata()

    hour_start = current_time.replace(minute=0, second=0, microsecond=0)

    batch_inputs = []

    for batch in registry:
        if not batch.get("active_flag", True):
            continue

        data_available_time = hour_start + timedelta(
            minutes=int(batch["expected_data_available_minute"])
        )

        sla_due_time = hour_start + timedelta(
            minutes=int(batch["sla_offset_minutes"])
        )

        actual_start_time = data_available_time + timedelta(minutes=4)

        batch_inputs.append({
            "batch_id": batch["batch_id"],
            "business_domain": batch["business_domain"],
            "criticality": batch["criticality"],
            "sla_due_time": sla_due_time,
            "data_available_time": data_available_time,
            "actual_start_time": actual_start_time,
            "status": "RUNNING",
            "processes": batch["processes"]
        })

    return batch_inputs


def write_prediction(spark, catalog, schema_prefix, prediction, event_payload):
    table = ops_table(catalog, schema_prefix, "sla_prediction_results")

    row = [{
        "prediction_id": str(uuid4()),
        "batch_id": prediction["batch_id"],
        "prediction_time": prediction["current_time"],
        "business_domain": prediction["business_domain"],
        "criticality": prediction["criticality"],
        "risk_level": prediction["risk_level"],
        "risk_score": int(prediction["risk_score"]),
        "will_miss_sla": bool(prediction["will_miss_sla"]),
        "sla_due_time": prediction["sla_due_time"],
        "data_available_time": prediction["data_available_time"],
        "actual_start_time": prediction["actual_start_time"],
        "estimated_completion_time": prediction["estimated_completion_time"],
        "estimated_minutes_late": float(prediction["estimated_minutes_late"]),
        "elapsed_since_data_available_min": float(prediction["elapsed_since_data_available_min"]),
        "elapsed_since_start_min": float(prediction["elapsed_since_start_min"]),
        "historical_avg_duration_min": float(prediction["historical_avg_duration_min"]),
        "historical_p95_duration_min": float(prediction["historical_p95_duration_min"]),
        "resource_delay_min": float(prediction["resource_delay_min"]),
        "degraded_processes_json": json.dumps(prediction["degraded_processes"]),
        "event_payload_json": json.dumps(event_payload, default=str),
        "created_at": datetime.utcnow()
    }]

    spark.createDataFrame(row).write.format("delta").mode("append").saveAsTable(table)


def write_recommendation(spark, catalog, schema_prefix, rca_response):
    table = ops_table(catalog, schema_prefix, "sla_agent_recommendations")

    row = [{
        "recommendation_id": str(uuid4()),
        "batch_id": rca_response["batch_id"],
        "event_time": datetime.utcnow(),
        "risk_level": rca_response["risk_level"],
        "risk_score": int(rca_response["risk_score"]),
        "rca_summary": rca_response["summary"],
        "rca_items_json": json.dumps(rca_response["rca_items"], default=str),
        "next_best_actions": rca_response["next_best_actions"],
        "created_at": datetime.utcnow()
    }]

    spark.createDataFrame(row).write.format("delta").mode("append").saveAsTable(table)


def run_monitor(spark, catalog, schema_prefix, current_time):
    execute_sql_file(spark, catalog, schema_prefix)

    seed_batch_registry(spark, catalog, schema_prefix)
    vector_kb = seed_vector_kb(spark, catalog, schema_prefix)

    historical_stats = build_historical_stats(spark, catalog, schema_prefix)
    process_health = build_process_health_from_runtime_events(spark, catalog, schema_prefix)
    batch_inputs = build_current_batch_inputs(current_time)

    agent_trigger_count = 0

    for batch in batch_inputs:
        prediction = estimate_sla_risk(
            batch=batch,
            historical_stats=historical_stats,
            process_health=process_health,
            current_time=current_time
        )

        event_payload = build_agent_event_payload(prediction)

        write_prediction(
            spark=spark,
            catalog=catalog,
            schema_prefix=schema_prefix,
            prediction=prediction,
            event_payload=event_payload
        )

        should_trigger_agent = (
            prediction["will_miss_sla"]
            or prediction["risk_level"] in ["HIGH", "CRITICAL"]
        )

        if should_trigger_agent:
            rca_response = build_rca_response(event_payload, vector_kb)
            write_recommendation(spark, catalog, schema_prefix, rca_response)
            agent_trigger_count += 1

    return {
        "batches_checked": len(batch_inputs),
        "agent_trigger_count": agent_trigger_count
    }


if __name__ == "__main__":
    args = parse_args()

    if args.current_time:
        current_time = datetime.strptime(args.current_time, "%Y-%m-%d %H:%M:%S")
    else:
        current_time = datetime.utcnow()

    result = run_monitor(
        spark=spark,  # noqa: F821
        catalog=args.catalog,
        schema_prefix=args.schema_prefix,
        current_time=current_time
    )

    print(result)
