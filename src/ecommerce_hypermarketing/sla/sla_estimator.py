from datetime import datetime, timedelta
import math


def minutes_between(start, end):
    return (end - start).total_seconds() / 60.0


def percentile(values, pct):
    if not values:
        return None

    values_sorted = sorted(values)
    k = (len(values_sorted) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)

    if f == c:
        return values_sorted[int(k)]

    d0 = values_sorted[int(f)] * (c - k)
    d1 = values_sorted[int(c)] * (k - f)

    return d0 + d1


def compute_resource_delay(processes, process_health):
    total_delay = 0.0
    degraded_processes = []

    for process in processes:
        health = process_health.get(process, {})
        delay = float(health.get("delay_min", 0.0))
        status = health.get("status", "UNKNOWN")

        if status != "HEALTHY":
            degraded_processes.append({
                "process": process,
                "status": status,
                "delay_min": delay,
                "resource": health.get("resource"),
                "message": health.get("message")
            })

        total_delay += delay

    return total_delay, degraded_processes


def estimate_sla_risk(batch, historical_stats, process_health, current_time):
    batch_id = batch["batch_id"]

    data_available_time = batch["data_available_time"]
    actual_start_time = batch["actual_start_time"]
    sla_due_time = batch["sla_due_time"]

    stats = historical_stats.get(batch_id, {
        "avg_duration_min": 30.0,
        "p90_duration_min": 40.0,
        "p95_duration_min": 45.0,
        "historical_success_rate_pct": 95.0,
        "historical_sla_miss_count_30d": 0
    })

    elapsed_since_data_available = minutes_between(data_available_time, current_time)
    elapsed_since_start = minutes_between(actual_start_time, current_time)
    remaining_to_sla = minutes_between(current_time, sla_due_time)

    historical_p95 = float(stats["p95_duration_min"])
    historical_avg = float(stats["avg_duration_min"])

    resource_delay_min, degraded_processes = compute_resource_delay(
        batch["processes"],
        process_health
    )

    runtime_pressure = max(0.0, elapsed_since_start - historical_avg)

    estimated_total_runtime = historical_p95 + resource_delay_min + runtime_pressure

    estimated_completion_time = actual_start_time + timedelta(
        minutes=estimated_total_runtime
    )

    estimated_minutes_late = max(
        0.0,
        minutes_between(sla_due_time, estimated_completion_time)
    )

    risk_score = 0

    if estimated_completion_time > sla_due_time:
        risk_score += 45

    if remaining_to_sla < historical_p95:
        risk_score += 20

    if resource_delay_min >= 10:
        risk_score += 15

    if stats.get("historical_sla_miss_count_30d", 0) >= 3:
        risk_score += 10

    if batch.get("criticality") == "HIGH":
        risk_score += 10

    risk_score = min(100, risk_score)

    if risk_score >= 75:
        risk_level = "CRITICAL"
    elif risk_score >= 50:
        risk_level = "HIGH"
    elif risk_score >= 30:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    return {
        "batch_id": batch_id,
        "business_domain": batch["business_domain"],
        "criticality": batch["criticality"],
        "sla_due_time": sla_due_time,
        "data_available_time": data_available_time,
        "actual_start_time": actual_start_time,
        "current_time": current_time,
        "elapsed_since_data_available_min": round(elapsed_since_data_available, 2),
        "elapsed_since_start_min": round(elapsed_since_start, 2),
        "remaining_to_sla_min": round(remaining_to_sla, 2),
        "historical_avg_duration_min": historical_avg,
        "historical_p95_duration_min": historical_p95,
        "resource_delay_min": resource_delay_min,
        "estimated_total_runtime_min": round(estimated_total_runtime, 2),
        "estimated_completion_time": estimated_completion_time,
        "estimated_minutes_late": round(estimated_minutes_late, 2),
        "will_miss_sla": estimated_completion_time > sla_due_time,
        "risk_score": risk_score,
        "risk_level": risk_level,
        "degraded_processes": degraded_processes,
        "processes": batch["processes"],
        "historical_stats": stats
    }


def build_agent_event_payload(estimation):
    return {
        "event_type": "SLA_MISMATCH_PREDICTED",
        "event_time": estimation["current_time"].isoformat(),
        "batch_id": estimation["batch_id"],
        "business_domain": estimation["business_domain"],
        "criticality": estimation["criticality"],
        "risk_level": estimation["risk_level"],
        "risk_score": estimation["risk_score"],
        "will_miss_sla": estimation["will_miss_sla"],
        "sla_due_time": estimation["sla_due_time"].isoformat(),
        "estimated_completion_time": estimation["estimated_completion_time"].isoformat(),
        "estimated_minutes_late": estimation["estimated_minutes_late"],
        "data_availability": {
            "data_available_time": estimation["data_available_time"].isoformat(),
            "elapsed_since_data_available_min": estimation["elapsed_since_data_available_min"]
        },
        "runtime_stats": {
            "actual_start_time": estimation["actual_start_time"].isoformat(),
            "elapsed_since_start_min": estimation["elapsed_since_start_min"],
            "historical_avg_duration_min": estimation["historical_avg_duration_min"],
            "historical_p95_duration_min": estimation["historical_p95_duration_min"],
            "estimated_total_runtime_min": estimation["estimated_total_runtime_min"]
        },
        "historical_stats": estimation["historical_stats"],
        "processes_involved": estimation["processes"],
        "degraded_processes": estimation["degraded_processes"]
    }
