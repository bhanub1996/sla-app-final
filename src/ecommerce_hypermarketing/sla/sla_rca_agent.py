from ecommerce_hypermarketing.sla.sla_vector_kb import text_to_vector, cosine_similarity
# sla-app-final/src/ecommerce_hypermarketing/sla/sla_rca_agent.py
from .sla_config import SLAConfig

class SLARCAAgent:
    def __init__(self):
        self.client = SLAConfig.get_llm_client()
        self.model = SLAConfig.MODEL_NAME
        self.temperature = SLAConfig.TEMPERATURE

    def generate_rca(self, telemetry_data: dict, retrieved_context: str) -> str:
        system_prompt = (
            "You are an expert E-commerce Data Operations Agent specializing in Databricks Lakehouse architecture.\n"
            "Your objective is to evaluate current operational telemetry data against historical reference knowledge "
            "to formulate a structured Root Cause Analysis (RCA) and outline immediate, actionable mitigation steps."
        )
        
        user_prompt = f"""
        ### OPERATIONAL TELEMETRY ANOMALY:
        - Target Table: {telemetry_data.get('table_name', 'Unknown')}
        - Observed Ingestion Latency: {telemetry_data.get('latency_minutes', 0)} minutes
        - SLA Threshold Limit: {telemetry_data.get('sla_threshold', 0)} minutes
        - Status: {telemetry_data.get('status', 'CRITICAL')}
        
        ### RETRIEVED KNOWLEDGE BASE CONTEXT (RAG):
        {retrieved_context}
        
        ### INSTRUCTIONS:
        Based on the telemetry data and retrieved context provided above, generate a professional RCA including:
        1. **Root Cause Analysis**: Why this delay happened.
        2. **Impact Score**: A value from 1 to 10 evaluating impact on hyper-marketing push rankings.
        3. **Actionable Mitigation Plans**: Step-by-step commands or steps to recover the pipeline (e.g., table optimization, cluster changes).
        """

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=self.temperature
        )
        
        return response.choices[0].message.content

def retrieve_kb_matches(event_payload, vector_kb, top_k=3):
    query_parts = [
        event_payload["batch_id"],
        event_payload["risk_level"],
        " ".join(event_payload["processes_involved"])
    ]

    for degraded in event_payload.get("degraded_processes", []):
        query_parts.append(degraded.get("process", ""))
        query_parts.append(degraded.get("resource", ""))
        query_parts.append(degraded.get("message", ""))

    query_text = " ".join(query_parts)
    query_vector = text_to_vector(query_text)

    scored = []

    for kb in vector_kb:
        score = cosine_similarity(query_vector, kb["embedding_vector"])

        for process in event_payload["processes_involved"]:
            if process == kb["process"]:
                score += 0.25

        for degraded in event_payload.get("degraded_processes", []):
            if degraded.get("process") == kb["process"]:
                score += 0.40

        scored.append({
            "score": round(score, 4),
            "kb": kb
        })

    return sorted(scored, key=lambda x: x["score"], reverse=True)[:top_k]


def build_next_best_actions(event_payload, rca_items):
    actions = []

    if event_payload["risk_level"] in ["HIGH", "CRITICAL"]:
        actions.append("Notify batch owner and SLA operations channel immediately.")

    if event_payload["estimated_minutes_late"] > 0:
        actions.append("Prioritize this batch over non-critical downstream workloads.")

    for rca in rca_items:
        for mitigation in rca["recommended_mitigations"][:2]:
            actions.append(f"{rca['process']}: {mitigation}")

    unique = []
    seen = set()

    for action in actions:
        if action not in seen:
            unique.append(action)
            seen.add(action)

    return unique


def build_rca_response(event_payload, vector_kb):
    matches = retrieve_kb_matches(event_payload, vector_kb, top_k=3)

    rca_items = []

    for match in matches:
        kb = match["kb"]

        rca_items.append({
            "match_score": match["score"],
            "kb_id": kb["kb_id"],
            "process": kb["process"],
            "resource": kb["resource"],
            "likely_root_causes": kb["likely_root_causes"],
            "recommended_mitigations": kb["mitigations"],
            "resources_to_check": kb["resources_to_check"],
            "escalation_team": kb["escalation_team"]
        })

    return {
        "event_type": "SLA_MISMATCH_RCA_RESPONSE",
        "batch_id": event_payload["batch_id"],
        "risk_level": event_payload["risk_level"],
        "risk_score": event_payload["risk_score"],
        "estimated_minutes_late": event_payload["estimated_minutes_late"],
        "summary": (
            f"Batch {event_payload['batch_id']} is predicted to miss SLA by "
            f"{event_payload['estimated_minutes_late']} minutes. "
            f"Risk level is {event_payload['risk_level']}."
        ),
        "rca_items": rca_items,
        "next_best_actions": build_next_best_actions(event_payload, rca_items)
    }
