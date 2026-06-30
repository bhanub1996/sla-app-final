import hashlib
import math
# sla-app-final/src/ecommerce_hypermarketing/sla/sla_vector_kb.py
import json
import os

class SLAVectorKB:
    def __init__(self, kb_path="/Workspace/Repos/bhanub1996/sla-app-final/src/ecommerce_hypermarketing/sla/sla_process_kb.json"):
        self.kb_path = kb_path
        self.kb_data = self._load_kb()

    def _load_kb(self):
        try:
            with open(self.kb_path, "r") as f:
                return json.load(f)
        except Exception:
            # Fallback mock database if path structure varies in runtime context
            return {
                "marketing_pipeline_delay": "Verify downstream cluster sizing in Unity Catalog. Check for network latency or unoptimized Delta Tables missing Z-Ordering.",
                "stock_movement_missing": "Check upstream streaming data ingestion rate. Rerun Bronze staging data-cleansing jobs.",
                "database_lock": "Isolate pipeline job write locks. Scale Databricks clusters or shift to micro-batching intervals."
            }

    def retrieve_context(self, anomaly_keywords: str) -> str:
        """
        Retrieves matching context blocks. For a deeper RAG implementation,
        replace this with an embedding similarity search against a Delta Vector Index.
        """
        matched_contexts = []
        for key, resolution in self.kb_data.items():
            if any(word in anomaly_keywords.lower() for word in key.split("_")):
                matched_contexts.append(f"[{key}]: {resolution}")
        
        if not matched_contexts:
            return "No specific runbook found. Default to checking cluster execution limits and Delta table optimization logs."
            
        return "\n".join(matched_contexts)

def text_to_vector(text, dim=32):
    vector = [0.0] * dim

    tokens = (
        text.lower()
        .replace(",", " ")
        .replace(".", " ")
        .replace("_", " ")
        .replace("-", " ")
        .split()
    )

    for token in tokens:
        digest = hashlib.md5(token.encode("utf-8")).hexdigest()
        idx = int(digest[:8], 16) % dim
        sign = 1 if int(digest[8:10], 16) % 2 == 0 else -1
        vector[idx] += sign * 1.0

    norm = math.sqrt(sum(v * v for v in vector))

    if norm == 0:
        return vector

    return [round(v / norm, 6) for v in vector]


def cosine_similarity(v1, v2):
    return sum(a * b for a, b in zip(v1, v2))


def vectorize_kb(kb_items):
    vectorized = []

    for item in kb_items:
        text_blob = " ".join([
            item["process"],
            item["resource"],
            " ".join(item["symptoms"]),
            " ".join(item["likely_root_causes"]),
            " ".join(item["mitigations"]),
            " ".join(item["resources_to_check"])
        ])

        row = dict(item)
        row["text_blob"] = text_blob
        row["embedding_vector"] = text_to_vector(text_blob)

        vectorized.append(row)

    return vectorized
