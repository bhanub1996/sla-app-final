import json
from pathlib import Path
from openai import OpenAI

def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_json(relative_path: str):
    return json.loads((project_root() / relative_path).read_text(encoding="utf-8"))


def fq(catalog: str, schema_prefix: str, schema_suffix: str, table_name: str) -> str:
    return f"`{catalog}`.`{schema_prefix}_{schema_suffix}`.`{table_name}`"


def ops_table(catalog: str, schema_prefix: str, table_name: str) -> str:
    return fq(catalog, schema_prefix, "ops", table_name)



def gold_table(catalog: str, schema_prefix: str, table_name: str) -> str:
    return fq(catalog, schema_prefix, "gold", table_name)


def load_batch_registry_metadata():
    return load_json("metadata/sla_batch_registry.json")

class SLAConfig:
    BASE_URL = "https://cadet-leotard-remission.ngrok-free.dev/v1"
    API_KEY = "ollama"
    MODEL_NAME = "tinyllama:latest"  # Change to qwen or your choice if updated in Ollama
    TEMPERATURE = 0.2
    
    @classmethod
    def get_llm_client(cls):
        return OpenAI(
            base_url=cls.BASE_URL,
            api_key=cls.API_KEY
        )

def load_kb_metadata():
    return load_json("metadata/sla_process_kb.json")
