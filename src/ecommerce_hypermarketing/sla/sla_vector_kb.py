import hashlib
import math


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
