import json, requests, time

API = "http://localhost:8777"
HEAD = {"Content-Type": "application/json"}

with open("RAGGrafo/outputs/pc6_export/graph.jsonl", "r", encoding="utf-8") as f:
    lines = [json.loads(l) for l in f if l.strip()]

for obj in lines:
    if obj["type"] == "node":
        payload = {
            "entity_name": obj["name"],
            "entity_data": {
                "entity_type": obj.get("entity_type", "Concept"),
                "description": obj.get("description", "")
            }
        }
        requests.post(f"{API}/graph/entity/create", headers=HEAD, json=payload)
        time.sleep(0.05)

for obj in lines:
    if obj["type"] == "edge":
        payload = {
            "source_entity": obj["src"],
            "target_entity": obj["dst"],
            "relation_data": {
                "description": obj.get("relation", ""),
                "keywords": obj.get("keywords", ""),
                "weight": obj.get("weight", 1.0)
            }
        }
        requests.post(f"{API}/graph/relation/create", headers=HEAD, json=payload)
        time.sleep(0.05)

print("✅ Graph upload completed.")
