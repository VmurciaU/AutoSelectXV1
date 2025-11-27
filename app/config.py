# app/config.py
import os
from pathlib import Path

# Detects if running locally or in Render
PROJECT_ROOT = os.getenv("PROJECT_ROOT")

if PROJECT_ROOT:
    PROJECT_ROOT = Path(PROJECT_ROOT)
else:
    # LOCAL path (your machine)
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Path where RAG stores graphs, VDB, JSON, PDFs, etc
RAG_STORAGE = PROJECT_ROOT / "raggrafo" / "rag_storage"

# Create directory if missing (important for Render)
RAG_STORAGE.mkdir(parents=True, exist_ok=True)
