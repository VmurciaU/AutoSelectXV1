# app/schemas/case_schema.py
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class CaseCreate(BaseModel):
    name: Optional[str] = Field(default=None, max_length=200)
    status: str = "queued"
    input_dir: str
    index_dir: str
    rag_version: Optional[str] = None
    notes: Optional[str] = None
    customer_id: Optional[int] = None

class CaseOut(BaseModel):
    id: int
    user_id: int
    customer_id: Optional[int] = None
    name: Optional[str] = None
    status: str
    input_dir: str
    index_dir: str
    rag_version: Optional[str] = None
    doc_count: int
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
