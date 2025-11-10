# app/schemas/document_schema.py
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class DocumentCreate(BaseModel):
    filename: str
    original_path: str
    stored_path: str
    mime_type: Optional[str] = None
    size_bytes: Optional[int] = None
    pages: Optional[int] = None
    status: str = "queued"
    notes: Optional[str] = None
    user_id: Optional[int] = None   # quien sube

class DocumentOut(BaseModel):
    id: int
    case_id: int
    user_id: Optional[int] = None
    filename: str
    original_path: str
    stored_path: str
    mime_type: Optional[str] = None
    size_bytes: Optional[int] = None
    pages: Optional[int] = None
    status: str
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
