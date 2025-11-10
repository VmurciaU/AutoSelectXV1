# app/models/documents.py
from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database.conection import Base

class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, index=True)

    # Relaciones
    case_id = Column(Integer, ForeignKey("cases.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    # Datos básicos del PDF
    filename = Column(String(255), nullable=False)
    original_path = Column(String(600), nullable=False)
    stored_path = Column(String(600), nullable=False)
    mime_type = Column(String(80), nullable=True)
    size_bytes = Column(Integer, nullable=True)
    pages = Column(Integer, nullable=True)

    # Estado del procesamiento
    status = Column(String(20), nullable=False, default="queued")  # queued|cleaned|indexed|error

    # Notas o comentarios
    notes = Column(Text, nullable=True)

    # Auditoría
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relaciones ORM
    case = relationship("Case", backref="documents", lazy="joined")
    user = relationship("User", backref="documents", lazy="joined")

    __table_args__ = (
        Index("ix_documents_case_status", "case_id", "status"),
    )

    def touch(self):
        self.updated_at = datetime.utcnow()
