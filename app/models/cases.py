# app/models/cases.py
from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database.conection import Base

class Case(Base):
    __tablename__ = "cases"

    id = Column(Integer, primary_key=True, index=True)  # consecutivo del caso
    user_id = Column(Integer, ForeignKey("users.id", ondelete="RESTRICT"), nullable=False, index=True)
    customer_id = Column(Integer, nullable=True, index=True)  # futuro FK a clientes

    # Datos principales del caso
    name = Column(String(200), nullable=True)  # título o nombre del caso
    status = Column(String(20), nullable=False, default="queued")  # queued|indexing|done|error|quoted|archived

    # Rutas usadas por tu pipeline PC1–PC6 / LightRAG (MVP)
    input_dir = Column(String(500), nullable=False)   # p.ej. shared_data/inbox/<case_id>/original/
    index_dir = Column(String(500), nullable=False)   # p.ej. shared_data/index/<case_id>/

    # Métricas simples para control
    rag_version = Column(String(50), nullable=True)   # p.ej. "pc1-6@2025.11.09"
    doc_count = Column(Integer, nullable=False, default=0)

    # Notas libres
    notes = Column(Text, nullable=True)

    # Auditoría mínima
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relaciones básicas
    user = relationship("User", backref="cases", lazy="joined")

    # Índices útiles (consultas por usuario/estado)
    __table_args__ = (
        Index("ix_cases_user_status", "user_id", "status"),
    )

    def touch(self):
        self.updated_at = datetime.utcnow()
