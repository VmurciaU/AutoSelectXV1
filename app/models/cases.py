# app/models/cases.py
from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database.conection import Base


class Case(Base):
    __tablename__ = "cases"

    id = Column(Integer, primary_key=True, index=True)  # consecutivo del caso
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    customer_id = Column(Integer, nullable=True, index=True)  # futuro FK a clientes

    # Datos principales del caso
    name = Column(String(200), nullable=True)
    status = Column(String(20), nullable=False, default="queued")

    # Rutas usadas por tu pipeline PC1–PC6 / LightRAG (MVP)
    input_dir = Column(String(500), nullable=False)
    index_dir = Column(String(500), nullable=False)

    # Métricas simples
    rag_version = Column(String(50), nullable=True)
    doc_count = Column(Integer, nullable=False, default=0)

    # Notas libres
    notes = Column(Text, nullable=True)

    # Auditoría mínima
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relaciones
    user = relationship("User", backref="cases", lazy="joined")

    # 🔥 RELACIÓN CORRECTA con bombas detectadas
    pumps_detected = relationship(
        "PumpsDetected",
        back_populates="case",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    # 👇 NUEVA relación con bombas MROY seleccionadas
    selected_pumps = relationship(
        "MroySelectedPump",
        back_populates="case",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    # Índices
    __table_args__ = (
        Index("ix_cases_user_status", "user_id", "status"),
    )

    def touch(self):
        self.updated_at = datetime.utcnow()
