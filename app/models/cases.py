# app/models/cases.py
from __future__ import annotations

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, ForeignKey,
    DateTime, Index, func
)
from sqlalchemy.orm import relationship

from app.database.conection import Base


class Case(Base):
    """
    Representa un caso (expediente) del usuario.
    - Guarda rutas base útiles para tu pipeline PC1–PC6 / LightRAG.
    - 'status' guía el estado general del caso: queued|indexing|done|error|quoted|archived.
    - 'doc_count' es un contador denormalizado para performance en la vista /cases.
      (Actualízalo en los endpoints de carga/borrado.)
    """
    __tablename__ = "cases"

    # PK y FKs
    id = Column(Integer, primary_key=True, index=True)  # consecutivo del caso
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    customer_id = Column(Integer, nullable=True, index=True)  # (futuro FK a clientes)

    # Datos principales del caso
    name   = Column(String(200), nullable=True)  # título o nombre del caso
    status = Column(
        String(20),
        nullable=False,
        default="queued",  # queued|indexing|done|error|quoted|archived
        index=True,
    )

    # Rutas usadas por el pipeline PC1–PC6 / LightRAG
    # Ejemplos:
    #   input_dir = "shared_data/inbox/<case_id>/original/"
    #   index_dir = "shared_data/index/<case_id>/"
    input_dir = Column(String(500), nullable=False)
    index_dir = Column(String(500), nullable=False)

    # Métricas / versión de pipeline
    rag_version = Column(String(50), nullable=True)   # p.ej. "pc1-6@2025.1_
