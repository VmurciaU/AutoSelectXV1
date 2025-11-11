# app/models/documents.py
from __future__ import annotations

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, ForeignKey,
    DateTime, Index, func
)
from sqlalchemy.orm import relationship

from app.database.conection import Base


class Document(Base):
    """
    Representa un archivo PDF u otro documento cargado en un caso.
    - Se almacena el nombre original, la ruta donde quedó almacenado
      y metadatos básicos (mime/size/pages).
    - 'status' modela el avance del pipeline: queued|cleaned|indexed|error.
    """
    __tablename__ = "documents"

    # PK
    id = Column(Integer, primary_key=True, index=True)

    # Relaciones
    case_id = Column(
        Integer,
        ForeignKey("cases.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Datos básicos del PDF
    filename = Column(String(255), nullable=False)          # nombre visible (p.ej. Taller4-...pdf)
    original_path = Column(String(600), nullable=False)     # ruta de origen (si deseas conservarla)
    stored_path   = Column(String(600), nullable=False)     # ruta relativa donde se guardó
    mime_type     = Column(String(80),  nullable=True)
    size_bytes    = Column(Integer,      nullable=True)
    pages         = Column(Integer,      nullable=True)

    # Estado del procesamiento
    status = Column(
        String(20),
        nullable=False,
        default="queued",  # queued|cleaned|indexed|error
        index=True,
    )

    # Notas o comentarios
    notes = Column(Text, nullable=True)

    # Auditoría
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Relaciones ORM
    case = relationship("Case", backref="documents", lazy="joined")
    user = relationship("User", backref="documents", lazy="joined")

    __table_args__ = (
        # consultas comunes: por caso y por estado
        Index("ix_documents_case_status", "case_id", "status"),
    )

    def touch(self) -> None:
        """Actualiza updated_at manualmente si haces cambios in-memory antes de flush/commit."""
        self.updated_at = datetime.utcnow()

    def __repr__(self) -> str:
        return (
            f"<Document id={self.id} case_id={self.case_id} "
            f"filename={self.filename!r} status={self.status!r}>"
        )
