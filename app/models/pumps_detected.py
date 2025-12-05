# app/models/pumps_detected.py

from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    Index,
)
from sqlalchemy.orm import relationship

from app.database.conection import Base


class PumpsDetected(Base):
    __tablename__ = "pumps_detected"

    # PK
    id = Column(Integer, primary_key=True, index=True)

    # --- Relaciones fuertes (FK) ---
    case_id = Column(
        Integer,
        ForeignKey("cases.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    created_by = Column(
        Integer,
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    updated_by = Column(
        Integer,
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )

    # --- Auditoría ---
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Soft delete
    is_active = Column(Boolean, default=True, nullable=False)

    # ============================
    #  CAMPOS TÉCNICOS (ROOT)
    # ============================
    fluid = Column(String(200), nullable=True)
    viscosity = Column(Float, nullable=True)

    discharge_pressure = Column(Float, nullable=True)
    discharge_pressure_std = Column(Float, nullable=True)

    flow_min = Column(Float, nullable=True)
    flow_nominal = Column(Float, nullable=True)
    flow_max = Column(Float, nullable=True)

    flow_max_std = Column(Float, nullable=True)
    flow_unit_std = Column(String(50), nullable=True)

    cantidad_bombas = Column(Integer, nullable=False, default=1)

    # borrador | validado | ajustado
    estado = Column(String(20), nullable=False, default="borrador")

    # ============================
    #  CAMPOS OPTIONAL
    # ============================
    description = Column(Text, nullable=True)
    tag = Column(String(100), nullable=True)
    service = Column(String(200), nullable=True)
    temperature = Column(Float, nullable=True)
    materials = Column(String(200), nullable=True)
    area = Column(String(100), nullable=True)
    location = Column(String(200), nullable=True)

    # ============================
    #  RELACIONES ORM
    # ============================
    case = relationship(
        "Case",
        back_populates="pumps_detected",
    )

    creator = relationship(
        "User",
        foreign_keys=[created_by],
        back_populates="pumps_detected_created",
    )

    updater = relationship(
        "User",
        foreign_keys=[updated_by],
        back_populates="pumps_detected_updated",
    )

    __table_args__ = (
        Index("ix_pumps_detected_case_active", "case_id", "is_active"),
    )

    def touch(self):
        """Actualizar timestamp de modificación."""
        self.updated_at = datetime.utcnow()

    def to_dict(self) -> dict:
        """Opcional: útil si luego quieres devolver esta bomba como JSON."""
        return {
            "id": self.id,
            "case_id": self.case_id,
            "fluid": self.fluid,
            "viscosity": self.viscosity,
            "discharge_pressure": self.discharge_pressure,
            "discharge_pressure_std": self.discharge_pressure_std,
            "flow": {
                "min": self.flow_min,
                "nominal": self.flow_nominal,
                "max": self.flow_max,
            },
            "flow_max_std": self.flow_max_std,
            "flow_unit_std": self.flow_unit_std,
            "cantidad_bombas": self.cantidad_bombas,
            "estado": self.estado,
            "optional": {
                "description": self.description,
                "tag": self.tag,
                "service": self.service,
                "temperature": self.temperature,
                "materials": self.materials,
                "area": self.area,
                "location": self.location,
            },
            "is_active": self.is_active,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
