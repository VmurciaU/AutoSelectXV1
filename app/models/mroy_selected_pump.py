# app/models/mroy_selected_pump.py

from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Boolean,
    Text,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.orm import relationship

from app.database.conection import Base


class MroySelectedPump(Base):
    """
    Bomba MROY seleccionada para una bomba detectada (pumps_detected).

    MVP:
      - Siempre pertenece a un case_id.
      - Siempre está asociada a UNA bomba detectada (1–1).
      - Guarda FKs mínimas a catálogo + snapshot técnico y económico.
    """

    __tablename__ = "mroy_selected_pumps"

    # ---------------------------
    # PK + relaciones fuertes
    # ---------------------------
    id = Column(Integer, primary_key=True, index=True)

    case_id = Column(
        Integer,
        ForeignKey("cases.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    detected_pump_id = Column(
        Integer,
        ForeignKey("pumps_detected.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
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

    # ---------------------------
    # Auditoría + soft delete
    # ---------------------------
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    is_active = Column(Boolean, default=True, nullable=False)

    # ---------------------------
    # Identidad MROY
    # ---------------------------
    mroy_series = Column(String(10), nullable=False)
    full_code = Column(String(100), nullable=False)
    summary_text = Column(Text, nullable=True)

    # ---------------------------
    # FKs mínimas a catálogo
    # ---------------------------
    capacity_master_id = Column(
        Integer,
        ForeignKey("mroya_capacity_master.id", ondelete="RESTRICT"),
        nullable=True,
    )

    liquid_end_id = Column(
        Integer,
        ForeignKey("mroy_mra1_01_liquid.id", ondelete="RESTRICT"),
        nullable=False,
    )

    plunger_id = Column(
        Integer,
        ForeignKey("mroy_mra1_02_plunger.id", ondelete="RESTRICT"),
        nullable=False,
    )

    gear_ratio_id = Column(
        Integer,
        ForeignKey("mroy_mra1_03_gear.id", ondelete="RESTRICT"),
        nullable=False,
    )

    viscosity_master_id = Column(
        Integer,
        ForeignKey("mroya_viscosidad_master.id", ondelete="RESTRICT"),
        nullable=True,
    )

    # ---------------------------
    # Snapshot técnico cacheado
    # ---------------------------
    design_flow_gph = Column(Float, nullable=True)
    design_flow_lph = Column(Float, nullable=True)
    design_pressure_psi = Column(Float, nullable=True)
    design_viscosity_cp = Column(Float, nullable=True)

    # ---------------------------
    # Precios (MVP)
    # ---------------------------
    price_total_usd = Column(Float, nullable=True)
    currency = Column(String(10), nullable=True)

    # ---------------------------
    # Relaciones ORM
    # ---------------------------
    case = relationship(
        "Case",
        back_populates="selected_pumps",
    )

    detected_pump = relationship(
        "PumpsDetected",
        back_populates="selected_pump",
        uselist=False,
    )

    __table_args__ = (
        Index(
            "ix_mroy_selected_case_active",
            "case_id",
            "is_active",
        ),
    )

    def touch(self):
        self.updated_at = datetime.utcnow()

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "case_id": self.case_id,
            "detected_pump_id": self.detected_pump_id,
            "mroy_series": self.mroy_series,
            "full_code": self.full_code,
            "summary_text": self.summary_text,
            "capacity_master_id": self.capacity_master_id,
            "liquid_end_id": self.liquid_end_id,
            "plunger_id": self.plunger_id,
            "gear_ratio_id": self.gear_ratio_id,
            "viscosity_master_id": self.viscosity_master_id,
            "design_flow_gph": self.design_flow_gph,
            "design_flow_lph": self.design_flow_lph,
            "design_pressure_psi": self.design_pressure_psi,
            "design_viscosity_cp": self.design_viscosity_cp,
            "price_total_usd": self.price_total_usd,
            "currency": self.currency,
            "is_active": self.is_active,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
