# app/models/delivery_terms.py
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, Index
from datetime import datetime
from sqlalchemy.orm import relationship
from app.database.conection import Base

class DeliveryTerm(Base):
    __tablename__ = "delivery_terms"

    id = Column(Integer, primary_key=True, index=True)
    incoterm = Column(String(10), nullable=False)           # EXW/FOB/CIF/DDP
    place = Column(String(200), nullable=True)              # puerto / ciudad
    lead_time_days = Column(Integer, nullable=True)         # tiempo de entrega general
    validity_days = Column(Integer, nullable=True)          # validez de oferta
    warranty_months = Column(Integer, nullable=True)        # garantía
    shipping_mode = Column(String(30), nullable=True)       # Aéreo/Marítimo/Terrestre
    notes = Column(Text, nullable=True)

    active = Column(Boolean, nullable=False, default=True)
    created_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    creator = relationship("User", backref="delivery_terms", lazy="joined")

    __table_args__ = (
        Index("ix_delivery_terms_active_incoterm", "active", "incoterm"),
    )

    def touch(self):
        self.updated_at = datetime.utcnow()
