# app/models/products.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Numeric, Index
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from app.database.conection import Base

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    model_code = Column(String(50), unique=True, nullable=False, index=True)  # código de modelo
    name = Column(String(200), nullable=False)
    type = Column(String(50), nullable=False, default="pump")  # pump|accessory|...
    specs = Column(JSONB, nullable=True)                       # atributos técnicos libres
    base_price = Column(Numeric(12, 2), nullable=True)
    active = Column(Boolean, nullable=False, default=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_products_active_type", "active", "type"),
    )

    def touch(self):
        self.updated_at = datetime.utcnow()
