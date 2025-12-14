# app/models/customers.py
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, Index
from datetime import datetime
from app.database.conection import Base


class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, index=True)

    name = Column(String(200), nullable=False)
    nit = Column(String(32), nullable=True, index=True)  # ✅ dejamos SOLO este index

    contact_name = Column(String(150), nullable=True)
    email = Column(String(150), nullable=True)
    phone = Column(String(60), nullable=True)

    country = Column(String(80), nullable=True, default="Colombia")
    city = Column(String(120), nullable=True)
    address = Column(String(220), nullable=True)

    notes = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_customers_name", "name"),  # ✅ solo este
    )

    def touch(self):
        self.updated_at = datetime.utcnow()
