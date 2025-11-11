# app/models/customers.py
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database.conection import Base

class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, index=True)
    # Empresa y contacto
    company_name = Column(String(200), nullable=False)
    contact_name = Column(String(150), nullable=True)
    job_title = Column(String(120), nullable=True)
    email = Column(String(200), nullable=True, index=True)
    phone = Column(String(50), nullable=True)    # fijo
    mobile = Column(String(50), nullable=True)   # celular
    city = Column(String(120), nullable=True)
    country = Column(String(120), nullable=True)
    comments = Column(Text, nullable=True)

    active = Column(Boolean, nullable=False, default=True)
    created_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    creator = relationship("User", backref="customers", lazy="joined")

    __table_args__ = (
        Index("ix_customers_active_company", "active", "company_name"),
    )

    def touch(self):
        self.updated_at = datetime.utcnow()
