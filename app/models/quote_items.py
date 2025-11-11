# app/models/quote_items.py
from sqlalchemy import Column, Integer, Text, DateTime, Numeric, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from app.database.conection import Base

class QuoteItem(Base):
    __tablename__ = "quote_items"

    id = Column(Integer, primary_key=True, index=True)
    quote_id = Column(Integer, ForeignKey("quotes.id", ondelete="CASCADE"), nullable=False, index=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="RESTRICT"), nullable=False, index=True)

    qty = Column(Numeric(12, 3), nullable=False, default=1)        # soporta cantidades no enteras si aplica
    unit_price = Column(Numeric(14, 2), nullable=True)
    discount_pct = Column(Numeric(6, 2), nullable=True)            # %
    delivery_time_days = Column(Integer, nullable=True)            # tiempo entrega por ítem

    subtotal = Column(Numeric(14, 2), nullable=True)

    internal_note = Column(Text, nullable=True)
    customer_note = Column(Text, nullable=True)

    metadata = Column(JSONB, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relaciones
    quote = relationship("Quote", backref="items", lazy="joined")
    product = relationship("Product", backref="quote_items", lazy="joined")

    __table_args__ = (
        Index("ix_quote_items_quote_product", "quote_id", "product_id"),
    )

    def touch(self):
        self.updated_at = datetime.utcnow()
