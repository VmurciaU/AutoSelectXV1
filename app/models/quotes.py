# app/models/quotes.py
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, Numeric, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database.conection import Base

# Opciones: COP / USD
CURRENCY_COP = "COP"
CURRENCY_USD = "USD"
CURRENCIES = (CURRENCY_COP, CURRENCY_USD)

# Estados: draft / final
QUOTE_DRAFT = "draft"
QUOTE_FINAL = "final"

class Quote(Base):
    __tablename__ = "quotes"

    id = Column(Integer, primary_key=True, index=True)
    quote_code = Column(String(20), unique=True, nullable=True, index=True)  # CA-###-AA (se asigna al emitir, o al crear si prefieres)

    case_id = Column(Integer, ForeignKey("cases.id", ondelete="CASCADE"), nullable=False, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id", ondelete="RESTRICT"), nullable=False, index=True)
    delivery_term_id = Column(Integer, ForeignKey("delivery_terms.id", ondelete="RESTRICT"), nullable=False, index=True)

    currency = Column(String(3), nullable=False, default=CURRENCY_COP)  # COP|USD
    exchange_rate = Column(Numeric(14, 6), nullable=False, default=1.0) # TRM si USD, 1.0 si COP
    exchange_date = Column(Date, nullable=True)

    status = Column(String(10), nullable=False, default=QUOTE_DRAFT)  # draft|final

    subtotal = Column(Numeric(14, 2), nullable=True)
    discount = Column(Numeric(14, 2), nullable=True)
    tax = Column(Numeric(14, 2), nullable=True)
    total = Column(Numeric(14, 2), nullable=True)

    internal_notes = Column(Text, nullable=True)
    customer_notes = Column(Text, nullable=True)

    created_by = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    final_at = Column(DateTime, nullable=True)

    # Relaciones
    case = relationship("Case", backref="quote", lazy="joined", uselist=False)
    customer = relationship("Customer", backref="quotes", lazy="joined")
    delivery_term = relationship("DeliveryTerm", backref="quotes", lazy="joined")
    creator = relationship("User", backref="quotes", lazy="joined")

    __table_args__ = (
        Index("ix_quotes_status_currency", "status", "currency"),
        Index("ix_quotes_case_customer", "case_id", "customer_id"),
    )

    def touch(self):
        self.updated_at = datetime.utcnow()
