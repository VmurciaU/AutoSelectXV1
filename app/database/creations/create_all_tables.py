# scripts/create_all_tables.py
# Ejecuta:  python -m scripts.create_all_tables
# (o con bootstrap como arriba si prefieres ejecutarlo directo)

# --- bootstrap opcional para ejecutar SIN -m ---
if __name__ == "__main__" and __package__ is None:
    import os, sys
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    ROOT2 = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
    if ROOT2 not in sys.path:
        sys.path.insert(0, ROOT2)
# ------------------------------------------------

from app.database.conection import Base, engine

# IMPORTA TODOS LOS MODELOS para registrarlos en la metadata
from app.models.user import User
from app.models.cases import Case
from app.models.documents import Document
from app.models.products import Product
from app.models.customers import Customer
from app.models.delivery_terms import DeliveryTerm
from app.models.quotes import Quote
from app.models.quote_items import QuoteItem

def main():
    # Idempotente: crea solo si no existen
    Base.metadata.create_all(bind=engine)
    print("✅ Tablas creadas / verificadas (todas las entidades del MVP).")

if __name__ == "__main__":
    main()
