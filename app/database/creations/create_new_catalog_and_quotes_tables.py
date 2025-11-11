# scripts/create_new_catalog_and_quotes_tables.py
# Ejecuta:  python -m scripts.create_new_catalog_and_quotes_tables
# (o con el bootstrap de abajo si prefieres python scripts/create_new_catalog_and_quotes_tables.py)

# --- bootstrap opcional para ejecutar SIN -m ---
if __name__ == "__main__" and __package__ is None:
    import os, sys
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    # subir un nivel más para resolver 'app'
    ROOT2 = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
    if ROOT2 not in sys.path:
        sys.path.insert(0, ROOT2)
# ------------------------------------------------

from sqlalchemy.orm import Session
from app.database.conection import engine

# IMPORTA SOLO MODELOS NUEVOS (parcial)
from app.models.products import Product
from app.models.customers import Customer
from app.models.delivery_terms import DeliveryTerm
from app.models.quotes import Quote
from app.models.quote_items import QuoteItem

def create_new_tables():
    # ⚠️ Orden respeta FKs: primero catálogos, luego cabecera de quote, luego detalle
    Product.__table__.create(bind=engine, checkfirst=True)
    Customer.__table__.create(bind=engine, checkfirst=True)
    DeliveryTerm.__table__.create(bind=engine, checkfirst=True)
    Quote.__table__.create(bind=engine, checkfirst=True)
    QuoteItem.__table__.create(bind=engine, checkfirst=True)

    print("✅ Tablas nuevas creadas/verificadas: products, customers, delivery_terms, quotes, quote_items.")

    # pequeño sanity check
    with Session(engine) as s:
        count_products = s.query(Product).count()
        count_customers = s.query(Customer).count()
        count_terms = s.query(DeliveryTerm).count()
        count_quotes = s.query(Quote).count()
        count_items = s.query(QuoteItem).count()
        print(f"ℹ️ Totales -> products:{count_products} customers:{count_customers} terms:{count_terms} quotes:{count_quotes} items:{count_items}")

if __name__ == "__main__":
    create_new_tables()
