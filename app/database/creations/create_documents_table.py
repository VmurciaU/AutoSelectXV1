# --- bootstrap para ejecutar SIN -m ---
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
# --------------------------------------

from sqlalchemy.orm import Session
from app.database.conection import engine
from app.models.user import User
from app.models.cases import Case
from app.models.documents import Document

def create_documents_table():
    # Asegura dependencias en orden: users -> cases -> documents
    User.__table__.create(bind=engine, checkfirst=True)
    Case.__table__.create(bind=engine, checkfirst=True)
    Document.__table__.create(bind=engine, checkfirst=True)
    print("✅ Tablas 'users', 'cases' y 'documents' creadas/verificadas.")

    with Session(engine) as session:
        c_docs = session.query(Document).count()
        print(f"📄 Documentos existentes: {c_docs}")

if __name__ == "__main__":
    create_documents_table()
