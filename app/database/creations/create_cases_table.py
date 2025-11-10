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

def create_cases_table():
    # 1) Asegura 'users' (FK target) y luego 'cases'
    User.__table__.create(bind=engine, checkfirst=True)
    Case.__table__.create(bind=engine, checkfirst=True)
    print("✅ Tablas 'users' y 'cases' creadas/verificadas.")

    # 2) Prueba rápida
    with Session(engine) as session:
        count = session.query(Case).count()
        print(f"📊 Casos existentes: {count}")

if __name__ == "__main__":
    create_cases_table()
