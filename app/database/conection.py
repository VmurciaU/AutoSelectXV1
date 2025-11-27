# app/database/conection.py

import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# =====================================================
# 1. Leer DATABASE_URL desde variables de entorno
# =====================================================
DATABASE_URL = os.getenv("DATABASE_URL")

# Si no existe DATABASE_URL (por ejemplo en local)
# usamos la URL por defecto local:
if not DATABASE_URL:
    print("⚠️ Aviso: DATABASE_URL no encontrada. Usando configuración LOCAL.")
    DATABASE_URL = "postgresql://postgres:root@localhost:5433/autoselectx"

# =====================================================
# 2. Crear engine SQLAlchemy para cualquier entorno
# =====================================================
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,   # Evita conexiones rotas en Render
)

# =====================================================
# 3. 🔥 FORZAR CONEXIÓN AL ARRANCAR (debug Render)
# =====================================================
try:
    with engine.connect() as conn:
        print("✔ Connected to database!")
except Exception as e:
    print("❌ Database connection failed:", e)

# =====================================================
# 4. Crear sesión SQLAlchemy
# =====================================================
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Base de modelos
Base = declarative_base()

# Auxiliar para debug
def get_db_url() -> str:
    return DATABASE_URL
