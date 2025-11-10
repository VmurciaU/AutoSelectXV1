# app/database/conection.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# URL de conexión a tu base de datos PostgreSQL
DATABASE_URL = "postgresql://postgres:root@localhost:5433/autoselectx"

# Motor SQLAlchemy
engine = create_engine(DATABASE_URL)

# Sesión local (manejo de transacciones)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base declarativa para todos los modelos
Base = declarative_base()

# ✅ Función auxiliar (para usar en scripts de creación)
def get_db_url() -> str:
    """Retorna la URL de conexión a la base de datos"""
    return DATABASE_URL
