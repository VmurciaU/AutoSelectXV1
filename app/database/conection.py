# app/database/conection.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Cargar .env
load_dotenv()

# Cargar .env
load_dotenv()
print("🟡 DEBUG: .env real leído desde:", os.path.abspath(".env"))

DATABASE_URL = os.getenv("DATABASE_URL")
print("🟡 DEBUG: DATABASE_URL=", DATABASE_URL)



DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("❌ ERROR FATAL: DATABASE_URL no está definida en .env")

# Crear engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

"""  Prueba para TES WSL caido DB 5
# Probar conexión
try:
    with engine.connect() as conn:
        print(f"✔ Connected to database: {DATABASE_URL}")
except Exception as e:
    print("❌ Database connection failed:", e)
    raise
    
"""

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db_url():
    return DATABASE_URL
