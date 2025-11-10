# app/models/user.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from datetime import datetime
from app.database.conection import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(100), nullable=False)
    email = Column(String(120), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    rol = Column(String(50), default="usuario")  # usuario, admin, etc.
    activo = Column(Boolean, default=False)
    fecha_creacion = Column(DateTime, default=datetime.utcnow)
