from sqlalchemy import Column, Integer, String, Boolean, DateTime
from datetime import datetime
from app.database.conection import Base
from sqlalchemy.orm import relationship

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(100), nullable=False)
    email = Column(String(120), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    rol = Column(String(50), default="usuario")
    activo = Column(Boolean, default=False)
    fecha_creacion = Column(DateTime, default=datetime.utcnow)

    # Relaciones con auditoría de bombas detectadas
    pumps_detected_created = relationship(
        "PumpsDetected",
        foreign_keys="PumpsDetected.created_by",
        back_populates="creator",
        lazy="selectin"
    )

    pumps_detected_updated = relationship(
        "PumpsDetected",
        foreign_keys="PumpsDetected.updated_by",
        back_populates="updater",
        lazy="selectin"
    )