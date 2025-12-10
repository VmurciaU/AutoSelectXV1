# app/models/mroy_master.py
"""
Modelos SQLAlchemy para las tablas maestras de selección de bomba:
- mroya_capacity_master
- mroya_hp_master
- mroya_viscosidad_master
"""

from sqlalchemy import Column, Integer, String, Float, Text
from app.database.conection import Base


class MroyaCapacityMaster(Base):
    """
    Tabla: mroya_capacity_master.xlsx

    Relaciona:
      - serie (A, B, etc.)
      - tipo de cabeza (metal / plastic)
      - código de plunger
      - código de gear ratio
      - strokes a 60/50 Hz
      - capacidades a 100 psi y a presión máxima
    """
    __tablename__ = "mroya_capacity_master"

    id = Column(Integer, primary_key=True, index=True)

    series = Column(String(10), nullable=False)  # A, B...
    head_type = Column(String(20), nullable=False)  # metal / plastic
    plunger_code = Column(String(5), nullable=False)  # H, C, D, E, F...
    plunger_diameter_in = Column(String(10), nullable=False)  # "3/8", "7/16", etc.
    plunger_diameter_mm = Column(Float, nullable=False)

    gear_ratio_code = Column(String(10), nullable=False)  # 77, 48, 24, 15, 10...

    strokes60 = Column(Float, nullable=False)
    strokes50 = Column(Float, nullable=False)

    cap60_100psi_gph = Column(Float, nullable=False)
    cap60_100psi_lph = Column(Float, nullable=False)
    cap60_maxP_gph = Column(Float, nullable=False)
    cap60_maxP_lph = Column(Float, nullable=False)

    maxP_psi = Column(Float, nullable=False)
    maxP_bar = Column(Float, nullable=False)

    cap50_100psi_gph = Column(Float, nullable=False)
    cap50_100psi_lph = Column(Float, nullable=False)
    cap50_maxP_gph = Column(Float, nullable=False)
    cap50_maxP_lph = Column(Float, nullable=False)

    notes = Column(Text, nullable=True)


class MroyaHpMaster(Base):
    """
    Tabla: mroya_hp_master.xlsx

    Relaciona:
      - serie, frame, plunger_code
      - fase del motor y configuración (Simplex / Duplex)
      - condición de presión (cuando aplique)
      - HP y kW requeridos
    """
    __tablename__ = "mroya_hp_master"

    id = Column(Integer, primary_key=True, index=True)

    mroy_series = Column(String(10), nullable=False)  # A, B...
    frame = Column(String(5), nullable=False)  # A, B, etc.
    plunger_code = Column(String(5), nullable=False)

    motor_phase = Column(Integer, nullable=False)  # 1, 3, etc.
    configuration = Column(String(20), nullable=False)  # Simplex, Duplex, etc.

    # Ej: "NA", "pressure ≤ 400 psi", etc. En Excel venía como texto/NaN.
    pressure_condition = Column(String(100), nullable=True)
    pressure_max_psi = Column(Float, nullable=True)

    required_hp = Column(Float, nullable=False)
    required_kw = Column(Float, nullable=False)

    label_hp_kw = Column(String(50), nullable=False)  # "1/3 HP(0.25 kW)", etc.
    notes = Column(Text, nullable=True)


class MroyaViscosidadMaster(Base):
    """
    Tabla: mroya_viscosidad_master.xlsx

    Define la viscosidad máxima permitida por combinación:
      - tamaño/código de plunger
      - gear ratio
      - strokes a 60/50 Hz
      - viscosidad con/sin código V (cp)
    """
    __tablename__ = "mroya_viscosidad_master"

    id = Column(Integer, primary_key=True, index=True)

    plunger_size_in = Column(String(10), nullable=False)  # "3/8", etc.
    plunger_size_mm = Column(Float, nullable=False)
    plunger_code = Column(String(5), nullable=False)

    gear_ratio = Column(String(10), nullable=False)  # 77, 48, 24...

    strokes_60hz = Column(Float, nullable=False)
    strokes_50hz = Column(Float, nullable=False)

    viscosidad_con_codigo_V_cp = Column(Float, nullable=True)
    viscosidad_sin_codigo_V_cp = Column(Float, nullable=True)

    notes = Column(Text, nullable=True)
