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
    __tablename__ = "mroya_capacity_master"

    id = Column(Integer, primary_key=True, index=True)

    series = Column(String(10), nullable=False)  # A, B...
    head_type = Column(String(20), nullable=False)  # metal / plastic
    plunger_code = Column(String(5), nullable=False)
    plunger_diameter_in = Column(String(10), nullable=False)
    plunger_diameter_mm = Column(Float, nullable=False)

    gear_ratio_code = Column(String(10), nullable=False)  # 77, 48, 24, 15, 10, 08...

    # 👇 AQUÍ VIENE EL CAMBIO IMPORTANTE
    # 60 Hz: hay combinaciones que NO existen (gear 08), por eso permitimos NULL
    strokes60 = Column(Float, nullable=True)
    cap60_100psi_gph = Column(Float, nullable=True)
    cap60_100psi_lph = Column(Float, nullable=True)
    cap60_maxP_gph = Column(Float, nullable=True)
    cap60_maxP_lph = Column(Float, nullable=True)
    maxP_psi = Column(Float, nullable=True)
    maxP_bar = Column(Float, nullable=True)

    # 50 Hz: en el catálogo SIEMPRE hay datos para esas filas, los dejamos obligatorios
    strokes50 = Column(Float, nullable=False)
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
    __tablename__ = "mroya_viscosidad_master"

    id = Column(Integer, primary_key=True, index=True)

    plunger_size_in = Column(String(10), nullable=False)
    plunger_size_mm = Column(Float, nullable=False)
    plunger_code = Column(String(5), nullable=False)

    gear_ratio = Column(String(10), nullable=False)

    # Algunas combinaciones NO existen a 60 Hz → deben permitir NULL
    strokes_60hz = Column(Float, nullable=True)

    # 50 Hz está presente para todos los plungers → obligatorio
    strokes_50hz = Column(Float, nullable=False)

    # Estas dos columnas muchas veces dicen "N/A" → deben ser NULL
    viscosidad_con_codigo_V_cp = Column(Float, nullable=True)
    viscosidad_sin_codigo_V_cp = Column(Float, nullable=True)

    notes = Column(Text, nullable=True)
